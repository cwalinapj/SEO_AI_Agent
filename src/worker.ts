export interface Env {
  DB: D1Database;
  PAGESPEED_API_KEY: string;
  WP_PLUGIN_SHARED_SECRET?: string;
  APIFY_TOKEN?: string;
  APIFY_GOOGLE_ACTOR?: string;
  PROXY_CONTROL_SECRET?: string;
}

const COOLDOWN_MS = 12 * 60 * 60 * 1000; // 12 hours
const PLUGIN_TIMESTAMP_WINDOW_MS = 5 * 60 * 1000;
const SERP_MAX_RESULTS = 20;

function nowMs(): number {
  return Date.now();
}
function toDateYYYYMMDD(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}
function uuid(prefix: string): string {
  return `${prefix}_${crypto.randomUUID()}`;
}

function toHex(bytes: ArrayBuffer): string {
  return [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function hmacSha256Hex(secret: string, payload: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signed = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return toHex(signed);
}

async function sha256Hex(payload: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(payload));
  return toHex(digest);
}

function safeEqualHex(left: string, right: string): boolean {
  const a = left.trim().toLowerCase();
  const b = right.trim().toLowerCase();
  if (!a || !b || a.length !== b.length) {
    return false;
  }
  let mismatch = 0;
  for (let i = 0; i < a.length; i += 1) {
    mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return mismatch === 0;
}

function parseTimestampMs(raw: string): number | null {
  const v = Number(raw);
  if (!Number.isFinite(v)) {
    return null;
  }
  if (v > 1e12) {
    return Math.round(v);
  }
  if (v > 1e9) {
    return Math.round(v * 1000);
  }
  return null;
}

function normalizeRedirectPath(rawPath: unknown): string | null {
  let path = String(rawPath ?? "").trim();
  if (!path) {
    return null;
  }

  if (/^https?:\/\//i.test(path)) {
    try {
      const parsed = new URL(path);
      path = `${parsed.pathname}${parsed.search}`;
    } catch {
      return null;
    }
  }

  if (!path.startsWith("/")) {
    path = `/${path}`;
  }

  const [pathname, query = ""] = path.split("?");
  const collapsedPath = pathname.replace(/\/+/g, "/");
  path = query ? `${collapsedPath}?${query}` : collapsedPath;

  if (path.length > 240) {
    return null;
  }

  if (
    path === "/" ||
    path.startsWith("/wp-admin") ||
    path.startsWith("/wp-login.php") ||
    path.startsWith("/wp-json")
  ) {
    return null;
  }

  return path;
}

function clampInt(input: unknown, min: number, max: number, fallback: number): number {
  const asNumber = Number(input);
  if (!Number.isFinite(asNumber)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, Math.round(asNumber)));
}

function parseJsonObject(input: unknown): Record<string, unknown> | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  return input as Record<string, unknown>;
}

type SerpResultRow = {
  rank: number;
  url: string;
  domain: string;
  title: string | null;
  snippet: string | null;
};

function cleanString(input: unknown, maxLen = 4000): string {
  const value = String(input ?? "").trim();
  if (!value) {
    return "";
  }
  return value.slice(0, maxLen);
}

function safeJsonStringify(input: unknown, maxLen = 64000): string {
  let raw = "";
  try {
    raw = JSON.stringify(input ?? null);
  } catch {
    raw = JSON.stringify({ serialization_error: true });
  }
  if (raw.length <= maxLen) {
    return raw;
  }
  return raw.slice(0, maxLen);
}

type JobStatus = "running" | "succeeded" | "failed" | "dead_letter";

async function createJobRecord(
  env: Env,
  input: {
    userId?: string | null;
    siteId?: string | null;
    type: string;
    request?: unknown;
    maxAttempts?: number;
  }
): Promise<string> {
  const ts = nowMs();
  const jobId = uuid("job");
  await env.DB.prepare(
    `INSERT INTO jobs (
      job_id,
      user_id,
      site_id,
      type,
      status,
      attempts,
      max_attempts,
      cost_units,
      error_code,
      error_json,
      request_json,
      created_at,
      updated_at,
      started_at,
      finished_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, NULL)`
  )
    .bind(
      jobId,
      cleanString(input.userId, 120) || null,
      cleanString(input.siteId, 120) || null,
      cleanString(input.type, 80),
      "running",
      1,
      clampInt(input.maxAttempts, 1, 10, 3),
      0,
      input.request == null ? null : safeJsonStringify(input.request, 16000),
      ts,
      ts,
      ts
    )
    .run();
  return jobId;
}

async function finalizeJobSuccess(env: Env, jobId: string, costUnits = 1): Promise<void> {
  const ts = nowMs();
  await env.DB.prepare(
    `UPDATE jobs
     SET status = 'succeeded',
         cost_units = ?,
         error_code = NULL,
         error_json = NULL,
         updated_at = ?,
         finished_at = ?
     WHERE job_id = ?`
  )
    .bind(Math.max(0, Math.round(costUnits)), ts, ts, jobId)
    .run();
}

async function finalizeJobFailure(
  env: Env,
  jobId: string,
  errorCode: string,
  errorPayload: unknown,
  status: JobStatus = "failed"
): Promise<void> {
  const ts = nowMs();
  await env.DB.prepare(
    `UPDATE jobs
     SET status = ?,
         error_code = ?,
         error_json = ?,
         updated_at = ?,
         finished_at = ?
     WHERE job_id = ?`
  )
    .bind(
      status,
      cleanString(errorCode, 120) || "job_failed",
      safeJsonStringify(errorPayload, 16000),
      ts,
      ts,
      jobId
    )
    .run();
}

async function createArtifactRecord(
  env: Env,
  input: {
    jobId: string;
    kind: string;
    payload?: unknown;
    r2Key?: string | null;
  }
): Promise<{ artifact_id: string; checksum: string; r2_key: string }> {
  const artifactId = uuid("art");
  const payloadJson = input.payload == null ? null : safeJsonStringify(input.payload, 32000);
  const checksum = await sha256Hex(payloadJson ?? `${input.jobId}:${input.kind}:${artifactId}`);
  const r2Key =
    cleanString(input.r2Key, 500) || `r2://artifacts/${cleanString(input.jobId, 120)}/${cleanString(input.kind, 120)}.json`;
  await env.DB.prepare(
    `INSERT INTO artifacts (
      artifact_id,
      job_id,
      kind,
      r2_key,
      checksum,
      payload_json,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(artifactId, input.jobId, cleanString(input.kind, 120), r2Key, checksum, payloadJson, nowMs())
    .run();
  return { artifact_id: artifactId, checksum, r2_key: r2Key };
}

async function loadJobRecord(env: Env, jobId: string): Promise<Record<string, unknown> | null> {
  return await env.DB.prepare(
    `SELECT
       job_id,
       user_id,
       site_id,
       type,
       status,
       attempts,
       max_attempts,
       cost_units,
       error_code,
       error_json,
       request_json,
       created_at,
       updated_at,
       started_at,
       finished_at
     FROM jobs
     WHERE job_id = ?
     LIMIT 1`
  )
    .bind(jobId)
    .first<Record<string, unknown>>();
}

async function loadArtifactsForJob(env: Env, jobId: string): Promise<Array<Record<string, unknown>>> {
  const rows = await env.DB.prepare(
    `SELECT
       artifact_id,
       job_id,
       kind,
       r2_key,
       checksum,
       payload_json,
       created_at
     FROM artifacts
     WHERE job_id = ?
     ORDER BY created_at ASC`
  )
    .bind(jobId)
    .all<Record<string, unknown>>();
  return rows.results ?? [];
}

function normalizeDomain(urlValue: string): string {
  try {
    const parsed = new URL(urlValue);
    return parsed.hostname.toLowerCase();
  } catch {
    return "";
  }
}

function normalizeRegion(input: unknown): Record<string, unknown> {
  const parsed = parseJsonObject(input);
  if (!parsed) {
    return { country: "US" };
  }
  const next: Record<string, unknown> = {};
  const country = cleanString(parsed.country, 2).toUpperCase();
  const language = cleanString(parsed.language, 2).toLowerCase();
  const region = cleanString(parsed.region, 120);
  const city = cleanString(parsed.city, 120);
  if (country) next.country = country;
  if (language) next.language = language;
  if (region) next.region = region;
  if (city) next.city = city;
  if (!next.country) {
    next.country = "US";
  }
  return next;
}

function buildGoogleQueryUrl(phrase: string, region: Record<string, unknown>): string {
  const q = new URL("https://www.google.com/search");
  q.searchParams.set("q", phrase);
  q.searchParams.set("num", String(SERP_MAX_RESULTS));

  const country = cleanString(region.country ?? "", 2).toUpperCase();
  const language = cleanString(region.language ?? "", 2).toLowerCase();
  if (country) {
    q.searchParams.set("gl", country);
  }
  if (language) {
    q.searchParams.set("hl", language);
  }
  return q.toString();
}

type ProxyInventoryRow = {
  proxy_id: string;
  label: string | null;
  proxy_url: string;
  country: string | null;
  region: string | null;
  metro_area: string | null;
  max_concurrent_leases: number;
  hourly_rate_usd: number;
  status: string;
  created_at: number;
  updated_at: number;
};

type ProxyLeaseRow = {
  lease_id: string;
  proxy_id: string;
  user_id: string;
  keyword: string | null;
  metro_area: string | null;
  status: "active" | "released" | "expired";
  leased_at: number;
  expires_at: number;
  released_at: number | null;
  hourly_rate_usd: number;
  proxy_url: string;
  country: string | null;
  region: string | null;
};

function requireProxyAdmin(req: Request, env: Env): Response | null {
  const expected = cleanString(env.PROXY_CONTROL_SECRET ?? "", 500);
  if (!expected) {
    return Response.json({ ok: false, error: "PROXY_CONTROL_SECRET not configured." }, { status: 503 });
  }
  const incoming = cleanString(req.headers.get("x-proxy-control-secret") ?? "", 500);
  if (!incoming || !safeEqualHex(toHexString(incoming), toHexString(expected))) {
    return Response.json({ ok: false, error: "proxy_admin_unauthorized" }, { status: 401 });
  }
  return null;
}

function toHexString(value: string): string {
  return Array.from(new TextEncoder().encode(value))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function normalizeProxyGeo(value: unknown, maxLen = 80): string {
  const clean = cleanString(value, maxLen).toLowerCase();
  return clean || "";
}

async function registerResidentialProxy(
  env: Env,
  payload: Record<string, unknown>
): Promise<{ ok: true; proxy: ProxyInventoryRow } | { ok: false; error: string }> {
  const proxyUrl = cleanString(payload.proxy_url, 2000);
  if (!proxyUrl || !/^https?:\/\//i.test(proxyUrl)) {
    return { ok: false, error: "proxy_url is required and must be http/https." };
  }

  const proxyId = cleanString(payload.proxy_id, 120) || uuid("proxy");
  const label = cleanString(payload.label, 160) || null;
  const country = normalizeProxyGeo(payload.country, 2) || null;
  const region = normalizeProxyGeo(payload.region, 120) || null;
  const metroArea = normalizeProxyGeo(payload.metro_area ?? payload.metro, 120) || null;
  const maxConcurrentLeases = clampInt(payload.max_concurrent_leases, 1, 200, 1);
  const hourlyRateUsd = Math.max(0, Number(payload.hourly_rate_usd ?? 0) || 0);
  const status = cleanString(payload.status, 20).toLowerCase() === "disabled" ? "disabled" : "active";
  const ts = nowMs();

  await env.DB.prepare(
    `INSERT INTO residential_proxies (
      proxy_id, label, proxy_url, country, region, metro_area,
      max_concurrent_leases, hourly_rate_usd, status, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(proxy_id) DO UPDATE SET
      label = excluded.label,
      proxy_url = excluded.proxy_url,
      country = excluded.country,
      region = excluded.region,
      metro_area = excluded.metro_area,
      max_concurrent_leases = excluded.max_concurrent_leases,
      hourly_rate_usd = excluded.hourly_rate_usd,
      status = excluded.status,
      updated_at = excluded.updated_at`
  )
    .bind(
      proxyId,
      label,
      proxyUrl,
      country,
      region,
      metroArea,
      maxConcurrentLeases,
      hourlyRateUsd,
      status,
      ts,
      ts
    )
    .run();

  return {
    ok: true,
    proxy: {
      proxy_id: proxyId,
      label,
      proxy_url: proxyUrl,
      country,
      region,
      metro_area: metroArea,
      max_concurrent_leases: maxConcurrentLeases,
      hourly_rate_usd: hourlyRateUsd,
      status,
      created_at: ts,
      updated_at: ts,
    },
  };
}

async function listResidentialProxyAvailability(
  env: Env,
  filters: { country: string; region: string; metro_area: string }
): Promise<Array<Record<string, unknown>>> {
  const rows = await env.DB.prepare(
    `SELECT
      rp.proxy_id,
      rp.label,
      rp.proxy_url,
      rp.country,
      rp.region,
      rp.metro_area,
      rp.max_concurrent_leases,
      rp.hourly_rate_usd,
      rp.status,
      (
        SELECT COUNT(*)
        FROM proxy_leases pl
        WHERE pl.proxy_id = rp.proxy_id
          AND pl.status = 'active'
          AND pl.expires_at > ?
      ) AS active_leases
    FROM residential_proxies rp
    WHERE rp.status = 'active'
      AND (? = '' OR rp.country = ?)
      AND (? = '' OR rp.region = ?)
      AND (? = '' OR rp.metro_area = ?)
    ORDER BY
      CASE WHEN ? <> '' AND rp.metro_area = ? THEN 0 ELSE 1 END,
      active_leases ASC,
      rp.updated_at DESC`
  )
    .bind(
      nowMs(),
      filters.country,
      filters.country,
      filters.region,
      filters.region,
      filters.metro_area,
      filters.metro_area,
      filters.metro_area,
      filters.metro_area
    )
    .all<Record<string, unknown>>();
  return rows.results ?? [];
}

async function listResidentialProxyInventory(env: Env): Promise<Array<Record<string, unknown>>> {
  const rows = await env.DB.prepare(
    `SELECT
      rp.proxy_id,
      rp.label,
      rp.proxy_url,
      rp.country,
      rp.region,
      rp.metro_area,
      rp.max_concurrent_leases,
      rp.hourly_rate_usd,
      rp.status,
      rp.created_at,
      rp.updated_at,
      (
        SELECT COUNT(*)
        FROM proxy_leases pl
        WHERE pl.proxy_id = rp.proxy_id
          AND pl.status = 'active'
          AND pl.expires_at > ?
      ) AS active_leases
    FROM residential_proxies rp
    ORDER BY rp.updated_at DESC`
  )
    .bind(nowMs())
    .all<Record<string, unknown>>();
  return rows.results ?? [];
}

async function expireStaleProxyLeases(env: Env): Promise<void> {
  await env.DB.prepare(
    `UPDATE proxy_leases
     SET status = 'expired'
     WHERE status = 'active' AND expires_at <= ?`
  )
    .bind(nowMs())
    .run();
}

async function leaseResidentialProxy(
  env: Env,
  payload: Record<string, unknown>
): Promise<{ ok: true; lease: ProxyLeaseRow } | { ok: false; error: string; status: number }> {
  const userId = cleanString(payload.user_id, 120);
  if (!userId) {
    return { ok: false, error: "user_id required.", status: 400 };
  }

  const desiredCountry = normalizeProxyGeo(payload.country, 2);
  const desiredRegion = normalizeProxyGeo(payload.region, 120);
  const desiredMetro = normalizeProxyGeo(payload.metro_area ?? payload.metro, 120);
  const keyword = cleanString(payload.keyword, 300) || null;
  const durationMinutes = clampInt(payload.duration_minutes, 5, 180, 30);
  const now = nowMs();

  const candidates = await listResidentialProxyAvailability(env, {
    country: desiredCountry,
    region: desiredRegion,
    metro_area: desiredMetro,
  });
  if (candidates.length < 1) {
    return { ok: false, error: "no_proxy_available_for_requested_geo", status: 404 };
  }

  let selected: Record<string, unknown> | null = null;
  for (const row of candidates) {
    const maxLeases = clampInt(row.max_concurrent_leases, 1, 200, 1);
    const activeLeases = clampInt(row.active_leases, 0, 200, 0);
    if (activeLeases < maxLeases) {
      selected = row;
      break;
    }
  }
  if (!selected) {
    return { ok: false, error: "all_matching_proxies_are_currently_leased", status: 429 };
  }

  const leaseId = uuid("lease");
  const expiresAt = now + durationMinutes * 60_000;
  const proxyId = cleanString(selected.proxy_id, 120);
  const hourlyRateUsd = Math.max(0, Number(selected.hourly_rate_usd ?? 0) || 0);

  await env.DB.prepare(
    `INSERT INTO proxy_leases (
      lease_id, proxy_id, user_id, keyword, metro_area, status,
      leased_at, expires_at, released_at, hourly_rate_usd
    ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, NULL, ?)`
  )
    .bind(
      leaseId,
      proxyId,
      userId,
      keyword,
      desiredMetro || normalizeProxyGeo(selected.metro_area, 120) || null,
      now,
      expiresAt,
      hourlyRateUsd
    )
    .run();

  return {
    ok: true,
    lease: {
      lease_id: leaseId,
      proxy_id: proxyId,
      user_id: userId,
      keyword,
      metro_area: desiredMetro || normalizeProxyGeo(selected.metro_area, 120) || null,
      status: "active",
      leased_at: now,
      expires_at: expiresAt,
      released_at: null,
      hourly_rate_usd: hourlyRateUsd,
      proxy_url: cleanString(selected.proxy_url, 2000),
      country: normalizeProxyGeo(selected.country, 2) || null,
      region: normalizeProxyGeo(selected.region, 120) || null,
    },
  };
}

async function getActiveProxyLease(env: Env, leaseId: string, userId: string): Promise<ProxyLeaseRow | null> {
  const row = await env.DB.prepare(
    `SELECT
      pl.lease_id,
      pl.proxy_id,
      pl.user_id,
      pl.keyword,
      pl.metro_area,
      pl.status,
      pl.leased_at,
      pl.expires_at,
      pl.released_at,
      pl.hourly_rate_usd,
      rp.proxy_url,
      rp.country,
      rp.region
    FROM proxy_leases pl
    JOIN residential_proxies rp ON rp.proxy_id = pl.proxy_id
    WHERE pl.lease_id = ?
      AND pl.user_id = ?
      AND pl.status = 'active'
      AND pl.expires_at > ?
    LIMIT 1`
  )
    .bind(leaseId, userId, nowMs())
    .first<Record<string, unknown>>();

  if (!row) {
    return null;
  }
  return {
    lease_id: cleanString(row.lease_id, 120),
    proxy_id: cleanString(row.proxy_id, 120),
    user_id: cleanString(row.user_id, 120),
    keyword: cleanString(row.keyword, 300) || null,
    metro_area: cleanString(row.metro_area, 120) || null,
    status: "active",
    leased_at: clampInt(row.leased_at, 0, Number.MAX_SAFE_INTEGER, 0),
    expires_at: clampInt(row.expires_at, 0, Number.MAX_SAFE_INTEGER, 0),
    released_at: row.released_at ? clampInt(row.released_at, 0, Number.MAX_SAFE_INTEGER, 0) : null,
    hourly_rate_usd: Math.max(0, Number(row.hourly_rate_usd ?? 0) || 0),
    proxy_url: cleanString(row.proxy_url, 2000),
    country: cleanString(row.country, 2) || null,
    region: cleanString(row.region, 120) || null,
  };
}

async function releaseProxyLease(
  env: Env,
  leaseId: string,
  userId: string
): Promise<{ ok: true } | { ok: false; status: number; error: string }> {
  const result = await env.DB.prepare(
    `UPDATE proxy_leases
     SET status = 'released', released_at = ?
     WHERE lease_id = ? AND user_id = ? AND status = 'active'`
  )
    .bind(nowMs(), leaseId, userId)
    .run();

  const changed = Number(result.meta?.changes ?? 0);
  if (changed < 1) {
    return { ok: false, status: 404, error: "active_lease_not_found" };
  }
  return { ok: true };
}

function parseOrganicCandidate(raw: unknown, fallbackRank: number): SerpResultRow | null {
  const item = parseJsonObject(raw);
  if (!item) {
    return null;
  }

  const candidateUrl = cleanString(item.url ?? item.link ?? item.targetUrl ?? item.displayedUrl, 2000);
  if (!candidateUrl) {
    return null;
  }

  const parsedRank = clampInt(item.rank ?? item.position ?? fallbackRank, 1, 1000, fallbackRank);
  const title = cleanString(item.title ?? item.headline ?? "", 400);
  const snippet = cleanString(item.description ?? item.snippet ?? "", 1000);
  const domain = normalizeDomain(candidateUrl) || cleanString(item.domain ?? item.hostname ?? "", 200);

  return {
    rank: parsedRank,
    url: candidateUrl,
    domain,
    title: title || null,
    snippet: snippet || null,
  };
}

function parseSerpResultsFromApifyPayload(payload: unknown, maxResults: number): SerpResultRow[] {
  const rows: SerpResultRow[] = [];
  const seenUrls = new Set<string>();

  const pushRow = (candidate: SerpResultRow | null) => {
    if (!candidate) return;
    if (!candidate.url || seenUrls.has(candidate.url)) return;
    seenUrls.add(candidate.url);
    rows.push(candidate);
  };

  const list = Array.isArray(payload) ? payload : [payload];
  for (const entry of list) {
    if (rows.length >= maxResults) break;
    const item = parseJsonObject(entry);
    if (!item) continue;

    const arraysToScan: unknown[] = [];
    if (Array.isArray(item.organicResults)) arraysToScan.push(item.organicResults);
    if (Array.isArray(item.organic_results)) arraysToScan.push(item.organic_results);
    if (Array.isArray(item.results)) arraysToScan.push(item.results);
    if (parseJsonObject(item.results) && Array.isArray((item.results as Record<string, unknown>).organic)) {
      arraysToScan.push((item.results as Record<string, unknown>).organic as unknown[]);
    }

    if (arraysToScan.length > 0) {
      for (const arr of arraysToScan) {
        const organic = Array.isArray(arr) ? arr : [];
        for (const result of organic) {
          if (rows.length >= maxResults) break;
          pushRow(parseOrganicCandidate(result, rows.length + 1));
        }
      }
      continue;
    }

    pushRow(parseOrganicCandidate(item, rows.length + 1));
  }

  rows.sort((a, b) => a.rank - b.rank);
  return rows.slice(0, maxResults).map((row, idx) => ({
    ...row,
    rank: idx + 1,
  }));
}

async function fetchGoogleTop20ViaApify(
  env: Env,
  phrase: string,
  region: Record<string, unknown>,
  device: "mobile" | "desktop",
  maxResults = SERP_MAX_RESULTS,
  proxyUrl: string | null = null
): Promise<{
  rows: SerpResultRow[];
  provider: string;
  actor: string;
  runId: string | null;
  rawPayloadSha256: string;
}> {
  const token = cleanString(env.APIFY_TOKEN ?? "", 500);
  if (!token) {
    throw new Error("APIFY_TOKEN not configured");
  }

  const actor = cleanString(env.APIFY_GOOGLE_ACTOR ?? "apify/google-search-scraper", 200);
  const endpoint = new URL(`https://api.apify.com/v2/acts/${encodeURIComponent(actor)}/run-sync-get-dataset-items`);
  endpoint.searchParams.set("token", token);

  const inputPayload: Record<string, unknown> = {
    queries: phrase,
    maxPagesPerQuery: 1,
    maxResults,
    resultsPerPage: maxResults,
    mobileResults: device === "mobile",
  };

  const country = cleanString(region.country ?? "", 2).toUpperCase();
  const language = cleanString(region.language ?? "", 2).toLowerCase();
  if (country) inputPayload.countryCode = country;
  if (language) inputPayload.languageCode = language;
  if (proxyUrl) {
    inputPayload.proxyConfiguration = {
      useApifyProxy: false,
      proxyUrls: [proxyUrl],
    };
  }

  const response = await fetch(endpoint.toString(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(inputPayload),
  });
  if (!response.ok) {
    const raw = await response.text();
    throw new Error(`Apify request failed (${response.status}): ${raw.slice(0, 400)}`);
  }

  const rawPayload = await response.text();
  let payload: unknown = null;
  try {
    payload = JSON.parse(rawPayload);
  } catch {
    throw new Error("Apify payload parse failed: invalid JSON.");
  }
  const rows = parseSerpResultsFromApifyPayload(payload, maxResults);
  return {
    rows,
    provider: "apify-headless-chrome",
    actor,
    runId: null,
    rawPayloadSha256: await sha256Hex(rawPayload),
  };
}

async function insertSerpRun(
  env: Env,
  input: {
    serpId: string;
    userId: string;
    phrase: string;
    regionJson: string;
    device: string;
    engine: string;
    provider: string;
    actor: string;
    runId: string | null;
    status: "pending" | "ok" | "error";
    error: string | null;
    keywordNorm: string;
    regionKey: string;
    deviceKey: string;
    serpKey: string;
    parserVersion: string;
    rawPayloadSha256: string | null;
    extractorMode: string;
  }
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO serp_runs (
      serp_id, user_id, phrase, region_json, device, engine, provider, actor,
      run_id, status, error,
      keyword_norm, region_key, device_key, serp_key,
      parser_version, raw_payload_sha256, extractor_mode,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      input.serpId,
      input.userId,
      input.phrase,
      input.regionJson,
      input.device,
      input.engine,
      input.provider,
      input.actor,
      input.runId,
      input.status,
      input.error,
      input.keywordNorm,
      input.regionKey,
      input.deviceKey,
      input.serpKey,
      input.parserVersion,
      input.rawPayloadSha256,
      input.extractorMode,
      nowMs()
    )
    .run();
}

async function updateSerpRunStatus(
  env: Env,
  serpId: string,
  status: "ok" | "error",
  error: string | null,
  runId: string | null,
  extras?: {
    parserVersion?: string | null;
    rawPayloadSha256?: string | null;
    extractorMode?: string | null;
  }
): Promise<void> {
  await env.DB.prepare(
    `UPDATE serp_runs
     SET status = ?,
         error = ?,
         run_id = ?,
         parser_version = COALESCE(?, parser_version),
         raw_payload_sha256 = COALESCE(?, raw_payload_sha256),
         extractor_mode = COALESCE(?, extractor_mode)
     WHERE serp_id = ?`
  )
    .bind(
      status,
      error,
      runId,
      cleanString(extras?.parserVersion, 120) || null,
      cleanString(extras?.rawPayloadSha256, 128) || null,
      cleanString(extras?.extractorMode, 80) || null,
      serpId
    )
    .run();
}

async function insertSerpResults(env: Env, serpId: string, rows: SerpResultRow[]): Promise<void> {
  for (const row of rows) {
    await env.DB.prepare(
      "INSERT INTO serp_results (serp_id, rank, url, domain, title, snippet) VALUES (?, ?, ?, ?, ?, ?)"
    )
      .bind(serpId, row.rank, row.url, row.domain, row.title, row.snippet)
      .run();
  }
}

async function loadSerpResults(env: Env, serpId: string): Promise<SerpResultRow[]> {
  const query = await env.DB.prepare(
    "SELECT rank, url, domain, title, snippet FROM serp_results WHERE serp_id = ? ORDER BY rank ASC LIMIT 20"
  )
    .bind(serpId)
    .all<Record<string, unknown>>();
  const rows = query.results ?? [];
  return rows.map((row, idx) => ({
    rank: clampInt(row.rank, 1, 1000, idx + 1),
    url: cleanString(row.url, 2000),
    domain: cleanString(row.domain, 200),
    title: cleanString(row.title, 400) || null,
    snippet: cleanString(row.snippet, 1000) || null,
  }));
}

function normalizeKeywordForKey(keyword: string): string {
  return cleanString(keyword, 300).toLowerCase().replace(/\s+/g, " ").trim();
}

function buildRegionKey(region: Record<string, unknown>): string {
  const country = cleanString(region.country, 2).toUpperCase() || "US";
  const language = cleanString(region.language, 2).toLowerCase() || "en";
  return `${country}-${language}`;
}

async function buildSerpKey(
  keywordNorm: string,
  regionKey: string,
  device: "mobile" | "desktop",
  dayUtc: string
): Promise<string> {
  return await sha256Hex(`${keywordNorm}|${regionKey}|${device}|${dayUtc}`);
}

type SerpCollectionInput = {
  userId: string;
  phrase: string;
  region: Record<string, unknown>;
  device: "mobile" | "desktop";
  maxResults: number;
  proxyUrl: string | null;
};

type SerpCollectionResult =
  | {
      ok: true;
      serpId: string;
      regionJson: string;
      rows: SerpResultRow[];
      provider: string;
      actor: string;
      runId: string | null;
      rawPayloadSha256: string;
    }
  | {
      ok: false;
      serpId: string;
      error: string;
    };

async function collectAndPersistSerpTop20(env: Env, input: SerpCollectionInput): Promise<SerpCollectionResult> {
  const serpId = uuid("serp");
  const regionJson = JSON.stringify(input.region);
  const keywordNorm = normalizeKeywordForKey(input.phrase);
  const regionKey = buildRegionKey(input.region);
  const deviceKey = input.device;
  const serpKey = await buildSerpKey(keywordNorm, regionKey, deviceKey, toDateYYYYMMDD(nowMs()));

  await insertSerpRun(env, {
    serpId,
    userId: input.userId,
    phrase: input.phrase,
    regionJson,
    device: input.device,
    engine: "google",
    provider: "apify-headless-chrome",
    actor: cleanString(env.APIFY_GOOGLE_ACTOR ?? "apify/google-search-scraper", 200),
    runId: null,
    status: "pending",
    error: null,
    keywordNorm,
    regionKey,
    deviceKey,
    serpKey,
    parserVersion: "apify-google-v1",
    rawPayloadSha256: null,
    extractorMode: "apify",
  });

  try {
    const collected = await fetchGoogleTop20ViaApify(
      env,
      input.phrase,
      input.region,
      input.device,
      input.maxResults,
      input.proxyUrl
    );
    if (collected.rows.length < 1) {
      await updateSerpRunStatus(env, serpId, "error", "no_results", collected.runId, {
        parserVersion: "apify-google-v1",
        extractorMode: "apify",
      });
      return {
        ok: false,
        serpId,
        error: "No SERP rows found for keyword.",
      };
    }

    await insertSerpResults(env, serpId, collected.rows);
    await updateSerpRunStatus(env, serpId, "ok", null, collected.runId, {
      parserVersion: "apify-google-v1",
      rawPayloadSha256: collected.rawPayloadSha256,
      extractorMode: "apify",
    });
    return {
      ok: true,
      serpId,
      regionJson,
      rows: collected.rows,
      provider: collected.provider,
      actor: collected.actor,
      runId: collected.runId,
      rawPayloadSha256: collected.rawPayloadSha256,
    };
  } catch (error) {
    const message = String((error as Error)?.message ?? error).slice(0, 500);
    await updateSerpRunStatus(env, serpId, "error", message, null, {
      parserVersion: "apify-google-v1",
      extractorMode: "apify",
    });
    return {
      ok: false,
      serpId,
      error: message,
    };
  }
}

function normalizeRootDomain(input: string): string {
  const cleaned = cleanString(input, 255).toLowerCase();
  if (!cleaned) {
    return "";
  }
  const host = cleaned.replace(/^https?:\/\//, "").replace(/^www\./, "").split("/")[0];
  const parts = host.split(".").filter(Boolean);
  if (parts.length <= 2) {
    return host;
  }
  const twoLevelSuffixes = new Set([
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
  ]);
  const tail2 = parts.slice(-2).join(".");
  if (twoLevelSuffixes.has(tail2) && parts.length >= 3) {
    return parts.slice(-3).join(".");
  }
  return parts.slice(-2).join(".");
}

function parseKeywordListFromUrl(url: URL): string[] {
  const out: string[] = [];
  const push = (value: string) => {
    const cleaned = cleanString(value, 300);
    if (!cleaned) return;
    if (!out.includes(cleaned)) out.push(cleaned);
  };

  const multi = url.searchParams.getAll("keyword");
  for (const keyword of multi) {
    push(keyword);
  }

  const joined = cleanString(url.searchParams.get("keywords"), 3000);
  if (joined) {
    for (const part of joined.split(",")) {
      push(part);
    }
  }
  return out;
}

function parseBool(value: string | null, fallback = false): boolean {
  if (value == null) {
    return fallback;
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) return fallback;
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function parseBoolUnknown(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return fallback;
    return value > 0;
  }
  if (typeof value === "string") {
    return parseBool(value, fallback);
  }
  return fallback;
}

function normalizeDevice(input: unknown): "mobile" | "desktop" {
  return cleanString(input, 20).toLowerCase() === "mobile" ? "mobile" : "desktop";
}

type SerpWatchlistRow = {
  watch_id: string;
  user_id: string;
  phrase: string;
  region_json: string;
  region: Record<string, unknown>;
  device: "mobile" | "desktop";
  max_results: number;
  active: boolean;
  created_at: number;
  updated_at: number;
  last_run_at: number | null;
  last_serp_id: string | null;
  last_status: string | null;
  last_error: string | null;
};

function parseRegionJsonString(regionJson: string): Record<string, unknown> {
  if (!regionJson) {
    return normalizeRegion(null);
  }
  try {
    return normalizeRegion(JSON.parse(regionJson));
  } catch {
    return normalizeRegion(null);
  }
}

function mapSerpWatchlistRow(row: Record<string, unknown>): SerpWatchlistRow {
  const regionJson = cleanString(row.region_json, 2000);
  const rawLastRunAt = row.last_run_at;
  const lastRunAtNumber =
    rawLastRunAt == null ? null : Number.isFinite(Number(rawLastRunAt)) ? Math.round(Number(rawLastRunAt)) : null;
  return {
    watch_id: cleanString(row.watch_id, 120),
    user_id: cleanString(row.user_id, 120),
    phrase: cleanString(row.phrase, 300),
    region_json: regionJson,
    region: parseRegionJsonString(regionJson),
    device: normalizeDevice(row.device),
    max_results: clampInt(row.max_results, 1, SERP_MAX_RESULTS, SERP_MAX_RESULTS),
    active: clampInt(row.active, 0, 1, 1) === 1,
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    updated_at: clampInt(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
    last_run_at: lastRunAtNumber,
    last_serp_id: cleanString(row.last_serp_id, 120) || null,
    last_status: cleanString(row.last_status, 20) || null,
    last_error: cleanString(row.last_error, 500) || null,
  };
}

async function listSerpWatchlistRows(
  env: Env,
  options: { userId: string; activeOnly: boolean; limit: number }
): Promise<SerpWatchlistRow[]> {
  const rows = await env.DB.prepare(
    `SELECT
       watch_id,
       user_id,
       phrase,
       region_json,
       device,
       max_results,
       active,
       created_at,
       updated_at,
       last_run_at,
       last_serp_id,
       last_status,
       last_error
     FROM serp_watchlist
     WHERE (? = '' OR user_id = ?)
       AND (? = 0 OR active = 1)
     ORDER BY updated_at DESC
     LIMIT ?`
  )
    .bind(options.userId, options.userId, options.activeOnly ? 1 : 0, options.limit)
    .all<Record<string, unknown>>();

  return (rows.results ?? []).map(mapSerpWatchlistRow);
}

async function saveSerpWatchlistItem(
  env: Env,
  payload: Record<string, unknown>
): Promise<{ ok: true; item: SerpWatchlistRow } | { ok: false; status: number; error: string }> {
  const watchId = cleanString(payload.watch_id, 120);
  const userId = cleanString(payload.user_id, 120);
  const phrase = cleanString(payload.keyword ?? payload.phrase, 300);
  if (!userId || !phrase) {
    return {
      ok: false,
      status: 400,
      error: "user_id and keyword are required.",
    };
  }

  const region = normalizeRegion(payload.region);
  const regionJson = JSON.stringify(region);
  const device = normalizeDevice(payload.device);
  const maxResults = clampInt(payload.max_results, 1, SERP_MAX_RESULTS, SERP_MAX_RESULTS);
  const active = parseBoolUnknown(payload.active, true) ? 1 : 0;
  const ts = nowMs();

  if (watchId) {
    const updated = await env.DB.prepare(
      `UPDATE serp_watchlist
       SET user_id = ?,
           phrase = ?,
           region_json = ?,
           device = ?,
           max_results = ?,
           active = ?,
           updated_at = ?
       WHERE watch_id = ?`
    )
      .bind(userId, phrase, regionJson, device, maxResults, active, ts, watchId)
      .run();
    if (Number(updated.meta?.changes ?? 0) < 1) {
      return { ok: false, status: 404, error: "watch_id_not_found" };
    }
  } else {
    await env.DB.prepare(
      `INSERT INTO serp_watchlist (
         watch_id,
         user_id,
         phrase,
         region_json,
         device,
         max_results,
         active,
         created_at,
         updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(user_id, phrase, region_json, device) DO UPDATE SET
         max_results = excluded.max_results,
         active = excluded.active,
         updated_at = excluded.updated_at`
    )
      .bind(uuid("watch"), userId, phrase, regionJson, device, maxResults, active, ts, ts)
      .run();
  }

  const row = await env.DB.prepare(
    `SELECT
       watch_id,
       user_id,
       phrase,
       region_json,
       device,
       max_results,
       active,
       created_at,
       updated_at,
       last_run_at,
       last_serp_id,
       last_status,
       last_error
     FROM serp_watchlist
     WHERE (? <> '' AND watch_id = ?)
        OR (? = '' AND user_id = ? AND phrase = ? AND region_json = ? AND device = ?)
     LIMIT 1`
  )
    .bind(watchId, watchId, watchId, userId, phrase, regionJson, device)
    .first<Record<string, unknown>>();

  if (!row) {
    return {
      ok: false,
      status: 500,
      error: "watchlist_persist_failed",
    };
  }
  return {
    ok: true,
    item: mapSerpWatchlistRow(row),
  };
}

async function removeSerpWatchlistItem(
  env: Env,
  payload: Record<string, unknown>
): Promise<{ ok: true; removed: boolean } | { ok: false; status: number; error: string }> {
  const watchId = cleanString(payload.watch_id, 120);
  if (!watchId) {
    return { ok: false, status: 400, error: "watch_id required." };
  }
  const userId = cleanString(payload.user_id, 120);

  const result = await env.DB.prepare(
    `DELETE FROM serp_watchlist
     WHERE watch_id = ?
       AND (? = '' OR user_id = ?)`
  )
    .bind(watchId, userId, userId)
    .run();

  const removed = Number(result.meta?.changes ?? 0) > 0;
  return { ok: true, removed };
}

async function updateSerpWatchlistRunStatus(
  env: Env,
  watchId: string,
  input: { status: "ok" | "error"; error: string | null; serpId: string | null; lastRunAt: number }
): Promise<void> {
  await env.DB.prepare(
    `UPDATE serp_watchlist
     SET last_run_at = ?,
         last_serp_id = ?,
         last_status = ?,
         last_error = ?,
         updated_at = ?
     WHERE watch_id = ?`
  )
    .bind(input.lastRunAt, input.serpId, input.status, input.error, nowMs(), watchId)
    .run();
}

type SerpWatchlistRunSummary = {
  started_at: number;
  finished_at: number;
  day_utc: string;
  total_candidates: number;
  attempted: number;
  succeeded: number;
  failed: number;
  skipped: number;
  rows: Array<{
    watch_id: string;
    keyword: string;
    status: "ok" | "error" | "skipped";
    reason: string | null;
    serp_id: string | null;
    job_id: string | null;
  }>;
};

async function runSerpWatchlist(
  env: Env,
  input: { userId: string; watchId: string; limit: number; force: boolean }
): Promise<SerpWatchlistRunSummary> {
  const startedAt = nowMs();
  const dayUtc = toDateYYYYMMDD(startedAt);
  const allCandidates = await listSerpWatchlistRows(env, {
    userId: input.userId,
    activeOnly: true,
    limit: input.limit,
  });

  const watchIdFilter = cleanString(input.watchId, 120);
  const candidates = watchIdFilter
    ? allCandidates.filter((candidate) => candidate.watch_id === watchIdFilter)
    : allCandidates;

  const summary: SerpWatchlistRunSummary = {
    started_at: startedAt,
    finished_at: startedAt,
    day_utc: dayUtc,
    total_candidates: candidates.length,
    attempted: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    rows: [],
  };

  for (const candidate of candidates) {
    if (!input.force && candidate.last_run_at != null && toDateYYYYMMDD(candidate.last_run_at) === dayUtc) {
      summary.skipped += 1;
      summary.rows.push({
        watch_id: candidate.watch_id,
        keyword: candidate.phrase,
        status: "skipped",
        reason: "already_ran_today",
        serp_id: candidate.last_serp_id,
        job_id: null,
      });
      continue;
    }

    summary.attempted += 1;
    const region = parseRegionJsonString(candidate.region_json);
    const jobId = await createJobRecord(env, {
      userId: candidate.user_id,
      type: "serp_top20",
      request: {
        source: "watchlist",
        watch_id: candidate.watch_id,
        keyword: candidate.phrase,
        region,
        device: candidate.device,
        max_results: candidate.max_results,
      },
    });
    const collected = await collectAndPersistSerpTop20(env, {
      userId: candidate.user_id,
      phrase: candidate.phrase,
      region,
      device: candidate.device,
      maxResults: candidate.max_results,
      proxyUrl: null,
    });
    const finishedAt = nowMs();

    if (collected.ok === true) {
      summary.succeeded += 1;
      await finalizeJobSuccess(env, jobId, 1);
      await createArtifactRecord(env, {
        jobId,
        kind: "serp.raw.checksum",
        r2Key: `r2://artifacts/${jobId}/serp/raw.json`,
        payload: {
          serp_id: collected.serpId,
          raw_payload_sha256: collected.rawPayloadSha256,
          provider: collected.provider,
          actor: collected.actor,
        },
      });
      await createArtifactRecord(env, {
        jobId,
        kind: "serp.parsed.results",
        payload: {
          serp_id: collected.serpId,
          row_count: collected.rows.length,
          rows: collected.rows,
        },
      });
      await updateSerpWatchlistRunStatus(env, candidate.watch_id, {
        status: "ok",
        error: null,
        serpId: collected.serpId,
        lastRunAt: finishedAt,
      });
      summary.rows.push({
        watch_id: candidate.watch_id,
        keyword: candidate.phrase,
        status: "ok",
        reason: null,
        serp_id: collected.serpId,
        job_id: jobId,
      });
      continue;
    }

    summary.failed += 1;
    await finalizeJobFailure(env, jobId, "serp_collect_failed", {
      message: collected.error,
      serp_id: collected.serpId,
    });
    await createArtifactRecord(env, {
      jobId,
      kind: "serp.error",
      payload: {
        serp_id: collected.serpId,
        error: collected.error,
      },
    });
    await updateSerpWatchlistRunStatus(env, candidate.watch_id, {
      status: "error",
      error: collected.error,
      serpId: collected.serpId,
      lastRunAt: finishedAt,
    });
    summary.rows.push({
      watch_id: candidate.watch_id,
      keyword: candidate.phrase,
      status: "error",
      reason: collected.error,
      serp_id: collected.serpId,
      job_id: jobId,
    });
  }

  summary.finished_at = nowMs();
  return summary;
}

function toTitleCaseKeyword(input: string): string {
  const base = cleanString(input, 300).toLowerCase();
  if (!base) return "";
  const smallWords = new Set(["in", "and", "or", "of", "the", "for", "to", "a", "an", "on"]);
  const words = base.split(/\s+/g).filter(Boolean);
  return words
    .map((word, idx) => {
      const canonical = word.replace(/[^a-z0-9,/-]/g, "");
      if (canonical === "nv") return "NV";
      if (canonical === "ca") return "CA";
      if (canonical === "us") return "US";
      if (idx > 0 && smallWords.has(canonical)) return canonical;
      if (!canonical) return word;
      return canonical.charAt(0).toUpperCase() + canonical.slice(1);
    })
    .join(" ");
}

function trimToLength(input: string, maxLen: number): string {
  const value = cleanString(input, maxLen + 40);
  if (value.length <= maxLen) {
    return value;
  }
  return `${value.slice(0, Math.max(0, maxLen - 1)).trimEnd()}…`;
}

function extractLocationFromKeyword(keyword: string): string | null {
  const text = cleanString(keyword, 300);
  const inMatch = text.match(/\bin\s+([A-Za-z\s]+,\s*[A-Za-z]{2})\b/i);
  if (inMatch && inMatch[1]) {
    return inMatch[1].trim();
  }
  const nearMatch = text.match(/\bnear\s+([A-Za-z\s]+,\s*[A-Za-z]{2})\b/i);
  if (nearMatch && nearMatch[1]) {
    return nearMatch[1].trim();
  }
  return null;
}

async function loadLatestSerpRunForKeyword(
  env: Env,
  keyword: string,
  deviceFilter: string
): Promise<{ serp_id: string; phrase: string; created_at: number } | null> {
  const row = await env.DB.prepare(
    `SELECT serp_id, phrase, created_at
     FROM serp_runs
     WHERE phrase = ?
       AND status = 'ok'
       AND (? = '' OR device = ?)
     ORDER BY created_at DESC
     LIMIT 1`
  )
    .bind(keyword, deviceFilter, deviceFilter)
    .first<Record<string, unknown>>();

  if (!row) return null;
  return {
    serp_id: cleanString(row.serp_id, 120),
    phrase: cleanString(row.phrase, 300),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function loadDailyLatestSerpRows(
  env: Env,
  keyword: string,
  sinceMs: number,
  deviceFilter: string
): Promise<
  Array<{
    day: string;
    rank: number;
    url: string;
    domain: string;
    title: string | null;
    snippet: string | null;
  }>
> {
  const rows = await env.DB.prepare(
    `WITH runs AS (
       SELECT
         serp_id,
         date(created_at / 1000, 'unixepoch') AS day,
         created_at,
         ROW_NUMBER() OVER (
           PARTITION BY date(created_at / 1000, 'unixepoch')
           ORDER BY created_at DESC
         ) AS rn
       FROM serp_runs
       WHERE phrase = ?
         AND status = 'ok'
         AND created_at >= ?
         AND (? = '' OR device = ?)
     )
     SELECT
       runs.day,
       r.rank,
       r.url,
       r.domain,
       r.title,
       r.snippet
     FROM runs
     JOIN serp_results r ON r.serp_id = runs.serp_id
     WHERE runs.rn = 1
     ORDER BY runs.day ASC, r.rank ASC`
  )
    .bind(keyword, sinceMs, deviceFilter, deviceFilter)
    .all<Record<string, unknown>>();

  return (rows.results ?? []).map((row, idx) => ({
    day: cleanString(row.day, 20),
    rank: clampInt(row.rank, 1, 1000, idx + 1),
    url: cleanString(row.url, 2000),
    domain: cleanString(row.domain, 200),
    title: cleanString(row.title, 400) || null,
    snippet: cleanString(row.snippet, 1000) || null,
  }));
}

function buildSuggestedSeoCopy(
  targetKeyword: string,
  points: Array<{ title: string | null; snippet: string | null }>
): { title: string; meta_description: string; signals: string[] } {
  const haystack = points
    .map((point) => `${point.title ?? ""} ${point.snippet ?? ""}`.toLowerCase())
    .join(" ");
  const has = (patterns: string[]) => patterns.some((pattern) => haystack.includes(pattern));

  const signals: string[] = [];
  const hasSameDay = has(["same-day", "same day"]);
  const hasEmergency = has(["24/7", "24-hour", "emergency"]);
  const hasDrain = has(["drain"]);
  const hasWaterHeater = has(["water heater"]);
  const hasBackflow = has(["backflow"]);
  const hasTrust = has(["trusted", "licensed", "family-owned", "family owned", "since"]);

  if (hasSameDay) signals.push("same-day");
  if (hasEmergency) signals.push("24/7-emergency");
  if (hasDrain) signals.push("drain-cleaning");
  if (hasWaterHeater) signals.push("water-heaters");
  if (hasBackflow) signals.push("backflow");
  if (hasTrust) signals.push("trusted");

  const location = extractLocationFromKeyword(targetKeyword) ?? "your area";
  let serviceChunk = "Local Plumbing Services";
  if (hasSameDay && hasEmergency) {
    serviceChunk = "Same-Day & 24/7 Emergency Service";
  } else if (hasEmergency) {
    serviceChunk = "24/7 Emergency Plumbing Service";
  } else if (hasSameDay) {
    serviceChunk = "Same-Day Plumbing Service";
  }

  const title = trimToLength(`${toTitleCaseKeyword(targetKeyword)} | ${serviceChunk}`, 65);

  const serviceBits: string[] = [];
  if (hasSameDay) serviceBits.push("same-day repairs");
  if (hasEmergency) serviceBits.push("24/7 emergency service");
  if (hasDrain) serviceBits.push("drain cleaning");
  if (hasWaterHeater) serviceBits.push("water heater service");
  if (hasBackflow) serviceBits.push("backflow support");
  if (serviceBits.length < 1) {
    serviceBits.push("fast plumbing service");
  }
  const trustPrefix = hasTrust ? "Trusted " : "";
  const meta = trimToLength(
    `${trustPrefix}${location} plumbers offering ${serviceBits.join(", ")} for homes and businesses.`,
    160
  );

  return {
    title,
    meta_description: meta,
    signals,
  };
}

type VerifiedPluginRequest =
  | { ok: true; body: Record<string, unknown> }
  | { ok: false; response: Response };

async function verifySignedPluginRequest(req: Request, env: Env): Promise<VerifiedPluginRequest> {
  const secret = String(env.WP_PLUGIN_SHARED_SECRET || "").trim();
  if (!secret) {
    return {
      ok: false,
      response: Response.json({ ok: false, error: "WP plugin secret not configured." }, { status: 503 }),
    };
  }

  const timestampHeader = req.headers.get("x-plugin-timestamp") || "";
  const signatureHeader = req.headers.get("x-plugin-signature") || "";
  if (!timestampHeader || !signatureHeader) {
    return {
      ok: false,
      response: Response.json({ ok: false, error: "Missing plugin signature headers." }, { status: 401 }),
    };
  }

  const tsMs = parseTimestampMs(timestampHeader);
  if (!tsMs || Math.abs(nowMs() - tsMs) > PLUGIN_TIMESTAMP_WINDOW_MS) {
    return {
      ok: false,
      response: Response.json({ ok: false, error: "Invalid or expired plugin timestamp." }, { status: 401 }),
    };
  }

  const rawBody = await req.text();
  const expectedSignature = await hmacSha256Hex(secret, `${timestampHeader}.${rawBody}`);
  if (!safeEqualHex(signatureHeader, expectedSignature)) {
    return {
      ok: false,
      response: Response.json({ ok: false, error: "Invalid plugin signature." }, { status: 401 }),
    };
  }

  try {
    const parsed = JSON.parse(rawBody || "{}");
    const body = parseJsonObject(parsed);
    if (!body) {
      return {
        ok: false,
        response: Response.json({ ok: false, error: "Invalid JSON body." }, { status: 400 }),
      };
    }
    return { ok: true, body };
  } catch {
    return {
      ok: false,
      response: Response.json({ ok: false, error: "Invalid JSON body." }, { status: 400 }),
    };
  }
}

async function saveSchemaProfile(
  env: Env,
  sessionId: string,
  schemaStatus: string,
  schemaProfile: Record<string, unknown> | null,
  schemaJsonLd: string | null
) {
  const now = nowMs();
  await env.DB.prepare(
    `INSERT INTO wp_schema_profiles(session_id, schema_status, schema_profile_json, schema_jsonld, updated_at)
     VALUES(?,?,?,?,?)
     ON CONFLICT(session_id) DO UPDATE SET
       schema_status = excluded.schema_status,
       schema_profile_json = excluded.schema_profile_json,
       schema_jsonld = excluded.schema_jsonld,
       updated_at = excluded.updated_at`
  )
    .bind(
      sessionId,
      schemaStatus,
      schemaProfile ? JSON.stringify(schemaProfile) : null,
      schemaJsonLd,
      now
    )
    .run();
}

async function loadSchemaProfile(env: Env, sessionId: string): Promise<{
  schema_status: string;
  schema_profile: Record<string, unknown> | null;
  schema_jsonld: string | null;
}> {
  const row = await env.DB.prepare(
    "SELECT schema_status, schema_profile_json, schema_jsonld FROM wp_schema_profiles WHERE session_id = ?"
  )
    .bind(sessionId)
    .first();

  if (!row) {
    return { schema_status: "not_started", schema_profile: null, schema_jsonld: null };
  }

  let profile: Record<string, unknown> | null = null;
  if (typeof row.schema_profile_json === "string" && row.schema_profile_json.trim()) {
    try {
      profile = parseJsonObject(JSON.parse(row.schema_profile_json));
    } catch {
      profile = null;
    }
  }

  return {
    schema_status: String(row.schema_status || "not_started"),
    schema_profile: profile,
    schema_jsonld: typeof row.schema_jsonld === "string" ? row.schema_jsonld : null,
  };
}

async function saveRedirectProfile(
  env: Env,
  sessionId: string,
  checkedLinkCount: number,
  brokenLinkCount: number,
  redirectPaths: string[]
) {
  const now = nowMs();
  await env.DB.prepare(
    `INSERT INTO wp_redirect_profiles(session_id, checked_link_count, broken_link_count, redirect_paths_json, updated_at)
     VALUES(?,?,?,?,?)
     ON CONFLICT(session_id) DO UPDATE SET
       checked_link_count = excluded.checked_link_count,
       broken_link_count = excluded.broken_link_count,
       redirect_paths_json = excluded.redirect_paths_json,
       updated_at = excluded.updated_at`
  )
    .bind(sessionId, checkedLinkCount, brokenLinkCount, JSON.stringify(redirectPaths), now)
    .run();
}

async function loadRedirectProfile(env: Env, sessionId: string): Promise<{
  checked_link_count: number;
  broken_link_count: number;
  redirect_paths: string[];
}> {
  const row = await env.DB.prepare(
    "SELECT checked_link_count, broken_link_count, redirect_paths_json FROM wp_redirect_profiles WHERE session_id = ?"
  )
    .bind(sessionId)
    .first();

  if (!row) {
    return { checked_link_count: 0, broken_link_count: 0, redirect_paths: [] };
  }

  let redirectPaths: string[] = [];
  if (typeof row.redirect_paths_json === "string" && row.redirect_paths_json.trim()) {
    try {
      const parsed = JSON.parse(row.redirect_paths_json);
      if (Array.isArray(parsed)) {
        redirectPaths = parsed.map((item) => String(item)).filter(Boolean);
      }
    } catch {
      redirectPaths = [];
    }
  }

  return {
    checked_link_count: clampInt(row.checked_link_count, 0, 1000000, 0),
    broken_link_count: clampInt(row.broken_link_count, 0, 1000000, redirectPaths.length),
    redirect_paths: redirectPaths.slice(0, 200),
  };
}

// --- PSI extraction helpers ---
type PsiExtract = {
  performance_score: number | null;
  fcp_ms: number | null;
  lcp_ms: number | null;
  cls: number | null;
  tbt_ms: number | null;
  field_lcp_pctl: number | null;
  field_cls_pctl: number | null;
  field_inp_pctl: number | null;
  psi_fetch_time: string | null;
};

function safeNum(x: any): number | null {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : null;
}

function extractPsi(data: any): PsiExtract {
  // Lab metrics from Lighthouse audits
  const lh = data?.lighthouseResult;
  const audits = lh?.audits ?? {};
  const categories = lh?.categories ?? {};

  const performance_score = safeNum(categories?.performance?.score);

  const fcp_ms = safeNum(audits?.["first-contentful-paint"]?.numericValue);
  const lcp_ms = safeNum(audits?.["largest-contentful-paint"]?.numericValue);
  const cls = safeNum(audits?.["cumulative-layout-shift"]?.numericValue);
  const tbt_ms = safeNum(audits?.["total-blocking-time"]?.numericValue);

  // Field data (CrUX) if available
  const le = data?.loadingExperience?.metrics ?? {};
  const field_lcp_pctl = safeNum(le?.LARGEST_CONTENTFUL_PAINT_MS?.percentile);
  const field_cls_pctl = safeNum(le?.CUMULATIVE_LAYOUT_SHIFT_SCORE?.percentile);
  const field_inp_pctl = safeNum(le?.INTERACTION_TO_NEXT_PAINT?.percentile);

  const psi_fetch_time = typeof data?.analysisUTCTimestamp === "string"
    ? data.analysisUTCTimestamp
    : null;

  return {
    performance_score,
    fcp_ms,
    lcp_ms,
    cls,
    tbt_ms,
    field_lcp_pctl,
    field_cls_pctl,
    field_inp_pctl,
    psi_fetch_time,
  };
}

// --- delta thresholds ---
type SpeedDelta = {
  d_lcp_ms: number | null;
  d_cls: number | null;
  d_tbt_ms: number | null;
  d_field_lcp_pctl: number | null;
  d_field_inp_pctl: number | null;
  severity: "ok" | "warn" | "critical";
  messages: string[];
};

function diff(a: number | null, b: number | null): number | null {
  if (a == null || b == null) return null;
  return a - b; // new - old
}

function computeDelta(prev: PsiExtract | null, cur: PsiExtract): SpeedDelta {
  const d_lcp_ms = diff(cur.lcp_ms, prev?.lcp_ms ?? null);
  const d_cls = diff(cur.cls, prev?.cls ?? null);
  const d_tbt_ms = diff(cur.tbt_ms, prev?.tbt_ms ?? null);
  const d_field_lcp_pctl = diff(cur.field_lcp_pctl, prev?.field_lcp_pctl ?? null);
  const d_field_inp_pctl = diff(cur.field_inp_pctl, prev?.field_inp_pctl ?? null);

  const messages: string[] = [];
  let severity: "ok" | "warn" | "critical" = "ok";

  // thresholds
  const LCP_WARN = 300;
  const LCP_CRIT = 700;
  const CLS_WARN = 0.03;
  const CLS_CRIT = 0.10;
  const TBT_WARN = 150;
  const FIELD_LCP_WARN = 200;
  const FIELD_INP_WARN = 100;

  if (d_lcp_ms != null && d_lcp_ms >= LCP_WARN) {
    messages.push(`LCP regressed +${Math.round(d_lcp_ms)}ms`);
    severity = d_lcp_ms >= LCP_CRIT ? "critical" : "warn";
  }
  if (d_cls != null && d_cls >= CLS_WARN) {
    messages.push(`CLS regressed +${d_cls.toFixed(3)}`);
    severity = d_cls >= CLS_CRIT ? "critical" : (severity === "critical" ? "critical" : "warn");
  }
  if (d_tbt_ms != null && d_tbt_ms >= TBT_WARN) {
    messages.push(`TBT regressed +${Math.round(d_tbt_ms)}ms`);
    if (severity === "ok") severity = "warn";
  }
  if (d_field_lcp_pctl != null && d_field_lcp_pctl >= FIELD_LCP_WARN) {
    messages.push(`Field LCP percentile worsened +${Math.round(d_field_lcp_pctl)}ms`);
    if (severity === "ok") severity = "warn";
  }
  if (d_field_inp_pctl != null && d_field_inp_pctl >= FIELD_INP_WARN) {
    messages.push(`Field INP percentile worsened +${Math.round(d_field_inp_pctl)}ms`);
    if (severity === "ok") severity = "warn";
  }

  return { d_lcp_ms, d_cls, d_tbt_ms, d_field_lcp_pctl, d_field_inp_pctl, severity, messages };
}

async function getSite(env: Env, siteId: string) {
  const r = await env.DB.prepare(
    "SELECT site_id, user_id, production_url, default_strategy, last_deploy_hash, last_speed_check_at FROM sites WHERE site_id = ?"
  ).bind(siteId).first();
  return r as any | null;
}

async function getLastSnapshot(env: Env, siteId: string, strategy: string): Promise<PsiExtract | null> {
  const r = await env.DB.prepare(
    "SELECT performance_score, fcp_ms, lcp_ms, cls, tbt_ms, field_lcp_pctl, field_cls_pctl, field_inp_pctl, psi_fetch_time FROM speed_snapshots WHERE site_id = ? AND strategy = ? ORDER BY created_at DESC LIMIT 1"
  ).bind(siteId, strategy).first();
  if (!r) return null;
  return {
    performance_score: safeNum(r.performance_score),
    fcp_ms: safeNum(r.fcp_ms),
    lcp_ms: safeNum(r.lcp_ms),
    cls: safeNum(r.cls),
    tbt_ms: safeNum(r.tbt_ms),
    field_lcp_pctl: safeNum(r.field_lcp_pctl),
    field_cls_pctl: safeNum(r.field_cls_pctl),
    field_inp_pctl: safeNum(r.field_inp_pctl),
    psi_fetch_time: typeof r.psi_fetch_time === "string" ? r.psi_fetch_time : null,
  };
}

async function insertNote(env: Env, siteId: string, category: string, message: string, tags: string[]) {
  const ms = nowMs();
  await env.DB.prepare(
    "INSERT INTO seo_notes(note_id, site_id, created_at, date, note_type, category, message, tags_json) VALUES (?,?,?,?,?,?,?,?)"
  ).bind(uuid("note"), siteId, ms, toDateYYYYMMDD(ms), "auto", category, message, JSON.stringify(tags)).run();
}

async function runPsi(env: Env, url: string, strategy: "mobile" | "desktop") {
  const endpoint = new URL("https://www.googleapis.com/pagespeedonline/v5/runPagespeed");
  endpoint.searchParams.set("url", url);
  endpoint.searchParams.set("strategy", strategy);
  endpoint.searchParams.set("key", env.PAGESPEED_API_KEY);

  const r = await fetch(endpoint.toString());
  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`PSI error ${r.status}: ${txt.slice(0, 500)}`);
  }
  return await r.json();
}

async function canRunSpeed(env: Env, siteId: string): Promise<{ ok: boolean; waitMs?: number }> {
  const site = await getSite(env, siteId);
  if (!site) return { ok: false };

  const last = site.last_speed_check_at as number | null;
  if (!last) return { ok: true };

  const elapsed = nowMs() - last;
  if (elapsed >= COOLDOWN_MS) return { ok: true };

  return { ok: false, waitMs: COOLDOWN_MS - elapsed };
}

async function updateLastSpeedCheck(env: Env, siteId: string) {
  await env.DB.prepare("UPDATE sites SET last_speed_check_at = ? WHERE site_id = ?")
    .bind(nowMs(), siteId).run();
}

async function insertSnapshot(env: Env, siteId: string, strategy: string, trigger_reason: string, deploy_hash: string | null, psi: PsiExtract) {
  const snapshot_id = uuid("snap");
  await env.DB.prepare(
    `INSERT INTO speed_snapshots(
      snapshot_id, site_id, created_at, strategy, trigger_reason, deploy_hash,
      performance_score, fcp_ms, lcp_ms, cls, tbt_ms,
      field_lcp_pctl, field_cls_pctl, field_inp_pctl, psi_fetch_time
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
  ).bind(
    snapshot_id, siteId, nowMs(), strategy, trigger_reason, deploy_hash,
    psi.performance_score, psi.fcp_ms, psi.lcp_ms, psi.cls, psi.tbt_ms,
    psi.field_lcp_pctl, psi.field_cls_pctl, psi.field_inp_pctl, psi.psi_fetch_time
  ).run();
  return snapshot_id;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/proxy/admin/register") {
      const unauthorized = requireProxyAdmin(req, env);
      if (unauthorized) {
        return unauthorized;
      }

      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const saved = await registerResidentialProxy(env, body);
      if (!saved.ok) {
        return Response.json(saved, { status: 400 });
      }
      return Response.json(saved);
    }

    if (req.method === "GET" && url.pathname === "/proxy/admin/inventory") {
      const unauthorized = requireProxyAdmin(req, env);
      if (unauthorized) {
        return unauthorized;
      }
      await expireStaleProxyLeases(env);
      const inventory = await listResidentialProxyInventory(env);
      return Response.json({
        ok: true,
        count: inventory.length,
        rows: inventory,
      });
    }

    if (req.method === "GET" && url.pathname === "/proxy/availability") {
      await expireStaleProxyLeases(env);
      const country = normalizeProxyGeo(url.searchParams.get("country"), 2);
      const region = normalizeProxyGeo(url.searchParams.get("region"), 120);
      const metroArea = normalizeProxyGeo(url.searchParams.get("metro_area") ?? url.searchParams.get("metro"), 120);
      const rows = await listResidentialProxyAvailability(env, {
        country,
        region,
        metro_area: metroArea,
      });
      return Response.json({
        ok: true,
        filters: {
          country: country || null,
          region: region || null,
          metro_area: metroArea || null,
        },
        count: rows.length,
        rows,
      });
    }

    if (req.method === "POST" && url.pathname === "/proxy/lease") {
      await expireStaleProxyLeases(env);
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const userId = cleanString(body.user_id, 120);
      const durationMinutes = clampInt(body.duration_minutes, 5, 180, 30);
      const jobId = await createJobRecord(env, {
        userId: userId || null,
        type: "proxy_lease",
        request: {
          user_id: userId || null,
          keyword: cleanString(body.keyword, 300) || null,
          country: normalizeProxyGeo(body.country, 2) || null,
          region: normalizeProxyGeo(body.region, 120) || null,
          metro_area: normalizeProxyGeo(body.metro_area ?? body.metro, 120) || null,
          duration_minutes: durationMinutes,
        },
      });

      const leased = await leaseResidentialProxy(env, body);
      if ("status" in leased) {
        await finalizeJobFailure(env, jobId, leased.error, { status: leased.status, error: leased.error });
        await createArtifactRecord(env, {
          jobId,
          kind: "proxy.lease.error",
          payload: { error: leased.error, status: leased.status },
        });
        return Response.json({ ok: false, error: leased.error, job_id: jobId }, { status: leased.status });
      }
      await createArtifactRecord(env, {
        jobId,
        kind: "proxy.lease.receipt",
        payload: {
          lease_id: leased.lease.lease_id,
          proxy_id: leased.lease.proxy_id,
          user_id: leased.lease.user_id,
          keyword: leased.lease.keyword,
          metro_area: leased.lease.metro_area,
          status: leased.lease.status,
          leased_at: leased.lease.leased_at,
          expires_at: leased.lease.expires_at,
          hourly_rate_usd: leased.lease.hourly_rate_usd,
          country: leased.lease.country,
          region: leased.lease.region,
        },
      });
      await finalizeJobSuccess(env, jobId, durationMinutes);
      return Response.json({
        ok: true,
        job_id: jobId,
        lease: leased.lease,
      });
    }

    if (req.method === "GET" && url.pathname === "/proxy/lease") {
      await expireStaleProxyLeases(env);
      const leaseId = cleanString(url.searchParams.get("lease_id"), 120);
      const userId = cleanString(url.searchParams.get("user_id"), 120);
      if (!leaseId || !userId) {
        return Response.json({ ok: false, error: "lease_id and user_id required." }, { status: 400 });
      }
      const lease = await getActiveProxyLease(env, leaseId, userId);
      if (!lease) {
        return Response.json({ ok: false, error: "active_lease_not_found" }, { status: 404 });
      }
      return Response.json({ ok: true, lease });
    }

    if (req.method === "POST" && url.pathname === "/proxy/release") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const leaseId = cleanString(body.lease_id, 120);
      const userId = cleanString(body.user_id, 120);
      if (!leaseId || !userId) {
        return Response.json({ ok: false, error: "lease_id and user_id required." }, { status: 400 });
      }

      const released = await releaseProxyLease(env, leaseId, userId);
      if ("status" in released) {
        return Response.json({ ok: false, error: released.error }, { status: released.status });
      }
      return Response.json(released);
    }

    if (req.method === "POST" && (url.pathname === "/serp/google/top20" || url.pathname === "/serp/sample")) {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const userId = cleanString(body.user_id, 120);
      const phrase = cleanString(body.keyword ?? body.phrase, 300);
      if (!userId || !phrase) {
        return Response.json(
          { ok: false, error: "user_id and keyword are required." },
          { status: 400 }
        );
      }

      const region = normalizeRegion(body.region);
      const device = normalizeDevice(body.device);
      const maxResults = clampInt(body.max_results, 1, SERP_MAX_RESULTS, SERP_MAX_RESULTS);
      const serpUrl = buildGoogleQueryUrl(phrase, region);
      const proxyLeaseId = cleanString(body.proxy_lease_id, 120) || null;

      let activeLease: ProxyLeaseRow | null = null;
      if (proxyLeaseId) {
        await expireStaleProxyLeases(env);
        activeLease = await getActiveProxyLease(env, proxyLeaseId, userId);
        if (!activeLease) {
          return Response.json(
            { ok: false, error: "proxy_lease_not_found_or_expired", proxy_lease_id: proxyLeaseId },
            { status: 404 }
          );
        }
      }

      const jobId = await createJobRecord(env, {
        userId,
        type: "serp_top20",
        request: {
          keyword: phrase,
          region,
          device,
          max_results: maxResults,
          proxy_lease_id: proxyLeaseId,
        },
      });

      const collected = await collectAndPersistSerpTop20(env, {
        userId,
        phrase,
        region,
        device,
        maxResults,
        proxyUrl: activeLease?.proxy_url ?? null,
      });
      if (collected.ok === false) {
        await finalizeJobFailure(env, jobId, "serp_collect_failed", {
          message: collected.error,
          serp_id: collected.serpId,
        });
        await createArtifactRecord(env, {
          jobId,
          kind: "serp.error",
          payload: {
            serp_id: collected.serpId,
            error: collected.error,
          },
        });
        return Response.json(
          { ok: false, error: collected.error, serp_id: collected.serpId, job_id: jobId },
          { status: 502 }
        );
      }

      await createArtifactRecord(env, {
        jobId,
        kind: "serp.raw.checksum",
        r2Key: `r2://artifacts/${jobId}/serp/raw.json`,
        payload: {
          serp_id: collected.serpId,
          provider: collected.provider,
          actor: collected.actor,
          raw_payload_sha256: collected.rawPayloadSha256,
        },
      });
      await createArtifactRecord(env, {
        jobId,
        kind: "serp.parsed.results",
        payload: {
          serp_id: collected.serpId,
          columns: ["rank", "url", "domain", "title", "snippet"],
          row_count: collected.rows.length,
          rows: collected.rows,
        },
      });
      await finalizeJobSuccess(env, jobId, 1);

      return Response.json({
        ok: true,
        job_id: jobId,
        serp_id: collected.serpId,
        user_id: userId,
        keyword: phrase,
        region,
        serp_url: serpUrl,
        provider: collected.provider,
        actor: collected.actor,
        proxy_lease_id: activeLease?.lease_id ?? null,
        proxy_geo: activeLease
          ? {
              country: activeLease.country,
              region: activeLease.region,
              metro_area: activeLease.metro_area,
            }
          : null,
        columns: ["rank", "url", "domain", "title", "snippet"],
        row_count: collected.rows.length,
        rows: collected.rows,
      });
    }

    if (req.method === "GET" && url.pathname === "/serp/results") {
      const serpId = cleanString(url.searchParams.get("serp_id"), 120);
      if (!serpId) {
        return Response.json({ ok: false, error: "serp_id required." }, { status: 400 });
      }
      const rows = await loadSerpResults(env, serpId);
      return Response.json({
        ok: true,
        serp_id: serpId,
        columns: ["rank", "url", "domain", "title", "snippet"],
        row_count: rows.length,
        rows,
      });
    }

    if (req.method === "GET" && url.pathname === "/serp/graph") {
      const keyword = cleanString(url.searchParams.get("keyword"), 300);
      if (!keyword) {
        return Response.json({ ok: false, error: "keyword required." }, { status: 400 });
      }
      const days = clampInt(url.searchParams.get("days"), 1, 365, 30);
      const deviceFilter = cleanString(url.searchParams.get("device"), 20).toLowerCase();
      const sinceMs = nowMs() - days * 24 * 60 * 60 * 1000;
      const rows = await loadDailyLatestSerpRows(env, keyword, sinceMs, deviceFilter);

      const daySet = new Set<string>();
      for (const row of rows) {
        if (row.day) daySet.add(row.day);
      }
      const dayAxis = [...daySet].sort();
      const dayIndex = new Map(dayAxis.map((day, index) => [day, index]));

      const byUrl = new Map<
        string,
        {
          url: string;
          root_domain: string;
          title: string | null;
          ranks: Array<number | null>;
        }
      >();

      for (const row of rows) {
        if (!row.url || !row.day) continue;
        let slot = byUrl.get(row.url);
        if (!slot) {
          slot = {
            url: row.url,
            root_domain: normalizeRootDomain(row.domain || normalizeDomain(row.url)),
            title: row.title,
            ranks: dayAxis.map(() => null),
          };
          byUrl.set(row.url, slot);
        }
        const idx = dayIndex.get(row.day);
        if (idx == null) continue;
        const current = slot.ranks[idx];
        if (current == null || row.rank < current) {
          slot.ranks[idx] = row.rank;
        }
        if (!slot.title && row.title) {
          slot.title = row.title;
        }
      }

      const graph = [...byUrl.values()].sort((left, right) => {
        const avg = (values: Array<number | null>) => {
          const filtered = values.filter((value): value is number => typeof value === "number");
          if (filtered.length < 1) return Number.MAX_SAFE_INTEGER;
          return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
        };
        return avg(left.ranks) - avg(right.ranks);
      });

      return Response.json({
        ok: true,
        keyword,
        device: deviceFilter || null,
        days_requested: days,
        day_axis: dayAxis,
        url_series_count: graph.length,
        graph,
      });
    }

    if (req.method === "GET" && url.pathname === "/serp/domain-averages") {
      const keywords = parseKeywordListFromUrl(url);
      if (keywords.length < 1) {
        return Response.json({ ok: false, error: "keyword or keywords required." }, { status: 400 });
      }
      const deviceFilter = cleanString(url.searchParams.get("device"), 20).toLowerCase();
      const requireAllKeywords = parseBool(url.searchParams.get("require_all_keywords"), false);

      const domainMap = new Map<
        string,
        {
          root_domain: string;
          per_keyword: Record<string, number>;
        }
      >();
      const keywordsWithData: string[] = [];

      for (const keyword of keywords) {
        const run = await loadLatestSerpRunForKeyword(env, keyword, deviceFilter);
        if (!run) {
          continue;
        }
        keywordsWithData.push(keyword);
        const rows = await loadSerpResults(env, run.serp_id);
        const bestByDomain = new Map<string, number>();

        for (const row of rows) {
          const rootDomain = normalizeRootDomain(row.domain || normalizeDomain(row.url));
          if (!rootDomain) continue;
          const existing = bestByDomain.get(rootDomain);
          if (existing == null || row.rank < existing) {
            bestByDomain.set(rootDomain, row.rank);
          }
        }

        for (const [rootDomain, bestRank] of bestByDomain.entries()) {
          let aggregate = domainMap.get(rootDomain);
          if (!aggregate) {
            aggregate = {
              root_domain: rootDomain,
              per_keyword: {},
            };
            domainMap.set(rootDomain, aggregate);
          }
          aggregate.per_keyword[keyword] = bestRank;
        }
      }

      const rows = [...domainMap.values()]
        .map((entry) => {
          const ranks = Object.values(entry.per_keyword);
          const keywordCount = ranks.length;
          const avgRank =
            keywordCount < 1
              ? Number.MAX_SAFE_INTEGER
              : ranks.reduce((sum, value) => sum + value, 0) / keywordCount;
          return {
            root_domain: entry.root_domain,
            avg_rank: Number(avgRank.toFixed(3)),
            keyword_count: keywordCount,
            per_keyword: entry.per_keyword,
            appears_in_all_keywords: keywordCount === keywords.length,
          };
        })
        .filter((entry) => (requireAllKeywords ? entry.appears_in_all_keywords : true))
        .sort((left, right) => left.avg_rank - right.avg_rank);

      return Response.json({
        ok: true,
        requested_keywords: keywords,
        keywords_with_data: keywordsWithData,
        require_all_keywords: requireAllKeywords,
        best_domain: rows.length > 0 ? rows[0] : null,
        rows,
      });
    }

    if (req.method === "POST" && url.pathname === "/serp/seo-suggestion") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const targetKeyword = cleanString(body.target_keyword ?? body.keyword, 300);
      if (!targetKeyword) {
        return Response.json({ ok: false, error: "target_keyword required." }, { status: 400 });
      }

      const keywordsInput = Array.isArray(body.keywords) ? body.keywords : [];
      const keywordList = keywordsInput
        .map((value) => cleanString(value, 300))
        .filter((value) => value.length > 0);
      if (!keywordList.includes(targetKeyword)) {
        keywordList.unshift(targetKeyword);
      }

      const domainsInput = Array.isArray(body.domains) ? body.domains : [];
      const domainSet = new Set(
        domainsInput
          .map((value) => normalizeRootDomain(cleanString(value, 255)))
          .filter((value) => value.length > 0)
      );

      const maxPoints = clampInt(body.max_points, 1, 30, 6);
      const deviceFilter = cleanString(body.device, 20).toLowerCase();

      const keywordOrder = new Map(keywordList.map((value, index) => [value, index]));
      const points: Array<{
        keyword: string;
        rank: number;
        url: string;
        root_domain: string;
        title: string | null;
        snippet: string | null;
      }> = [];

      for (const keyword of keywordList) {
        const run = await loadLatestSerpRunForKeyword(env, keyword, deviceFilter);
        if (!run) continue;
        const rows = await loadSerpResults(env, run.serp_id);
        for (const row of rows) {
          const rootDomain = normalizeRootDomain(row.domain || normalizeDomain(row.url));
          if (!rootDomain) continue;
          if (domainSet.size > 0 && !domainSet.has(rootDomain)) continue;
          points.push({
            keyword,
            rank: row.rank,
            url: row.url,
            root_domain: rootDomain,
            title: row.title,
            snippet: row.snippet,
          });
        }
      }

      if (points.length < 1) {
        return Response.json(
          { ok: false, error: "no_matching_serp_data_for_keywords_and_domains" },
          { status: 404 }
        );
      }

      points.sort((left, right) => {
        const keywordDelta =
          (keywordOrder.get(left.keyword) ?? Number.MAX_SAFE_INTEGER) -
          (keywordOrder.get(right.keyword) ?? Number.MAX_SAFE_INTEGER);
        if (keywordDelta !== 0) return keywordDelta;
        return left.rank - right.rank;
      });
      const selectedPoints = points.slice(0, maxPoints);
      const suggestion = buildSuggestedSeoCopy(
        targetKeyword,
        selectedPoints.map((point) => ({ title: point.title, snippet: point.snippet }))
      );

      return Response.json({
        ok: true,
        target_keyword: targetKeyword,
        selected_domains: [...domainSet],
        points_used: selectedPoints,
        points_available: points.length,
        suggestion,
        reasoning:
          "Best-performing direct service-provider snippets were merged to produce conversion-focused title/meta copy.",
      });
    }

    if (req.method === "POST" && url.pathname === "/serp/watchlist/save") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const saved = await saveSerpWatchlistItem(env, body);
      if (saved.ok === false) {
        return Response.json({ ok: false, error: saved.error }, { status: saved.status });
      }
      return Response.json({
        ok: true,
        item: saved.item,
      });
    }

    if (req.method === "GET" && url.pathname === "/serp/watchlist") {
      const userId = cleanString(url.searchParams.get("user_id"), 120);
      const activeOnly = parseBool(url.searchParams.get("active_only"), false);
      const limit = clampInt(url.searchParams.get("limit"), 1, 500, 200);
      const rows = await listSerpWatchlistRows(env, {
        userId,
        activeOnly,
        limit,
      });
      return Response.json({
        ok: true,
        user_id: userId || null,
        active_only: activeOnly,
        count: rows.length,
        rows,
      });
    }

    if (req.method === "POST" && url.pathname === "/serp/watchlist/remove") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }

      const removed = await removeSerpWatchlistItem(env, body);
      if (removed.ok === false) {
        return Response.json({ ok: false, error: removed.error }, { status: removed.status });
      }
      return Response.json(removed);
    }

    if (req.method === "POST" && url.pathname === "/serp/watchlist/run") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      body = body ?? {};
      const userId = cleanString(body.user_id, 120);
      const watchId = cleanString(body.watch_id, 120);
      const limit = clampInt(body.limit, 1, 500, 100);
      const force = parseBoolUnknown(body.force, false);
      const summary = await runSerpWatchlist(env, {
        userId,
        watchId,
        limit,
        force,
      });
      return Response.json({
        ok: true,
        summary,
      });
    }

    if (req.method === "GET" && url.pathname === "/jobs/status") {
      const jobId = cleanString(url.searchParams.get("job_id"), 120);
      if (!jobId) {
        return Response.json({ ok: false, error: "job_id required." }, { status: 400 });
      }
      const row = await loadJobRecord(env, jobId);
      if (!row) {
        return Response.json({ ok: false, error: "job_not_found" }, { status: 404 });
      }
      return Response.json({
        ok: true,
        job: row,
      });
    }

    if (req.method === "GET" && url.pathname === "/jobs/artifacts") {
      const jobId = cleanString(url.searchParams.get("job_id"), 120);
      if (!jobId) {
        return Response.json({ ok: false, error: "job_id required." }, { status: 400 });
      }
      const rows = await loadArtifactsForJob(env, jobId);
      return Response.json({
        ok: true,
        job_id: jobId,
        count: rows.length,
        rows,
      });
    }

    if (req.method === "POST" && url.pathname === "/plugin/wp/schema/save") {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }

      const sessionId = String(verified.body.session_id || "").trim();
      if (!sessionId) {
        return Response.json({ ok: false, error: "session_id required." }, { status: 400 });
      }

      const schemaStatus = String(verified.body.schema_status || "not_started").trim().slice(0, 64) || "not_started";
      const schemaProfile = parseJsonObject(verified.body.schema_profile);
      const schemaJsonLd = typeof verified.body.schema_jsonld === "string" ? verified.body.schema_jsonld : null;

      await saveSchemaProfile(env, sessionId, schemaStatus, schemaProfile, schemaJsonLd);
      return Response.json({ ok: true, session_id: sessionId, schema_status: schemaStatus });
    }

    if (req.method === "POST" && url.pathname === "/plugin/wp/schema/profile") {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }

      const sessionId = String(verified.body.session_id || "").trim();
      if (!sessionId) {
        return Response.json({ ok: false, error: "session_id required." }, { status: 400 });
      }

      const profile = await loadSchemaProfile(env, sessionId);
      return Response.json({
        ok: true,
        session_id: sessionId,
        schema_status: profile.schema_status,
        schema_profile: profile.schema_profile,
        schema_jsonld: profile.schema_jsonld,
      });
    }

    if (req.method === "POST" && url.pathname === "/plugin/wp/redirects/save") {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }

      const sessionId = String(verified.body.session_id || "").trim();
      if (!sessionId) {
        return Response.json({ ok: false, error: "session_id required." }, { status: 400 });
      }

      const rawPaths = Array.isArray(verified.body.redirect_paths) ? verified.body.redirect_paths : [];
      const normalizedPaths: string[] = [];
      const seen = new Set<string>();
      for (const rawPath of rawPaths) {
        const normalized = normalizeRedirectPath(rawPath);
        if (!normalized || seen.has(normalized)) {
          continue;
        }
        seen.add(normalized);
        normalizedPaths.push(normalized);
        if (normalizedPaths.length >= 200) {
          break;
        }
      }

      const checkedLinkCount = clampInt(
        verified.body.checked_link_count,
        0,
        1000000,
        normalizedPaths.length
      );
      const brokenLinkCount = clampInt(
        verified.body.broken_link_count,
        0,
        1000000,
        normalizedPaths.length
      );

      await saveRedirectProfile(env, sessionId, checkedLinkCount, brokenLinkCount, normalizedPaths);
      return Response.json({
        ok: true,
        session_id: sessionId,
        checked_link_count: checkedLinkCount,
        broken_link_count: brokenLinkCount,
        redirect_paths: normalizedPaths,
      });
    }

    if (req.method === "POST" && url.pathname === "/plugin/wp/redirects/profile") {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }

      const sessionId = String(verified.body.session_id || "").trim();
      if (!sessionId) {
        return Response.json({ ok: false, error: "session_id required." }, { status: 400 });
      }

      const profile = await loadRedirectProfile(env, sessionId);
      return Response.json({
        ok: true,
        session_id: sessionId,
        checked_link_count: profile.checked_link_count,
        broken_link_count: profile.broken_link_count,
        redirect_paths: profile.redirect_paths,
      });
    }

    // POST /deploy/notify  {site_id, deploy_hash}
    if (req.method === "POST" && url.pathname === "/deploy/notify") {
      const body = await req.json();
      const site_id = String(body.site_id || "");
      const deploy_hash = String(body.deploy_hash || "");

      if (!site_id || !deploy_hash) {
        return new Response("Missing site_id/deploy_hash", { status: 400 });
      }

      const site = await getSite(env, site_id);
      if (!site) return new Response("Unknown site_id", { status: 404 });

      // Update last deploy hash
      await env.DB.prepare("UPDATE sites SET last_deploy_hash = ? WHERE site_id = ?")
        .bind(deploy_hash, site_id).run();

      // Trigger speed check if cooldown allows
      const cooldown = await canRunSpeed(env, site_id);
      if (!cooldown.ok) {
        return Response.json({ ok: true, queued: false, reason: "cooldown", wait_ms: cooldown.waitMs });
      }

      // Run speed check immediately (simple). If you want async, enqueue.
      const strategy = (site.default_strategy || "mobile").toLowerCase() === "desktop" ? "desktop" : "mobile";

      try {
        const data = await runPsi(env, site.production_url, strategy as any);
        const cur = extractPsi(data);
        const prev = await getLastSnapshot(env, site_id, strategy);
        const delta = computeDelta(prev, cur);

        await updateLastSpeedCheck(env, site_id);
        const snapshot_id = await insertSnapshot(env, site_id, strategy, "deploy", deploy_hash, cur);

        if (delta.severity !== "ok" && delta.messages.length) {
          for (const m of delta.messages) {
            await insertNote(env, site_id, "speed", `${m} (${strategy}) after deploy ${deploy_hash}`, ["speed", delta.severity, strategy, "deploy"]);
          }
        }

        return Response.json({ ok: true, queued: false, snapshot_id, strategy, delta });
      } catch (e: any) {
        return Response.json({ ok: false, error: String(e?.message ?? e) }, { status: 502 });
      }
    }

    // POST /speed/check  {site_id, trigger_reason, deploy_hash?, strategy?}
    if (req.method === "POST" && url.pathname === "/speed/check") {
      const body = await req.json();
      const site_id = String(body.site_id || "");
      const trigger_reason = String(body.trigger_reason || "manual");
      const deploy_hash = body.deploy_hash ? String(body.deploy_hash) : null;

      if (!site_id) return new Response("Missing site_id", { status: 400 });

      const site = await getSite(env, site_id);
      if (!site) return new Response("Unknown site_id", { status: 404 });

      const strategyInput = body.strategy ? String(body.strategy) : String(site.default_strategy || "mobile");
      const jobId = await createJobRecord(env, {
        userId: cleanString(site.user_id, 120) || null,
        siteId: site_id,
        type: "speed_check",
        request: {
          site_id,
          trigger_reason,
          deploy_hash,
          strategy: strategyInput,
        },
      });

      const cooldown = await canRunSpeed(env, site_id);
      if (!cooldown.ok) {
        await finalizeJobFailure(env, jobId, "cooldown", { wait_ms: cooldown.waitMs });
        await createArtifactRecord(env, {
          jobId,
          kind: "psi.cooldown",
          payload: {
            site_id,
            wait_ms: cooldown.waitMs ?? null,
          },
        });
        return Response.json({ ok: false, reason: "cooldown", wait_ms: cooldown.waitMs, job_id: jobId }, { status: 429 });
      }

      const strategy = strategyInput.toLowerCase();
      const s: "mobile" | "desktop" = strategy === "desktop" ? "desktop" : "mobile";

      try {
        const data = await runPsi(env, site.production_url, s);
        const cur = extractPsi(data);
        const prev = await getLastSnapshot(env, site_id, s);
        const delta = computeDelta(prev, cur);

        await updateLastSpeedCheck(env, site_id);
        const snapshot_id = await insertSnapshot(env, site_id, s, trigger_reason, deploy_hash, cur);

        if (delta.severity !== "ok" && delta.messages.length) {
          for (const m of delta.messages) {
            await insertNote(env, site_id, "speed", `${m} (${s})`, ["speed", delta.severity, s, trigger_reason]);
          }
        }

        await createArtifactRecord(env, {
          jobId,
          kind: "psi.raw.response",
          r2Key: `r2://artifacts/${jobId}/psi/raw.json`,
          payload: {
            strategy: s,
            checksum: await sha256Hex(safeJsonStringify(data, 256000)),
          },
        });
        await createArtifactRecord(env, {
          jobId,
          kind: "psi.metrics",
          payload: {
            site_id,
            strategy: s,
            cur,
            prev,
            delta,
            snapshot_id,
          },
        });
        await finalizeJobSuccess(env, jobId, 1);

        return Response.json({ ok: true, job_id: jobId, snapshot_id, strategy: s, delta, cur, prev });
      } catch (e: any) {
        const message = String(e?.message ?? e);
        await finalizeJobFailure(env, jobId, "psi_failed", { error: message });
        await createArtifactRecord(env, {
          jobId,
          kind: "psi.error",
          payload: {
            site_id,
            strategy: s,
            error: message,
          },
        });
        return Response.json({ ok: false, error: message, job_id: jobId }, { status: 502 });
      }
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(
    controller: { scheduledTime?: number; cron?: string },
    env: Env,
    ctx: { waitUntil(promise: Promise<unknown>): void }
  ): Promise<void> {
    const runPromise = (async () => {
      const summary = await runSerpWatchlist(env, {
        userId: "",
        watchId: "",
        limit: 500,
        force: false,
      });
      console.log(
        JSON.stringify({
          event: "serp_watchlist_daily_run",
          scheduled_time: controller.scheduledTime ?? null,
          cron: controller.cron ?? null,
          summary,
        })
      );
    })();
    ctx.waitUntil(runPromise);
    await runPromise;
  },
};
