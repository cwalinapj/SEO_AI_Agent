export interface Env {
  DB: D1Database;
  USER_MEM?: VectorizeIndex;
  MEMORY_R2?: R2Bucket;
  PAGESPEED_API_KEY: string;
  OPENAI_API_KEY?: string;
  WP_PLUGIN_SHARED_SECRET?: string;
  TASK_AGENT_SIGNING_SECRET?: string;
  MOZ_API_TOKEN?: string;
  DECODO_API_KEY?: string;
  DECODO_TASKS_URL?: string;
  SERP_PROVIDER_PRIMARY?: string;
  SERP_PROVIDER_FALLBACK?: string;
  PAGE_FETCH_PROVIDER_PRIMARY?: string;
  PAGE_FETCH_PROVIDER_FALLBACK?: string;
  PROVIDER_RETRY_MAX?: string;
  PROVIDER_RETRY_BACKOFF_MS?: string;
  APIFY_TOKEN?: string;
  APIFY_GOOGLE_ACTOR?: string;
  PROXY_CONTROL_SECRET?: string;
}

const COOLDOWN_MS = 12 * 60 * 60 * 1000; // 12 hours
const PLUGIN_TIMESTAMP_WINDOW_MS = 5 * 60 * 1000;
const SERP_MAX_RESULTS = 20;
const SITE_KEYWORD_CAP = 20;
const SITE_SERP_RESULTS_CAP = 20;
const SITE_MAX_URL_FETCHES_PER_DAY = 400;
const SITE_MAX_BACKLINK_ENRICH_PER_DAY = 400;
const SITE_MAX_GRAPH_DOMAINS_PER_DAY = 10;
const TASK_MAX_CLAIM_MINUTES = 24 * 60;
const MOZ_DEFAULT_COST_PER_1K_ROWS_USD = 5;
const DECODO_DEFAULT_TASKS_URL = "https://scraper-api.decodo.com/v1/tasks";
const MEMORY_EMBEDDING_MODEL = "text-embedding-3-small";
const MEMORY_EMBEDDING_DIMS = 1536;

type PlanDefinition = {
  key: "starter" | "growth" | "agency";
  name: string;
  monthly_usd: number;
  summary: string;
  features: string[];
  cta: string;
};

const PORTAL_PLAN_DEFINITIONS: PlanDefinition[] = [
  {
    key: "starter",
    name: "Starter",
    monthly_usd: 149,
    summary: "One site, daily SERP intelligence, and ready-to-apply tasks.",
    features: [
      "1 tracked site",
      "20 tracked keywords (10 primary + 10 secondary)",
      "Daily Step2 delta report",
      "Step3 task board with AUTO/DIY recommendations",
      "Signed WP plugin integration",
    ],
    cta: "Start Starter",
  },
  {
    key: "growth",
    name: "Growth",
    monthly_usd: 399,
    summary: "Multi-site operations with richer competitor memory and plan automation.",
    features: [
      "Up to 5 tracked sites",
      "Semantic memory layer with evidence retrieval",
      "Advanced task filtering and bulk operations",
      "Priority provider budget controls",
      "Client report portal access",
    ],
    cta: "Start Growth",
  },
  {
    key: "agency",
    name: "Agency",
    monthly_usd: 999,
    summary: "High-volume client reporting and team-assisted authority workflows.",
    features: [
      "Up to 20 tracked sites",
      "Agency-grade report cadence and history",
      "Authority gap + outreach planning",
      "Custom onboarding and execution support",
      "Delegated account operations where tier permits",
    ],
    cta: "Contact Sales",
  },
];

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

function normalizePlanKey(input: unknown): PlanDefinition["key"] | null {
  const key = cleanString(input, 40).toLowerCase();
  if (key === "starter" || key === "growth" || key === "agency") return key;
  return null;
}

function htmlEscape(input: unknown): string {
  return String(input ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderPortalHtml(hostname: string): string {
  const cards = PORTAL_PLAN_DEFINITIONS.map((plan) => {
    const features = plan.features.map((f) => `<li>${htmlEscape(f)}</li>`).join("");
    return `
      <article class="plan-card" data-plan="${plan.key}">
        <div class="plan-head">
          <h3>${htmlEscape(plan.name)}</h3>
          <p class="price">$${plan.monthly_usd}<span>/mo</span></p>
        </div>
        <p class="summary">${htmlEscape(plan.summary)}</p>
        <ul>${features}</ul>
        <button class="pick" data-plan="${plan.key}">${htmlEscape(plan.cta)}</button>
      </article>
    `;
  }).join("");

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>AIWPDev Client Portal</title>
  <style>
    :root {
      --bg: #0c1f1f;
      --panel: #102b2b;
      --ink: #f3f4ec;
      --muted: #adc4b9;
      --line: #295e58;
      --cta: #ec6f2d;
      --cta-ink: #1f120a;
      --ok: #2f9e64;
      --warn: #dc5537;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", "Sora", "Avenir Next", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 500px at 10% -20%, #1d4d4b 0, transparent 60%),
        radial-gradient(900px 500px at 90% -10%, #7b3520 0, transparent 55%),
        var(--bg);
    }
    .wrap { max-width: 1180px; margin: 0 auto; padding: 26px 20px 60px; }
    .brand { display: flex; justify-content: space-between; align-items: center; margin-bottom: 28px; }
    .brand strong { letter-spacing: .04em; font-size: 20px; }
    .brand .host { color: var(--muted); font-size: 13px; }
    h1 { margin: 0 0 10px; font-size: clamp(30px, 4vw, 52px); line-height: 1.05; }
    .lead { color: var(--muted); max-width: 770px; font-size: 17px; margin-bottom: 26px; }
    .plans { display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 16px; margin-bottom: 30px; }
    .plan-card {
      background: linear-gradient(160deg, #133533 0, #0f2929 70%);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 20px 30px rgba(0,0,0,.25);
    }
    .plan-head { display:flex; justify-content: space-between; align-items: baseline; gap: 12px; }
    .plan-head h3 { margin: 0; font-size: 24px; }
    .price { margin: 0; font-size: 27px; font-weight: 700; color: #ffe7d9; }
    .price span { color: var(--muted); font-size: 13px; margin-left: 4px; }
    .summary { color: #d8e6dd; font-size: 14px; min-height: 42px; }
    ul { margin: 12px 0 14px 20px; padding: 0; color: #d2e2d8; font-size: 14px; line-height: 1.45; }
    .pick {
      border: 0; width: 100%; padding: 11px 12px; font-weight: 700;
      background: var(--cta); color: var(--cta-ink); border-radius: 10px; cursor: pointer;
    }
    .panels { display: grid; grid-template-columns: 1.1fr .9fr; gap: 16px; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
    }
    label { display:block; font-size: 12px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin-bottom: 5px; }
    input, select, textarea {
      width: 100%; border: 1px solid #3f7770; border-radius: 10px; background: #0f2525; color: var(--ink);
      padding: 10px 11px; margin-bottom: 10px; font-family: inherit;
    }
    textarea { min-height: 86px; resize: vertical; }
    .row { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .btn {
      border: 0; padding: 10px 13px; border-radius: 10px; background: #2f9e64; color: #042212; font-weight: 700; cursor: pointer;
    }
    .btn.secondary { background: #355e91; color: #edf3ff; }
    .status { font-size: 13px; margin-top: 8px; color: var(--muted); }
    .status.ok { color: #9df0bf; }
    .status.err { color: #ffb7a8; }
    .report-box {
      margin-top: 12px; background: #0c1d1d; border: 1px solid #305e58; border-radius: 10px;
      padding: 10px; min-height: 170px; max-height: 460px; overflow: auto; font-family: "JetBrains Mono","IBM Plex Mono",monospace; font-size: 12px;
    }
    @media (max-width: 980px) {
      .plans { grid-template-columns: 1fr; }
      .panels { grid-template-columns: 1fr; }
      .row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <header class="brand">
      <strong>AIWPDev Portal</strong>
      <span class="host">${htmlEscape(hostname)}</span>
    </header>
    <h1>Client Reports And Plan Management</h1>
    <p class="lead">Customers can choose a plan, then load live Step2 and Step3 reports by site ID. This portal is built to be hosted at <code>www.aiwpdev.com</code> on the same Worker.</p>
    <section class="plans">${cards}</section>
    <section class="panels">
      <section class="panel">
        <h2>Pick A Plan</h2>
        <div class="row">
          <div><label>Site ID</label><input id="plan-site-id" placeholder="site_..." /></div>
          <div><label>Plan</label><select id="plan-key">${PORTAL_PLAN_DEFINITIONS.map((p)=>`<option value="${p.key}">${p.name} ($${p.monthly_usd}/mo)</option>`).join("")}</select></div>
        </div>
        <div class="row">
          <div><label>Company</label><input id="plan-company" placeholder="Acme Plumbing" /></div>
          <div><label>Contact Email</label><input id="plan-email" placeholder="owner@acme.com" /></div>
        </div>
        <label>Notes</label><textarea id="plan-notes" placeholder="Optional onboarding notes"></textarea>
        <button id="plan-submit" class="btn">Save Plan Selection</button>
        <p id="plan-status" class="status"></p>
      </section>
      <section class="panel">
        <h2>View Client Reports</h2>
        <div class="row">
          <div><label>Site ID</label><input id="report-site-id" placeholder="site_..." /></div>
          <div><label>Date (optional)</label><input id="report-date" placeholder="YYYY-MM-DD" /></div>
        </div>
        <div style="display:flex; gap:8px;">
          <button id="load-step2" class="btn">Load Step2</button>
          <button id="load-step3" class="btn secondary">Load Step3</button>
        </div>
        <p id="report-status" class="status"></p>
        <pre id="report-box" class="report-box"></pre>
      </section>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    for (const btn of document.querySelectorAll(".pick")) {
      btn.addEventListener("click", () => {
        const key = btn.getAttribute("data-plan");
        $("plan-key").value = key || "starter";
        window.scrollTo({ top: document.body.scrollHeight, behavior: "smooth" });
      });
    }
    $("plan-submit").addEventListener("click", async () => {
      const payload = {
        site_id: $("plan-site-id").value.trim(),
        plan_key: $("plan-key").value,
        company_name: $("plan-company").value.trim(),
        contact_email: $("plan-email").value.trim(),
        notes: $("plan-notes").value.trim()
      };
      const status = $("plan-status");
      status.textContent = "Saving...";
      status.className = "status";
      const res = await fetch("/v1/plans/select", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) {
        status.textContent = "Failed: " + (data.error || res.status);
        status.className = "status err";
        return;
      }
      status.textContent = "Plan saved for " + (data.site_id || "site");
      status.className = "status ok";
    });
    async function loadReport(kind) {
      const siteId = $("report-site-id").value.trim();
      const date = $("report-date").value.trim();
      const status = $("report-status");
      const box = $("report-box");
      if (!siteId) {
        status.textContent = "Site ID is required.";
        status.className = "status err";
        return;
      }
      const qs = date ? ("?date=" + encodeURIComponent(date)) : "";
      status.textContent = "Loading " + kind + "...";
      status.className = "status";
      const res = await fetch("/v1/sites/" + encodeURIComponent(siteId) + "/" + kind + "/report" + qs);
      const data = await res.json().catch(() => ({}));
      box.textContent = JSON.stringify(data, null, 2);
      if (!res.ok || !data.ok) {
        status.textContent = "Failed: " + (data.error || res.status);
        status.className = "status err";
        return;
      }
      status.textContent = kind.toUpperCase() + " report loaded.";
      status.className = "status ok";
    }
    $("load-step2").addEventListener("click", () => loadReport("step2"));
    $("load-step3").addEventListener("click", () => loadReport("step3"));
  </script>
</body>
</html>`;
}

function canonicalizeValue(input: unknown): unknown {
  if (input == null) return null;
  if (Array.isArray(input)) {
    return input.map((value) => canonicalizeValue(value));
  }
  if (typeof input === "object") {
    const source = parseJsonObject(input);
    if (!source) return null;
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(source).sort()) {
      out[key] = canonicalizeValue(source[key]);
    }
    return out;
  }
  if (typeof input === "string" || typeof input === "number" || typeof input === "boolean") {
    return input;
  }
  return String(input);
}

function canonicalJson(input: unknown): string {
  return JSON.stringify(canonicalizeValue(input));
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

function normalizeSerpProvider(input: unknown): SerpProvider {
  const raw = cleanString(input, 40).toLowerCase();
  if (raw === "decodo_serp_api" || raw === "decodo") return "decodo_serp_api";
  if (raw === "apify" || raw === "headless_google") return "headless_google";
  return "headless_google";
}

function normalizePageProvider(input: unknown): PageProvider {
  const raw = cleanString(input, 40).toLowerCase();
  if (raw === "decodo_web_api" || raw === "decodo") return "decodo_web_api";
  if (raw === "direct_fetch" || raw === "apify") return "direct_fetch";
  return "direct_fetch";
}

function normalizeGeoProvider(input: unknown): GeoProvider {
  return cleanString(input, 40).toLowerCase() === "decodo_geo" ? "decodo_geo" : "proxy_lease_pool";
}

async function loadSiteProviderProfile(env: Env, siteId: string): Promise<SiteProviderProfile> {
  const row = await env.DB.prepare(
    `SELECT site_id, serp_provider, page_provider, geo_provider, updated_at
     FROM site_provider_profiles
     WHERE site_id = ?
     LIMIT 1`
  )
    .bind(siteId)
    .first<Record<string, unknown>>();
  if (!row) {
    const updatedAt = Math.floor(nowMs() / 1000);
    await env.DB.prepare(
      `INSERT INTO site_provider_profiles (
        site_id, serp_provider, page_provider, geo_provider, updated_at
      ) VALUES (?, 'headless_google', 'direct_fetch', 'proxy_lease_pool', ?)`
    )
      .bind(siteId, updatedAt)
      .run();
    return {
      site_id: siteId,
      serp_provider: "headless_google",
      page_provider: "direct_fetch",
      geo_provider: "proxy_lease_pool",
      updated_at: updatedAt,
    };
  }
  return {
    site_id: cleanString(row.site_id, 120),
    serp_provider: normalizeSerpProvider(row.serp_provider),
    page_provider: normalizePageProvider(row.page_provider),
    geo_provider: normalizeGeoProvider(row.geo_provider),
    updated_at: clampInt(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function upsertSiteProviderProfile(
  env: Env,
  input: { siteId: string; serpProvider: SerpProvider; pageProvider: PageProvider; geoProvider: GeoProvider }
): Promise<SiteProviderProfile> {
  const updatedAt = Math.floor(nowMs() / 1000);
  await env.DB.prepare(
    `INSERT INTO site_provider_profiles (
      site_id, serp_provider, page_provider, geo_provider, updated_at
    ) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(site_id) DO UPDATE SET
      serp_provider = excluded.serp_provider,
      page_provider = excluded.page_provider,
      geo_provider = excluded.geo_provider,
      updated_at = excluded.updated_at`
  )
    .bind(input.siteId, input.serpProvider, input.pageProvider, input.geoProvider, updatedAt)
    .run();
  return await loadSiteProviderProfile(env, input.siteId);
}

function mapGeoForProvider(
  geoLabel: string,
  region: Record<string, unknown>,
  geoProvider: GeoProvider
): { region: Record<string, unknown>; decodoGeoLabel: string | null } {
  const out = { ...region };
  const isMetro = geoLabel.startsWith("metro:");
  const metro = isMetro ? cleanString(geoLabel.slice("metro:".length), 160) : "";
  if (geoProvider === "decodo_geo" && metro) {
    out.city = metro;
    out.region = metro;
    return { region: out, decodoGeoLabel: metro };
  }
  return { region: out, decodoGeoLabel: null };
}

function retryMax(env: Env): number {
  return clampInt(env.PROVIDER_RETRY_MAX, 1, 5, 2);
}

function retryBackoffMs(env: Env): number {
  return clampInt(env.PROVIDER_RETRY_BACKOFF_MS, 50, 5000, 250);
}

function resolveSerpPrimary(env: Env, explicit: SerpProvider | null): SerpProvider {
  if (explicit) return explicit;
  return normalizeSerpProvider(env.SERP_PROVIDER_PRIMARY || "decodo_serp_api");
}

function resolveSerpFallback(env: Env): SerpProvider | null {
  const raw = cleanString(env.SERP_PROVIDER_FALLBACK, 40);
  if (!raw) return "headless_google";
  const normalized = normalizeSerpProvider(raw);
  return normalized;
}

function resolvePagePrimary(env: Env, explicit: PageProvider | null): PageProvider {
  if (explicit) return explicit;
  return normalizePageProvider(env.PAGE_FETCH_PROVIDER_PRIMARY || "decodo_web_api");
}

function resolvePageFallback(env: Env): PageProvider | null {
  const raw = cleanString(env.PAGE_FETCH_PROVIDER_FALLBACK, 40);
  if (!raw) return "direct_fetch";
  return normalizePageProvider(raw);
}

async function sleepMs(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
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

function formatProxyGeo(lease: ProxyLeaseRow): string | null {
  const parts = [lease.metro_area, lease.region, lease.country]
    .map((v) => cleanString(v, 120))
    .filter(Boolean);
  if (parts.length < 1) return null;
  return parts.join(", ");
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

  const providerProfile = parseJsonObject(payload.provider_profile) ?? {};
  const serpProviderInput = cleanString(providerProfile.serp_provider || payload.serp_provider, 40);
  const pageProviderInput = cleanString(providerProfile.page_provider || payload.page_provider, 40);
  const geoProviderInput = cleanString(providerProfile.geo_provider || payload.geo_provider, 40);
  if (serpProviderInput || pageProviderInput || geoProviderInput) {
    await upsertSiteProviderProfile(env, {
      siteId,
      serpProvider: normalizeSerpProvider(serpProviderInput),
      pageProvider: normalizePageProvider(pageProviderInput),
      geoProvider: normalizeGeoProvider(geoProviderInput),
    });
  }

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

function parseSerpResultsFromDecodoPayload(payload: unknown, maxResults: number): SerpResultRow[] {
  const out: SerpResultRow[] = [];
  const seen = new Set<string>();
  const push = (raw: unknown, fallbackRank: number) => {
    const row = parseOrganicCandidate(raw, fallbackRank);
    if (!row) return;
    if (seen.has(row.url)) return;
    seen.add(row.url);
    out.push(row);
  };

  const root = parseJsonObject(payload);
  const candidateArrays: unknown[] = [];
  if (Array.isArray(payload)) candidateArrays.push(payload);
  if (root) {
    if (Array.isArray(root.results)) candidateArrays.push(root.results);
    if (Array.isArray(root.organic)) candidateArrays.push(root.organic);
    if (Array.isArray(root.organic_results)) candidateArrays.push(root.organic_results);
    if (Array.isArray(root.data)) candidateArrays.push(root.data);
    const parsed = parseJsonObject(root.parsed);
    if (parsed) {
      if (Array.isArray(parsed.organic)) candidateArrays.push(parsed.organic);
      if (Array.isArray(parsed.organic_results)) candidateArrays.push(parsed.organic_results);
      if (Array.isArray(parsed.results)) candidateArrays.push(parsed.results);
    }
  }

  for (const arrRaw of candidateArrays) {
    const arr = Array.isArray(arrRaw) ? arrRaw : [];
    for (const item of arr) {
      if (out.length >= maxResults) break;
      push(item, out.length + 1);
    }
    if (out.length >= maxResults) break;
  }

  out.sort((a, b) => a.rank - b.rank);
  return out.slice(0, maxResults).map((row, idx) => ({ ...row, rank: idx + 1 }));
}

async function callDecodoTask(env: Env, payload: Record<string, unknown>): Promise<{ raw: string; parsed: unknown }> {
  const token = cleanString(env.DECODO_API_KEY ?? "", 500);
  if (!token) {
    throw new Error("DECODO_API_KEY not configured");
  }
  const endpoint = cleanString(env.DECODO_TASKS_URL ?? "", 2000) || DECODO_DEFAULT_TASKS_URL;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const rawErr = await res.text();
    throw new Error(`Decodo request failed (${res.status}): ${rawErr.slice(0, 400)}`);
  }
  const raw = await res.text();
  let parsed: unknown = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Decodo response parse failed: invalid JSON.");
  }
  return { raw, parsed };
}

async function fetchGoogleTop20ViaDecodo(
  env: Env,
  phrase: string,
  region: Record<string, unknown>,
  device: "mobile" | "desktop",
  maxResults = SERP_MAX_RESULTS,
  geoLabel: string | null = null
): Promise<{
  rows: SerpResultRow[];
  provider: string;
  actor: string;
  runId: string | null;
  rawPayloadSha256: string;
}> {
  const locale = cleanString(region.country, 2).toUpperCase() || "US";
  const language = cleanString(region.language, 2).toLowerCase() || "en";
  const taskPayload: Record<string, unknown> = {
    target: "google_search",
    parse: true,
    query: phrase,
    locale: `${language}-${locale}`,
    device_type: device,
    num: Math.max(1, Math.min(SERP_MAX_RESULTS, maxResults)),
  };
  if (geoLabel) {
    taskPayload.geo = geoLabel;
    taskPayload.location = geoLabel;
  } else {
    const city = cleanString(region.city, 120) || cleanString(region.region, 120);
    if (city) taskPayload.location = city;
  }
  const decodo = await callDecodoTask(env, taskPayload);
  const rows = parseSerpResultsFromDecodoPayload(decodo.parsed, maxResults);
  const root = parseJsonObject(decodo.parsed) ?? {};
  return {
    rows,
    provider: "decodo-serp-api",
    actor: "decodo/google_search",
    runId: cleanString(root.task_id ?? root.id, 200) || null,
    rawPayloadSha256: await sha256Hex(decodo.raw),
  };
}

async function fetchPageViaDecodo(
  env: Env,
  input: { url: string; geoLabel: string | null; timeoutMs?: number }
): Promise<{ html: string | null; markdown: string | null; rawPayloadSha256: string; taskId: string | null }> {
  const taskPayload: Record<string, unknown> = {
    target: "web",
    parse: true,
    url: input.url,
    output_format: "html",
  };
  if (input.geoLabel) {
    taskPayload.geo = input.geoLabel;
    taskPayload.location = input.geoLabel;
  }
  const decodo = await callDecodoTask(env, taskPayload);
  const root = parseJsonObject(decodo.parsed) ?? {};
  const dataObj = parseJsonObject(root.data) ?? root;
  const html =
    cleanString(dataObj.html, 3_000_000) ||
    cleanString(dataObj.content, 3_000_000) ||
    cleanString(root.html, 3_000_000) ||
    null;
  const markdown =
    cleanString(dataObj.markdown, 3_000_000) ||
    cleanString(root.markdown, 3_000_000) ||
    null;
  return {
    html,
    markdown,
    rawPayloadSha256: await sha256Hex(decodo.raw),
    taskId: cleanString(root.task_id ?? root.id, 200) || null,
  };
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
    fallbackReason?: string | null;
  }
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO serp_runs (
      serp_id, user_id, phrase, region_json, device, engine, provider, actor,
      run_id, status, error,
      keyword_norm, region_key, device_key, serp_key,
      parser_version, raw_payload_sha256, extractor_mode, fallback_reason,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
      cleanString(input.fallbackReason, 300) || null,
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
    fallbackReason?: string | null;
  }
): Promise<void> {
  await env.DB.prepare(
    `UPDATE serp_runs
     SET status = ?,
         error = ?,
         run_id = ?,
         parser_version = COALESCE(?, parser_version),
         raw_payload_sha256 = COALESCE(?, raw_payload_sha256),
         extractor_mode = COALESCE(?, extractor_mode),
         fallback_reason = COALESCE(?, fallback_reason)
     WHERE serp_id = ?`
  )
    .bind(
      status,
      error,
      runId,
      cleanString(extras?.parserVersion, 120) || null,
      cleanString(extras?.rawPayloadSha256, 128) || null,
      cleanString(extras?.extractorMode, 80) || null,
      cleanString(extras?.fallbackReason, 300) || null,
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
  provider: SerpProvider | null;
  geoProvider: GeoProvider;
  geoLabel?: string | null;
  auditJobId?: string | null;
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
      extractorMode: "decodo_serp_api" | "apify";
      fallbackReason: string | null;
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

  const primaryProvider = resolveSerpPrimary(env, input.provider);
  const fallbackProvider = resolveSerpFallback(env);
  const actor =
    primaryProvider === "decodo_serp_api"
      ? "decodo/google_search"
      : cleanString(env.APIFY_GOOGLE_ACTOR ?? "apify/google-search-scraper", 200);
  const providerLabel = primaryProvider === "decodo_serp_api" ? "decodo-serp-api" : "apify-headless-chrome";
  await insertSerpRun(env, {
    serpId,
    userId: input.userId,
    phrase: input.phrase,
    regionJson,
    device: input.device,
    engine: "google",
    provider: providerLabel,
    actor,
    runId: null,
    status: "pending",
    error: null,
    keywordNorm,
    regionKey,
    deviceKey,
    serpKey,
    parserVersion: primaryProvider === "decodo_serp_api" ? "decodo-google-v1" : "apify-google-v1",
    rawPayloadSha256: null,
    extractorMode: primaryProvider === "decodo_serp_api" ? "decodo_serp_api" : "apify",
    fallbackReason: null,
  });

  try {
    let collected:
      | {
          rows: SerpResultRow[];
          provider: string;
          actor: string;
          runId: string | null;
          rawPayloadSha256: string;
          extractorMode: "decodo_serp_api" | "apify";
          fallbackReason: string | null;
        }
      | null = null;

    if (primaryProvider === "decodo_serp_api") {
      try {
        let decodo: Awaited<ReturnType<typeof fetchGoogleTop20ViaDecodo>> | null = null;
        let lastErr: string | null = null;
        for (let attempt = 1; attempt <= retryMax(env); attempt += 1) {
          try {
            decodo = await fetchGoogleTop20ViaDecodo(
              env,
              input.phrase,
              input.region,
              input.device,
              input.maxResults,
              input.geoProvider === "decodo_geo" ? cleanString(input.geoLabel, 120) || null : null
            );
            if ((decodo.rows ?? []).length > 0) break;
            lastErr = "empty_decodo_rows";
          } catch (err) {
            lastErr = cleanString((err as Error)?.message ?? err, 180) || "decodo_error";
          }
          if (attempt < retryMax(env)) {
            await sleepMs(retryBackoffMs(env) * attempt);
          }
        }
        if (!decodo || decodo.rows.length < 1) {
          throw new Error(lastErr || "decodo_failed_after_retries");
        }
        collected = {
          ...decodo,
          extractorMode: "decodo_serp_api",
          fallbackReason: null,
        };
      } catch (decodoError) {
        const fallbackReason = `decodo_failed:${cleanString((decodoError as Error)?.message ?? decodoError, 180)}`;
        if (fallbackProvider === "headless_google") {
          const apify = await fetchGoogleTop20ViaApify(
            env,
            input.phrase,
            input.region,
            input.device,
            input.maxResults,
            input.proxyUrl
          );
          collected = {
            ...apify,
            extractorMode: "apify",
            fallbackReason,
          };
        } else {
          throw decodoError;
        }
      }
    } else {
      const apify = await fetchGoogleTop20ViaApify(
        env,
        input.phrase,
        input.region,
        input.device,
        input.maxResults,
        input.proxyUrl
      );
      collected = {
        ...apify,
        extractorMode: "apify",
        fallbackReason: null,
      };
    }

    if (!collected) {
      throw new Error("serp_provider_unavailable");
    }
    if (collected.rows.length < 1) {
      await updateSerpRunStatus(env, serpId, "error", "no_results", collected.runId, {
        parserVersion: collected.extractorMode === "decodo_serp_api" ? "decodo-google-v1" : "apify-google-v1",
        extractorMode: collected.extractorMode,
      });
      return {
        ok: false,
        serpId,
        error: "No SERP rows found for keyword.",
      };
    }

    await insertSerpResults(env, serpId, collected.rows);
    await updateSerpRunStatus(env, serpId, "ok", null, collected.runId, {
      parserVersion: collected.extractorMode === "decodo_serp_api" ? "decodo-google-v1" : "apify-google-v1",
      rawPayloadSha256: collected.rawPayloadSha256,
      extractorMode: collected.extractorMode,
      fallbackReason: collected.fallbackReason,
    });
    if (input.auditJobId) {
      await createArtifactRecord(env, {
        jobId: input.auditJobId,
        kind: "serp.provider.audit",
        payload: {
          serp_id: serpId,
          provider: collected.provider,
          actor: collected.actor,
          extractor_mode: collected.extractorMode,
          fallback_reason: collected.fallbackReason,
          geo_label: cleanString(input.geoLabel, 120) || null,
          raw_payload_sha256: collected.rawPayloadSha256,
        },
      });
    }
    return {
      ok: true,
      serpId,
      regionJson,
      rows: collected.rows,
      provider: collected.provider,
      actor: collected.actor,
      runId: collected.runId,
      rawPayloadSha256: collected.rawPayloadSha256,
      extractorMode: collected.extractorMode,
      fallbackReason: collected.fallbackReason,
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

type Step1SiteRecord = {
  site_id: string;
  site_url: string;
  site_name: string | null;
  business_address: string | null;
  primary_location_hint: string | null;
  site_type_hint: string | null;
  local_mode: boolean;
  input_json: string;
  site_profile_json: string;
  last_analysis_at: number;
  last_research_at: number | null;
  created_at: number;
  updated_at: number;
};

type SerpProvider = "decodo_serp_api" | "headless_google";
type PageProvider = "decodo_web_api" | "direct_fetch";
type GeoProvider = "decodo_geo" | "proxy_lease_pool";

type SiteProviderProfile = {
  site_id: string;
  serp_provider: SerpProvider;
  page_provider: PageProvider;
  geo_provider: GeoProvider;
  updated_at: number;
};

type Step1SemrushMetric = {
  keyword: string;
  volume_us: number;
  kd: number;
  cpc: number;
  competitive_density: number;
  serp_features: string[];
  top_domains: string[];
};

type Step1KeywordCandidate = {
  keyword: string;
  keyword_norm: string;
  intent: "informational" | "commercial" | "transactional" | "navigational";
  local_intent: "yes" | "weak" | "no";
  page_type: "service" | "category" | "product" | "location landing" | "guide" | "faq" | "comparison" | "checklist";
  cluster: string;
  recommended_slug: string;
  recommended_page_type: string;
  volume_us: number;
  kd: number;
  cpc: number;
  competitive_density: number;
  serp_features: string[];
  relevance_score: number;
  intent_score: number;
  volume_score: number;
  cpc_proxy_score: number;
  winnability_score: number;
  local_boost: number;
  difficulty_penalty: number;
  serp_feature_penalty: number;
  opportunity_score: number;
  supporting_terms: string[];
};

function slugify(input: string, maxLen = 90): string {
  return cleanString(input, 400)
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, " ")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, maxLen);
}

function clamp01(v: number): number {
  if (!Number.isFinite(v)) return 0;
  if (v < 0) return 0;
  if (v > 1) return 1;
  return v;
}

function parseStringArray(input: unknown, maxItems = 200, maxLen = 300): string[] {
  if (!Array.isArray(input)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of input) {
    const value = cleanString(raw, maxLen);
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
    if (out.length >= maxItems) break;
  }
  return out;
}

function parseStringListInput(input: unknown, maxItems = 200, maxLen = 300): string[] {
  if (Array.isArray(input)) {
    return parseStringArray(input, maxItems, maxLen);
  }
  const asString = cleanString(input, maxItems * maxLen);
  if (!asString) return [];
  return parseStringArray(
    asString.split(",").map((value) => cleanString(value, maxLen)).filter(Boolean),
    maxItems,
    maxLen
  );
}

function normalizeDayString(input: unknown): string {
  const raw = cleanString(input, 20);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  return toDateYYYYMMDD(nowMs());
}

function parseMozGeoKey(input: unknown): string {
  const raw = cleanString(input, 120).toLowerCase();
  return raw || "us";
}

async function getOrCreateUrlId(env: Env, rawUrl: string): Promise<string | null> {
  const url = cleanString(rawUrl, 2000);
  if (!url) return null;
  const domain = normalizeDomain(url);
  if (!domain) return null;
  const urlHash = await sha256Hex(url.toLowerCase());
  const existing = await env.DB.prepare(
    `SELECT id FROM urls WHERE url_hash = ? LIMIT 1`
  )
    .bind(urlHash)
    .first<Record<string, unknown>>();
  if (existing) {
    return cleanString(existing.id, 120) || null;
  }
  const urlId = uuid("url");
  await env.DB.prepare(
    `INSERT INTO urls (id, url, url_hash, domain, created_at) VALUES (?, ?, ?, ?, ?)`
  )
    .bind(urlId, url, urlHash, domain, Math.floor(nowMs() / 1000))
    .run();
  return urlId;
}

function estimateMozRowBudget(input: {
  keywords?: number;
  daily_new_entrants?: number;
  daily_top_movers?: number;
  baseline_mode?: "light" | "fuller";
  cost_per_1k_rows_usd?: number;
}): {
  assumptions: Record<string, number | string>;
  daily_rows: number;
  weekly_rows: number;
  monthly_baseline_rows: number;
  daily_cost_usd: number;
  weekly_cost_usd: number;
  monthly_baseline_cost_usd: number;
} {
  const keywords = clampInt(input.keywords, 1, 500, SITE_KEYWORD_CAP);
  const newEntrants = clampInt(input.daily_new_entrants, 0, 5000, 0);
  const topMovers = clampInt(input.daily_top_movers, 0, 5000, 0);
  const baselineMode = input.baseline_mode === "fuller" ? "fuller" : "light";
  const costPer1k = Math.max(0, Number(input.cost_per_1k_rows_usd ?? MOZ_DEFAULT_COST_PER_1K_ROWS_USD) || 0);

  const dailyTop5Rows = keywords * 5;
  const dailyRows = dailyTop5Rows + newEntrants + topMovers;

  const weeklyTopUrls = keywords * 3;
  const weeklyAnchorRows = weeklyTopUrls * 20;
  const weeklyRootDomainRows = weeklyTopUrls * 50;
  const weeklyRows = weeklyAnchorRows + weeklyRootDomainRows;

  const baselineRows = baselineMode === "fuller" ? 1100 : 400;

  const cost = (rows: number) => Number(((rows / 1000) * costPer1k).toFixed(4));
  return {
    assumptions: {
      keywords,
      daily_top5_rows: dailyTop5Rows,
      daily_new_entrants: newEntrants,
      daily_top_movers: topMovers,
      weekly_tracked_top_urls: weeklyTopUrls,
      cost_per_1k_rows_usd: costPer1k,
      baseline_mode: baselineMode,
    },
    daily_rows: dailyRows,
    weekly_rows: weeklyRows,
    monthly_baseline_rows: baselineRows,
    daily_cost_usd: cost(dailyRows),
    weekly_cost_usd: cost(weeklyRows),
    monthly_baseline_cost_usd: cost(baselineRows),
  };
}

type MozProfileName = "single_site_max" | "scalable_delta";

type MozSiteProfile = {
  site_id: string;
  moz_profile: MozProfileName;
  monthly_rows_budget: number;
  weekly_focus_url_count: number;
  daily_keyword_depth: number;
  updated_at: number;
};

type MozExecutionPlan = {
  profile: MozProfileName;
  date: string;
  remaining_rows: number;
  projected_rows: number;
  degraded_mode: boolean;
  fallback_reason: string | null;
  daily: {
    keyword_depth: number;
    url_metrics_rows: number;
    competitor_target_urls: number;
    linking_root_domains_rows_per_target: number;
    anchor_rows_per_target: number;
    linking_root_domains_rows: number;
    anchor_rows: number;
  };
  weekly: {
    enabled: boolean;
    competitor_focus_urls: number;
    run_link_intersect: boolean;
  };
  monthly: {
    baseline_enabled: boolean;
    reason: string;
  };
};

async function loadMozSiteProfile(env: Env, siteId: string): Promise<MozSiteProfile> {
  const row = await env.DB.prepare(
    `SELECT
      site_id, moz_profile, monthly_rows_budget, weekly_focus_url_count, daily_keyword_depth, updated_at
     FROM moz_site_profiles
     WHERE site_id = ?
     LIMIT 1`
  )
    .bind(siteId)
    .first<Record<string, unknown>>();

  if (!row) {
    const ts = Math.floor(nowMs() / 1000);
    await env.DB.prepare(
      `INSERT INTO moz_site_profiles (
        site_id, moz_profile, monthly_rows_budget, weekly_focus_url_count, daily_keyword_depth, updated_at
      ) VALUES (?, 'single_site_max', 15000, 20, 20, ?)`
    )
      .bind(siteId, ts)
      .run();
    return {
      site_id: siteId,
      moz_profile: "single_site_max",
      monthly_rows_budget: 15000,
      weekly_focus_url_count: 20,
      daily_keyword_depth: 20,
      updated_at: ts,
    };
  }

  return {
    site_id: cleanString(row.site_id, 120),
    moz_profile:
      cleanString(row.moz_profile, 30) === "scalable_delta" ? "scalable_delta" : "single_site_max",
    monthly_rows_budget: clampInt(row.monthly_rows_budget, 0, 100_000_000, 15_000),
    weekly_focus_url_count: clampInt(row.weekly_focus_url_count, 1, 500, 20),
    daily_keyword_depth: clampInt(row.daily_keyword_depth, 1, 20, 20),
    updated_at: clampInt(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function upsertMozSiteProfile(
  env: Env,
  input: {
    siteId: string;
    mozProfile: MozProfileName;
    monthlyRowsBudget: number;
    weeklyFocusUrlCount: number;
    dailyKeywordDepth: number;
  }
): Promise<MozSiteProfile> {
  const ts = Math.floor(nowMs() / 1000);
  await env.DB.prepare(
    `INSERT INTO moz_site_profiles (
      site_id, moz_profile, monthly_rows_budget, weekly_focus_url_count, daily_keyword_depth, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(site_id) DO UPDATE SET
      moz_profile = excluded.moz_profile,
      monthly_rows_budget = excluded.monthly_rows_budget,
      weekly_focus_url_count = excluded.weekly_focus_url_count,
      daily_keyword_depth = excluded.daily_keyword_depth,
      updated_at = excluded.updated_at`
  )
    .bind(
      input.siteId,
      input.mozProfile,
      Math.max(0, input.monthlyRowsBudget),
      Math.max(1, input.weeklyFocusUrlCount),
      Math.max(1, Math.min(20, input.dailyKeywordDepth)),
      ts
    )
    .run();
  return await loadMozSiteProfile(env, input.siteId);
}

async function recordMozJobUsage(
  env: Env,
  input: {
    siteId: string | null;
    day: string;
    jobId: string;
    jobType: string;
    rowsUsed: number;
    degradedMode: boolean;
    fallbackReason: string | null;
    profile: MozProfileName | null;
  }
): Promise<void> {
  const endpoint =
    input.jobType === "moz_linking_root_domains"
      ? "root_domains"
      : input.jobType === "moz_url_metrics"
        ? "url_metrics"
        : input.jobType === "moz_anchor_text"
          ? "anchor_text"
          : input.jobType === "moz_link_intersect"
            ? "intersect"
            : input.jobType === "moz_usage_data"
              ? "usage"
              : input.jobType === "moz_index_metadata"
                ? "index_metadata"
                : input.jobType === "moz_profile_run"
                  ? "profile_run"
                  : cleanString(input.jobType, 80).toLowerCase();
  await env.DB.prepare(
    `INSERT OR REPLACE INTO moz_job_usage (
      job_id, site_id, collected_day, endpoint, rows_used, meta_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      cleanString(input.jobId, 120),
      cleanString(input.siteId, 120) || null,
      input.day,
      endpoint,
      Math.max(0, input.rowsUsed),
      safeJsonStringify(
        {
          degraded_mode: input.degradedMode,
          fallback_reason: cleanString(input.fallbackReason, 200) || null,
          profile: input.profile,
        },
        4000
      ),
      Math.floor(nowMs() / 1000)
    )
    .run();
  // Legacy compatibility table retained until old dashboards are removed.
  await env.DB.prepare(
    `INSERT INTO moz_job_row_usage (
      usage_id, site_id, collected_day, job_id, job_type, rows_used,
      degraded_mode, fallback_reason, profile, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("mozusage"),
      cleanString(input.siteId, 120) || null,
      input.day,
      cleanString(input.jobId, 120),
      cleanString(input.jobType, 80),
      Math.max(0, input.rowsUsed),
      input.degradedMode ? 1 : 0,
      cleanString(input.fallbackReason, 200) || null,
      input.profile,
      Math.floor(nowMs() / 1000)
    )
    .run();
}

async function loadMozMonthlyUsage(env: Env, siteId: string, monthPrefix: string): Promise<number> {
  const internal = await env.DB.prepare(
    `SELECT SUM(rows_used) AS total_rows
     FROM moz_job_usage
     WHERE site_id = ?
       AND collected_day LIKE ?`
  )
    .bind(siteId, `${monthPrefix}%`)
    .first<Record<string, unknown>>();
  const provider = await env.DB.prepare(
    `SELECT MAX(rows_used) AS max_rows_used
     FROM moz_usage_snapshots
     WHERE collected_day LIKE ?`
  )
    .bind(`${monthPrefix}%`)
    .first<Record<string, unknown>>();
  return Math.max(0, clampInt(internal?.total_rows, 0, 10_000_000_000, 0)) + Math.max(
    0,
    clampInt(provider?.max_rows_used, 0, 10_000_000_000, 0)
  );
}

async function loadMozMonthlyUsageBreakdown(
  env: Env,
  siteId: string,
  monthPrefix: string
): Promise<Array<{ endpoint: string; rows_used: number }>> {
  const rows = await env.DB.prepare(
    `SELECT endpoint, SUM(rows_used) AS rows_used
     FROM moz_job_usage
     WHERE site_id = ?
       AND collected_day LIKE ?
     GROUP BY endpoint
     ORDER BY rows_used DESC`
  )
    .bind(siteId, `${monthPrefix}%`)
    .all<Record<string, unknown>>();
  return (rows.results ?? []).map((row) => ({
    endpoint: cleanString(row.endpoint, 80),
    rows_used: clampInt(row.rows_used, 0, 10_000_000_000, 0),
  }));
}

async function loadCompetitorFocusUrlCount(
  env: Env,
  input: { siteId: string; day: string; desired: number }
): Promise<number> {
  const rows = await env.DB.prepare(
    `SELECT r.url, COUNT(*) AS c
     FROM step2_serp_results r
     JOIN step2_serp_snapshots s ON s.serp_id = r.serp_id
     WHERE s.site_id = ?
       AND s.date_yyyymmdd = ?
       AND r.rank <= 5
     GROUP BY r.url
     ORDER BY c DESC, MIN(r.rank) ASC
     LIMIT ?`
  )
    .bind(input.siteId, input.day, Math.max(1, input.desired))
    .all<Record<string, unknown>>();
  return (rows.results ?? []).length;
}

async function shouldRunMonthlyMozBaseline(
  env: Env,
  input: { day: string }
): Promise<{ enabled: boolean; reason: string }> {
  if (!input.day.endsWith("-01")) {
    return { enabled: false, reason: "not_month_boundary" };
  }
  const row = await env.DB.prepare(
    `SELECT index_updated_at, metadata_json
     FROM moz_index_metadata_snapshots
     ORDER BY created_at DESC
     LIMIT 1`
  ).first<Record<string, unknown>>();
  if (!row) return { enabled: true, reason: "no_index_metadata_snapshot" };
  const metadata = safeJsonParseObject(cleanString(row.metadata_json, 64000)) ?? {};
  if (parseBoolUnknown(metadata.major_refresh, false)) {
    return { enabled: true, reason: "major_refresh_flag" };
  }
  return { enabled: true, reason: "monthly_cycle" };
}

async function buildMozExecutionPlan(
  env: Env,
  input: { siteId: string; day: string; profile: MozSiteProfile; isWeekly: boolean }
): Promise<MozExecutionPlan> {
  const monthPrefix = input.day.slice(0, 7);
  const monthlyUsed = await loadMozMonthlyUsage(env, input.siteId, monthPrefix);
  const budget = Math.max(0, input.profile.monthly_rows_budget);
  const remaining = Math.max(0, budget - monthlyUsed);

  let keywordDepth = input.profile.daily_keyword_depth;
  let rdPerTarget = 50;
  let anchorPerTarget = 20;
  let intersectEnabled = input.isWeekly;
  let fallbackReason: string | null = null;

  const computeProjected = () => {
    const urlMetricsRows = SITE_KEYWORD_CAP * keywordDepth;
    const competitorTargetUrls = Math.max(1, Math.min(200, SITE_KEYWORD_CAP * 10));
    const linkingRows = competitorTargetUrls * rdPerTarget;
    const anchorRows = competitorTargetUrls * anchorPerTarget;
    const dailyRows = urlMetricsRows + linkingRows + anchorRows;
    const weeklyRows = intersectEnabled ? Math.max(1, input.profile.weekly_focus_url_count) : 0;
    return { urlMetricsRows, competitorTargetUrls, linkingRows, anchorRows, dailyRows, weeklyRows };
  };

  let projected = computeProjected();
  const totalNeeded = () => projected.dailyRows + projected.weeklyRows;

  if (totalNeeded() > remaining) {
    fallbackReason = "moz_budget_exhausted";
    keywordDepth = keywordDepth >= 20 ? 10 : keywordDepth;
    projected = computeProjected();
  }
  if (totalNeeded() > remaining) {
    keywordDepth = Math.min(keywordDepth, 5);
    projected = computeProjected();
  }
  if (totalNeeded() > remaining) {
    rdPerTarget = 25;
    projected = computeProjected();
  }
  if (totalNeeded() > remaining) {
    anchorPerTarget = 10;
    projected = computeProjected();
  }
  if (totalNeeded() > remaining) {
    intersectEnabled = false;
    projected = computeProjected();
  }

  const focusCount = await loadCompetitorFocusUrlCount(env, {
    siteId: input.siteId,
    day: input.day,
    desired: input.profile.weekly_focus_url_count,
  });
  const monthly = await shouldRunMonthlyMozBaseline(env, { day: input.day });
  return {
    profile: input.profile.moz_profile,
    date: input.day,
    remaining_rows: remaining,
    projected_rows: projected.dailyRows + projected.weeklyRows,
    degraded_mode: !!fallbackReason,
    fallback_reason: fallbackReason,
    daily: {
      keyword_depth: keywordDepth,
      url_metrics_rows: SITE_KEYWORD_CAP * keywordDepth,
      competitor_target_urls: projected.competitorTargetUrls,
      linking_root_domains_rows_per_target: rdPerTarget,
      anchor_rows_per_target: anchorPerTarget,
      linking_root_domains_rows: projected.linkingRows,
      anchor_rows: projected.anchorRows,
    },
    weekly: {
      enabled: input.isWeekly,
      competitor_focus_urls: focusCount || input.profile.weekly_focus_url_count,
      run_link_intersect: intersectEnabled && (focusCount || input.profile.weekly_focus_url_count) > 0,
    },
    monthly: {
      baseline_enabled: monthly.enabled,
      reason: monthly.reason,
    },
  };
}

async function runMozProfileForSite(
  env: Env,
  input: {
    site: Step1SiteRecord;
    day: string;
    isWeekly: boolean;
    trigger: "step2" | "cron" | "manual";
  }
): Promise<Record<string, unknown>> {
  const profile = await loadMozSiteProfile(env, input.site.site_id);
  const plan = await buildMozExecutionPlan(env, {
    siteId: input.site.site_id,
    day: input.day,
    profile,
    isWeekly: input.isWeekly,
  });

  const mozPlanJob = await createJobRecord(env, {
    siteId: input.site.site_id,
    type: "moz_profile_run",
    request: {
      profile: plan.profile,
      date: plan.date,
      trigger: input.trigger,
      plan,
    },
  });
  await createArtifactRecord(env, {
    jobId: mozPlanJob,
    kind: "moz.profile.plan",
    payload: {
      site_id: input.site.site_id,
      trigger: input.trigger,
      plan,
      fallback_reason: plan.fallback_reason,
    },
  });

  const plannedRows = plan.projected_rows;
  await recordMozJobUsage(env, {
    siteId: input.site.site_id,
    day: input.day,
    jobId: mozPlanJob,
    jobType: "moz_profile_run",
    rowsUsed: plannedRows,
    degradedMode: plan.degraded_mode,
    fallbackReason: plan.fallback_reason,
    profile: profile.moz_profile,
  });
  await finalizeJobSuccess(env, mozPlanJob, Math.max(1, plannedRows));
  return {
    site_id: input.site.site_id,
    profile: profile.moz_profile,
    plan,
    job_id: mozPlanJob,
  };
}

type MarketplaceTaskSpec = {
  title: string;
  description: string;
  platform: string;
  issuer_wallet: string;
  required_profile_fields: string[];
  required_media_hashes: Record<string, string>;
  verification_rules: Record<string, unknown>;
  payout: {
    amount: string;
    token: string;
    chain: string | null;
  };
  deadline_at: number;
  metadata: Record<string, unknown>;
};

type MarketplaceEvidenceBundle = {
  urls: string[];
  fields: Record<string, string>;
  screenshots: string[];
  trace_ref: string | null;
};

type MarketplaceVerificationResult = {
  task_id: string;
  evidence_id: string;
  passed: boolean;
  checked_at: number;
  diffs: Array<{
    rule: string;
    path: string;
    expected: string;
    actual: string;
    pass: boolean;
    message: string;
  }>;
};

function getTaskSigningSecret(env: Env): string | null {
  const direct = cleanString(env.TASK_AGENT_SIGNING_SECRET, 500);
  if (direct) return direct;
  const fallback = cleanString(env.WP_PLUGIN_SHARED_SECRET, 500);
  if (fallback) return fallback;
  return null;
}

async function signAgentPayload(env: Env, payload: string): Promise<string | null> {
  const secret = getTaskSigningSecret(env);
  if (!secret) return null;
  return await hmacSha256Hex(secret, payload);
}

function pickIdempotencyKey(req: Request, body: Record<string, unknown>): string {
  const fromHeader = cleanString(req.headers.get("idempotency-key"), 200);
  if (fromHeader) return fromHeader;
  return cleanString(body.idempotency_key, 200);
}

async function fetchIdempotencyRecord(
  env: Env,
  key: string,
  endpoint: string
): Promise<Record<string, unknown> | null> {
  return await env.DB.prepare(
    `SELECT idempotency_key, endpoint, request_hash, response_status, response_json, created_at
     FROM task_idempotency_keys
     WHERE endpoint = ? AND idempotency_key = ?
     LIMIT 1`
  )
    .bind(endpoint, key)
    .first<Record<string, unknown>>();
}

async function saveIdempotencyRecord(
  env: Env,
  input: {
    key: string;
    endpoint: string;
    requestHash: string;
    responseStatus: number;
    responseBody: unknown;
  }
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO task_idempotency_keys (
      idempotency_key, endpoint, request_hash, response_status, response_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(endpoint, idempotency_key) DO UPDATE SET
      request_hash = excluded.request_hash,
      response_status = excluded.response_status,
      response_json = excluded.response_json`
  )
    .bind(
      input.key,
      input.endpoint,
      input.requestHash,
      input.responseStatus,
      safeJsonStringify(input.responseBody, 32000),
      nowMs()
    )
    .run();
}

async function resolveIdempotency(
  env: Env,
  req: Request,
  endpoint: string,
  body: Record<string, unknown>,
  options: { required: boolean }
): Promise<
  | { ok: false; response: Response }
  | { ok: true; key: string; requestHash: string; replayResponse: Response | null }
> {
  const idempotencyKey = pickIdempotencyKey(req, body);
  if (!idempotencyKey) {
    if (options.required) {
      return {
        ok: false,
        response: Response.json({ ok: false, error: "idempotency_key_required" }, { status: 400 }),
      };
    }
    return { ok: true, key: "", requestHash: "", replayResponse: null };
  }
  const requestHash = await sha256Hex(`${endpoint}|${canonicalJson(body)}`);
  const existing = await fetchIdempotencyRecord(env, idempotencyKey, endpoint);
  if (existing) {
    const existingHash = cleanString(existing.request_hash, 1000);
    if (existingHash && !safeEqualHex(existingHash, requestHash)) {
      return {
        ok: false,
        response: Response.json({ ok: false, error: "idempotency_key_conflict" }, { status: 409 }),
      };
    }
    const status = clampInt(existing.response_status, 100, 599, 200);
    const raw = cleanString(existing.response_json, 64000);
    const parsed = safeJsonParseObject(raw) ?? {};
    return {
      ok: true,
      key: idempotencyKey,
      requestHash,
      replayResponse: Response.json(parsed, {
        status,
        headers: { "x-idempotent-replay": "1" },
      }),
    };
  }
  return { ok: true, key: idempotencyKey, requestHash, replayResponse: null };
}

async function writeTaskAuditLog(
  env: Env,
  input: {
    taskId: string;
    eventType: string;
    actorWallet: string | null;
    payload: unknown;
  }
): Promise<void> {
  const payloadJson = canonicalJson(input.payload);
  const payloadHash = await sha256Hex(payloadJson);
  await env.DB.prepare(
    `INSERT INTO task_audit_log (
      event_id, task_id, event_type, actor_wallet, payload_json, payload_hash, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("taudit"),
      input.taskId,
      cleanString(input.eventType, 80),
      cleanString(input.actorWallet, 200) || null,
      payloadJson,
      payloadHash,
      nowMs()
    )
    .run();
}

function normalizeTaskSpec(input: Record<string, unknown>): MarketplaceTaskSpec {
  const payoutRaw = parseJsonObject(input.payout) ?? {};
  const metadataRaw = parseJsonObject(input.metadata) ?? {};
  const requiredMediaHashes = parseJsonObject(input.required_media_hashes) ?? {};
  const normalizedMediaHashes: Record<string, string> = {};
  for (const [key, value] of Object.entries(requiredMediaHashes)) {
    const mediaKey = cleanString(key, 80).toLowerCase();
    const mediaHash = cleanString(value, 200).toLowerCase();
    if (!mediaKey || !mediaHash) continue;
    normalizedMediaHashes[mediaKey] = mediaHash;
  }
  return {
    title: cleanString(input.title, 200),
    description: cleanString(input.description, 3000),
    platform: cleanString(input.platform, 80).toLowerCase(),
    issuer_wallet: cleanString(input.issuer_wallet ?? input.wallet, 200).toLowerCase(),
    required_profile_fields: parseStringArray(input.required_profile_fields, 200, 120),
    required_media_hashes: normalizedMediaHashes,
    verification_rules: parseJsonObject(input.verification_rules) ?? {},
    payout: {
      amount: cleanString(payoutRaw.amount, 80),
      token: cleanString(payoutRaw.token, 80).toUpperCase(),
      chain: cleanString(payoutRaw.chain, 80) || null,
    },
    deadline_at: clampInt(input.deadline_at, 0, Number.MAX_SAFE_INTEGER, 0),
    metadata: metadataRaw,
  };
}

function validateTaskSpec(spec: MarketplaceTaskSpec): string | null {
  if (!spec.title) return "title_required";
  if (!spec.platform) return "platform_required";
  if (!spec.issuer_wallet) return "issuer_wallet_required";
  if (!spec.payout.amount || !spec.payout.token) return "payout_required";
  if (!spec.deadline_at) return "deadline_at_required";
  if (spec.deadline_at <= nowMs()) return "deadline_must_be_in_future";
  return null;
}

function normalizeEvidenceBundle(input: Record<string, unknown>): MarketplaceEvidenceBundle {
  const fieldsRaw = parseJsonObject(input.fields ?? input.structured_fields) ?? {};
  const fields: Record<string, string> = {};
  for (const [key, value] of Object.entries(fieldsRaw)) {
    const cleanKey = cleanString(key, 160);
    const cleanValue = cleanString(value, 4000);
    if (!cleanKey || !cleanValue) continue;
    fields[cleanKey] = cleanValue;
  }
  return {
    urls: parseStringArray(input.urls, 200, 2000),
    fields,
    screenshots: parseStringArray(input.screenshots ?? input.screenshot_refs, 200, 1000),
    trace_ref: cleanString(input.trace_ref, 1000) || null,
  };
}

async function evaluateEvidenceAgainstSpec(
  taskId: string,
  spec: MarketplaceTaskSpec,
  evidenceId: string,
  evidence: MarketplaceEvidenceBundle
): Promise<MarketplaceVerificationResult> {
  const diffs: MarketplaceVerificationResult["diffs"] = [];
  const add = (
    rule: string,
    path: string,
    expected: string,
    actual: string,
    pass: boolean,
    message: string
  ) => {
    diffs.push({ rule, path, expected, actual, pass, message });
  };

  for (const field of spec.required_profile_fields) {
    const actual = cleanString(evidence.fields[field], 4000);
    const pass = actual.length > 0;
    add("required_profile_field", `fields.${field}`, "non_empty", actual || "<missing>", pass, pass ? "ok" : "missing");
  }

  for (const [mediaKey, mediaHash] of Object.entries(spec.required_media_hashes)) {
    const evidenceKey = `media_hash.${mediaKey}`;
    const actual = cleanString(evidence.fields[evidenceKey], 300).toLowerCase();
    const pass = !!actual && safeEqualHex(actual, mediaHash.toLowerCase());
    add(
      "required_media_hash",
      `fields.${evidenceKey}`,
      mediaHash.toLowerCase(),
      actual || "<missing>",
      pass,
      pass ? "ok" : "hash_mismatch"
    );
  }

  const rules = parseJsonObject(spec.verification_rules) ?? {};
  const minScreenshots = clampInt(rules.min_screenshots, 0, 200, 0);
  if (minScreenshots > 0) {
    const pass = evidence.screenshots.length >= minScreenshots;
    add(
      "min_screenshots",
      "screenshots.length",
      String(minScreenshots),
      String(evidence.screenshots.length),
      pass,
      pass ? "ok" : "insufficient_screenshots"
    );
  }

  const requireTrace = parseBoolUnknown(rules.require_trace, false);
  if (requireTrace) {
    const pass = !!evidence.trace_ref;
    add("require_trace", "trace_ref", "present", evidence.trace_ref || "<missing>", pass, pass ? "ok" : "missing_trace_ref");
  }

  const deadlinePass = nowMs() <= spec.deadline_at;
  add(
    "deadline_window",
    "deadline_at",
    `<=${spec.deadline_at}`,
    String(nowMs()),
    deadlinePass,
    deadlinePass ? "ok" : "evidence_after_deadline"
  );

  const passed = diffs.every((row) => row.pass);
  return {
    task_id: taskId,
    evidence_id: evidenceId,
    passed,
    checked_at: nowMs(),
    diffs,
  };
}

function parseTopPages(input: unknown): Array<{ url: string; title: string; h1: string; meta: string }> {
  if (!Array.isArray(input)) return [];
  const out: Array<{ url: string; title: string; h1: string; meta: string }> = [];
  for (const raw of input) {
    const row = parseJsonObject(raw);
    if (!row) continue;
    out.push({
      url: cleanString(row.url, 2000),
      title: cleanString(row.title, 300),
      h1: cleanString(row.h1, 300),
      meta: cleanString(row.meta, 500),
    });
    if (out.length >= 300) break;
  }
  return out;
}

function normalizeSiteUpsertPayload(payload: Record<string, unknown>): Record<string, unknown> {
  const signals = parseJsonObject(payload.signals) ?? {};
  const plan = parseJsonObject(payload.plan) ?? {};
  const topPagesRaw = Array.isArray(signals.top_pages) ? signals.top_pages : [];

  const topPages = topPagesRaw
    .map((raw) => {
      const row = parseJsonObject(raw);
      if (!row) return null;
      return {
        url: cleanString(row.url, 2000),
        title: cleanString(row.title, 300),
        h1: cleanString(row.h1, 300),
        meta: cleanString(row.meta, 500),
        text_extract: cleanString(row.text_extract, 2000),
      };
    })
    .filter((row): row is { url: string; title: string; h1: string; meta: string; text_extract: string } => !!row)
    .slice(0, 300);

  const contentExtract = topPages
    .map((row) => row.text_extract)
    .filter((v) => cleanString(v, 2000).length > 0)
    .slice(0, 500);

  const isWoo = parseBoolUnknown(signals.is_woocommerce, false);
  const inferredSiteType =
    cleanString(payload.site_type_hint, 60) ||
    cleanString(signals.industry_hint, 60) ||
    (isWoo ? "woo" : "general");

  const normalized: Record<string, unknown> = {
    site_url: cleanString(payload.site_url, 2000),
    wp_site_id: cleanString(payload.wp_site_id, 120) || null,
    site_name: cleanString(signals.site_name ?? payload.site_name, 200),
    business_address: cleanString(
      signals.detected_address ?? payload.business_address ?? payload.primary_location_hint,
      400
    ),
    primary_location_hint: cleanString(
      payload.primary_location_hint ?? plan.metro ?? signals.detected_address ?? "",
      200
    ),
    site_type_hint: inferredSiteType,
    top_pages: topPages.map((row) => ({
      url: row.url,
      title: row.title,
      h1: row.h1,
      meta: row.meta,
    })),
    content_extract: contentExtract,
    sitemap_urls: parseStringArray(signals.sitemap_urls, 500, 2000),
    plan: {
      metro_proxy: parseBoolUnknown(plan.metro_proxy, false),
      metro: cleanString(plan.metro, 120) || null,
    },
    provider_profile: {
      serp_provider: normalizeSerpProvider(payload.serp_provider ?? plan.serp_provider),
      page_provider: normalizePageProvider(payload.page_provider ?? plan.page_provider),
      geo_provider: normalizeGeoProvider(payload.geo_provider ?? plan.geo_provider),
    },
    signals: {
      detected_phone: cleanString(signals.detected_phone, 80) || null,
      industry_hint: cleanString(signals.industry_hint, 120) || null,
      is_woocommerce: isWoo,
    },
  };

  return normalized;
}

function parseContentExtract(input: unknown): string[] {
  return parseStringArray(input, 500, 2000);
}

function tokenizeText(input: string): string[] {
  return cleanString(input, 2000)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .map((v) => v.trim())
    .filter((v) => v.length >= 2 && v.length <= 30);
}

function toKeywordNorm(input: string): string {
  return cleanString(input, 300).toLowerCase().replace(/\s+/g, " ").trim();
}

function safeJsonParseObject(input: string): Record<string, unknown> | null {
  if (!input) return null;
  try {
    return parseJsonObject(JSON.parse(input));
  } catch {
    return null;
  }
}

function parseSemrushMetrics(input: unknown): Step1SemrushMetric[] {
  if (!Array.isArray(input)) return [];
  const out: Step1SemrushMetric[] = [];
  const seen = new Set<string>();
  for (const raw of input) {
    const row = parseJsonObject(raw);
    if (!row) continue;
    const keyword = cleanString(row.keyword ?? row.kw ?? row.phrase, 300);
    if (!keyword) continue;
    const keywordNorm = toKeywordNorm(keyword);
    if (!keywordNorm || seen.has(keywordNorm)) continue;
    seen.add(keywordNorm);
    const serpFeatures = parseStringArray(
      Array.isArray(row.serp_features) ? row.serp_features : String(row.serp_features ?? "").split(","),
      30,
      80
    ).map((v) => v.toLowerCase());
    out.push({
      keyword,
      volume_us: Math.max(0, Number(row.volume_us ?? row.volume ?? 0) || 0),
      kd: Math.max(0, Math.min(100, Number(row.kd ?? row.keyword_difficulty ?? 0) || 0)),
      cpc: Math.max(0, Number(row.cpc ?? row.cost_per_click ?? 0) || 0),
      competitive_density: clamp01(Number(row.competitive_density ?? row.competition ?? 0) || 0),
      serp_features: serpFeatures,
      top_domains: parseStringArray(row.top_domains, 20, 120).map((v) => normalizeRootDomain(v)),
    });
    if (out.length >= 5000) break;
  }
  return out;
}

function detectSiteTypeHint(input: {
  siteTypeHint: string;
  siteUrl: string;
  services: string[];
  products: string[];
  categories: string[];
  tags: string[];
}): "local" | "woo" | "saas" | "publisher" | "general" {
  const hinted = cleanString(input.siteTypeHint, 60).toLowerCase();
  if (hinted.includes("local")) return "local";
  if (hinted.includes("woo") || hinted.includes("ecom") || hinted.includes("shop")) return "woo";
  if (hinted.includes("saas") || hinted.includes("software")) return "saas";
  if (hinted.includes("blog") || hinted.includes("publisher")) return "publisher";

  if (input.products.length > 2 || input.categories.length > 3) return "woo";
  if (input.tags.length > 10 && input.services.length < 3) return "publisher";
  if (/app|saas|software/.test(input.siteUrl.toLowerCase())) return "saas";
  return "general";
}

function detectLocalMode(input: {
  businessAddress: string;
  primaryLocationHint: string;
  topPages: Array<{ url: string; title: string; h1: string; meta: string }>;
  contentExtract: string[];
  siteType: string;
}): boolean {
  if (input.siteType === "local") return true;
  if (cleanString(input.businessAddress, 400)) return true;
  if (cleanString(input.primaryLocationHint, 200)) return true;
  const localSignals = /(near me|service area|local business|visit us|our location|city|county|state)/i;
  const haystack = `${input.topPages.map((p) => `${p.title} ${p.h1} ${p.meta}`).join(" ")} ${input.contentExtract
    .slice(0, 20)
    .join(" ")}`.slice(0, 12000);
  return localSignals.test(haystack);
}

function inferIndustry(offerTerms: string[], siteType: string): string {
  const joined = offerTerms.join(" ").toLowerCase();
  if (siteType === "woo") return "ecommerce";
  if (siteType === "saas") return "software";
  if (siteType === "publisher") return "media";
  if (/\b(plumbing|hvac|roof|electric|dental|lawyer|medspa|chiropractic)\b/.test(joined)) return "local_services";
  if (/\b(accounting|consulting|agency|marketing|design|development)\b/.test(joined)) return "professional_services";
  return "general_business";
}

function inferAudience(offerTerms: string[], contentExtract: string[]): string {
  const joined = `${offerTerms.join(" ")} ${contentExtract.slice(0, 20).join(" ")}`.toLowerCase();
  if (/\benterprise|b2b|agency|teams|organizations?\b/.test(joined)) return "b2b";
  if (/\bhomeowner|residential|family|consumer|shop\b/.test(joined)) return "b2c";
  return "mixed";
}

function inferPricingTier(offerTerms: string[], contentExtract: string[]): "budget" | "mid" | "premium" | "unknown" {
  const joined = `${offerTerms.join(" ")} ${contentExtract.slice(0, 20).join(" ")}`.toLowerCase();
  if (/\b(luxury|premium|white glove|high-end)\b/.test(joined)) return "premium";
  if (/\b(cheap|affordable|budget|low cost|discount)\b/.test(joined)) return "budget";
  if (/\b(pricing|quote|plans|packages)\b/.test(joined)) return "mid";
  return "unknown";
}

function inferPrimaryGoals(siteType: string, offerTerms: string[]): string[] {
  if (siteType === "woo") return ["sales", "revenue"];
  if (siteType === "publisher") return ["traffic", "newsletter_growth"];
  if (siteType === "saas") return ["trials", "demos", "pipeline"];
  if (offerTerms.length > 0) return ["leads", "calls", "bookings"];
  return ["traffic", "leads"];
}

function inferConstraints(contentExtract: string[]): string[] {
  const joined = contentExtract.slice(0, 40).join(" ").toLowerCase();
  const out: string[] = [];
  if (/\bhipaa\b/.test(joined)) out.push("hipaa");
  if (/\bgdpr\b/.test(joined)) out.push("gdpr");
  if (/\blicensed|insured\b/.test(joined)) out.push("regulated_service");
  if (/\bemergency|24\/7\b/.test(joined)) out.push("urgent_response_expectation");
  return out;
}

function inferContentMix(topPages: Array<{ url: string; title: string; h1: string; meta: string }>): string {
  let productLike = 0;
  let blogLike = 0;
  let serviceLike = 0;
  for (const page of topPages) {
    const text = `${page.url} ${page.title} ${page.h1}`.toLowerCase();
    if (/\b(product|shop|cart|category|sku)\b/.test(text)) productLike += 1;
    if (/\b(blog|post|guide|article|news)\b/.test(text)) blogLike += 1;
    if (/\b(service|location|about|contact|quote|book)\b/.test(text)) serviceLike += 1;
  }
  if (productLike > blogLike && productLike > serviceLike) return "ecommerce-heavy";
  if (blogLike > productLike && blogLike > serviceLike) return "content-heavy";
  if (serviceLike > 0) return "service-heavy";
  return "mixed";
}

function inferSiteBriefType(siteType: string): string {
  if (siteType === "local") return "local_service";
  if (siteType === "woo") return "ecommerce";
  if (siteType === "saas") return "saas";
  if (siteType === "publisher") return "publisher";
  return "general";
}

function inferBriefConfidence(input: {
  offerTerms: string[];
  locations: string[];
  topPagesCount: number;
  contentExtractCount: number;
}): number {
  let score = 0.25;
  if (input.offerTerms.length >= 5) score += 0.25;
  if (input.locations.length >= 1) score += 0.15;
  if (input.topPagesCount >= 8) score += 0.2;
  if (input.contentExtractCount >= 8) score += 0.15;
  return Number(clamp01(score).toFixed(2));
}

function buildStep1SiteProfile(input: Record<string, unknown>): Record<string, unknown> {
  const services = parseStringArray(input.services, 200, 160);
  const products = parseStringArray(input.products, 400, 160);
  const categories = parseStringArray(input.categories, 400, 160);
  const tags = parseStringArray(input.tags, 400, 80);
  const topPages = parseTopPages(input.top_pages);
  const contentExtract = parseContentExtract(input.content_extract);
  const siteUrl = cleanString(input.site_url, 2000);
  const siteType = detectSiteTypeHint({
    siteTypeHint: cleanString(input.site_type_hint, 60),
    siteUrl,
    services,
    products,
    categories,
    tags,
  });
  const localMode = detectLocalMode({
    businessAddress: cleanString(input.business_address, 400),
    primaryLocationHint: cleanString(input.primary_location_hint, 200),
    topPages,
    contentExtract,
    siteType,
  });

  const offerTerms = [...services, ...products, ...categories]
    .map((v) => cleanString(v, 160))
    .filter(Boolean)
    .slice(0, 300);

  const locations = [
    cleanString(input.primary_location_hint, 200),
    ...cleanString(input.business_address, 400)
      .split(/[,\n]/g)
      .map((v) => cleanString(v, 80)),
  ]
    .filter(Boolean)
    .slice(0, 8);
  const industry = inferIndustry(offerTerms, siteType);
  const audience = inferAudience(offerTerms, contentExtract);
  const pricingTier = inferPricingTier(offerTerms, contentExtract);
  const primaryGoals = inferPrimaryGoals(siteType, offerTerms);
  const constraints = inferConstraints(contentExtract);
  const contentMix = inferContentMix(topPages);
  const conversionGoal = primaryGoals.includes("sales")
    ? "sales"
    : primaryGoals.includes("leads")
      ? "leads"
      : "traffic";
  const briefConstraints = [...constraints];
  if (!briefConstraints.includes("US-only")) {
    briefConstraints.push("US-only");
  }
  const briefConfidence = inferBriefConfidence({
    offerTerms,
    locations,
    topPagesCount: topPages.length,
    contentExtractCount: contentExtract.length,
  });

  return {
    site_type_detected: siteType,
    local_mode: localMode,
    offer_terms: offerTerms,
    top_pages_count: topPages.length,
    content_extract_count: contentExtract.length,
    site_brief: {
      industry,
      offerings: offerTerms.slice(0, 60),
      audiences: [audience],
      locations,
      site_type: inferSiteBriefType(siteType),
      conversion_goal: conversionGoal,
      constraints: briefConstraints,
      confidence: briefConfidence,
      content_type_mix: contentMix,
      pricing_tier: pricingTier,
      primary_goals: primaryGoals,
    },
  };
}

async function upsertStep1Site(env: Env, payload: Record<string, unknown>): Promise<Step1SiteRecord> {
  const siteUrl = cleanString(payload.site_url, 2000);
  if (!siteUrl) {
    throw new Error("site_url required.");
  }
  const wpSiteId = cleanString(payload.wp_site_id, 120) || null;
  const siteName = cleanString(payload.site_name, 200) || null;
  const businessAddress = cleanString(payload.business_address, 400) || null;
  const primaryLocationHint = cleanString(payload.primary_location_hint, 200) || null;
  const siteTypeHint = cleanString(payload.site_type_hint, 60) || null;
  const profile = buildStep1SiteProfile(payload);
  const localMode = parseBoolUnknown(profile.local_mode, false) ? 1 : 0;
  const ts = nowMs();
  const siteIdentity = wpSiteId ? `${siteUrl.toLowerCase()}|${wpSiteId.toLowerCase()}` : siteUrl.toLowerCase();
  const siteId = `site_${await sha256Hex(siteIdentity)}`.slice(0, 48);
  const inputJson = safeJsonStringify(payload, 320000);
  const profileJson = safeJsonStringify(profile, 32000);

  await env.DB.prepare(
    `INSERT INTO wp_ai_seo_sites (
      site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
      local_mode, input_json, site_profile_json, last_analysis_at, last_research_at, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
    ON CONFLICT(site_id) DO UPDATE SET
      site_url = excluded.site_url,
      site_name = excluded.site_name,
      business_address = excluded.business_address,
      primary_location_hint = excluded.primary_location_hint,
      site_type_hint = excluded.site_type_hint,
      local_mode = excluded.local_mode,
      input_json = excluded.input_json,
      site_profile_json = excluded.site_profile_json,
      last_analysis_at = excluded.last_analysis_at,
      updated_at = excluded.updated_at`
  )
    .bind(
      siteId,
      siteUrl,
      siteName,
      businessAddress,
      primaryLocationHint,
      siteTypeHint,
      localMode,
      inputJson,
      profileJson,
      ts,
      ts,
      ts
    )
    .run();

  return {
    site_id: siteId,
    site_url: siteUrl,
    site_name: siteName,
    business_address: businessAddress,
    primary_location_hint: primaryLocationHint,
    site_type_hint: siteTypeHint,
    local_mode: localMode === 1,
    input_json: inputJson,
    site_profile_json: profileJson,
    last_analysis_at: ts,
    last_research_at: null,
    created_at: ts,
    updated_at: ts,
  };
}

async function loadStep1Site(env: Env, siteId: string): Promise<Step1SiteRecord | null> {
  const row = await env.DB.prepare(
    `SELECT
      site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
      local_mode, input_json, site_profile_json, last_analysis_at, last_research_at, created_at, updated_at
     FROM wp_ai_seo_sites
     WHERE site_id = ?
     LIMIT 1`
  )
    .bind(siteId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    site_id: cleanString(row.site_id, 64),
    site_url: cleanString(row.site_url, 2000),
    site_name: cleanString(row.site_name, 200) || null,
    business_address: cleanString(row.business_address, 400) || null,
    primary_location_hint: cleanString(row.primary_location_hint, 200) || null,
    site_type_hint: cleanString(row.site_type_hint, 60) || null,
    local_mode: clampInt(row.local_mode, 0, 1, 0) === 1,
    input_json: cleanString(row.input_json, 320000),
    site_profile_json: cleanString(row.site_profile_json, 32000),
    last_analysis_at: clampInt(row.last_analysis_at, 0, Number.MAX_SAFE_INTEGER, 0),
    last_research_at:
      row.last_research_at == null ? null : clampInt(row.last_research_at, 0, Number.MAX_SAFE_INTEGER, 0),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    updated_at: clampInt(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

function parseSiteIdFromPath(pathname: string, suffix: string): string | null {
  if (!pathname.startsWith("/v1/sites/")) return null;
  if (!pathname.endsWith(suffix)) return null;
  const middle = pathname.slice("/v1/sites/".length, pathname.length - suffix.length);
  const siteId = cleanString(decodeURIComponent(middle), 120);
  if (!siteId || siteId.includes("/")) return null;
  return siteId;
}

function parseSiteIdFromPrefixedPath(pathname: string, prefix: string, suffix: string): string | null {
  if (!pathname.startsWith(prefix)) return null;
  if (!pathname.endsWith(suffix)) return null;
  const middle = pathname.slice(prefix.length, pathname.length - suffix.length);
  const siteId = cleanString(decodeURIComponent(middle), 120);
  if (!siteId || siteId.includes("/")) return null;
  return siteId;
}

function extractOfferTerms(input: Record<string, unknown>): string[] {
  const direct = [
    ...parseStringArray(input.services, 300, 160),
    ...parseStringArray(input.products, 500, 160),
    ...parseStringArray(input.categories, 500, 160),
    ...parseStringArray(input.tags, 500, 80),
  ];
  if (direct.length > 0) return direct.slice(0, 800);

  const topPages = parseTopPages(input.top_pages);
  const phrases: string[] = [];
  for (const page of topPages) {
    for (const source of [page.title, page.h1]) {
      const cleaned = cleanString(source, 200);
      if (!cleaned) continue;
      const parts = cleaned.split(/[|:-]/g).map((v) => cleanString(v, 120)).filter(Boolean);
      for (const part of parts) {
        if (part.length >= 4) {
          phrases.push(part);
        }
      }
    }
  }
  return phrases.slice(0, 600);
}

function extractLocationTerms(input: Record<string, unknown>): string[] {
  const out = new Set<string>();
  const raw = [
    cleanString(input.primary_location_hint, 200),
    cleanString(input.business_address, 400),
  ]
    .filter(Boolean)
    .join(" ");
  const parts = raw.split(/[,\n]/g).map((v) => cleanString(v, 80)).filter(Boolean);
  for (const part of parts) {
    if (part.length >= 2) out.add(part.toLowerCase());
  }
  return [...out].slice(0, 20);
}

function classifyIntent(keyword: string): "informational" | "commercial" | "transactional" | "navigational" {
  const text = toKeywordNorm(keyword);
  if (/\b(login|signin|facebook|youtube|wikipedia)\b/.test(text)) return "navigational";
  if (/\b(buy|book|hire|quote|near me|order|pricing|price|cost)\b/.test(text)) return "transactional";
  if (/\b(best|top|service|company|agency|software for|platform)\b/.test(text)) return "commercial";
  if (/\b(how|what|why|guide|tips|checklist|vs|comparison|faq|requirements|time)\b/.test(text)) {
    return "informational";
  }
  return "commercial";
}

function classifyLocalIntent(keyword: string, locationTerms: string[], localMode: boolean): "yes" | "weak" | "no" {
  const text = toKeywordNorm(keyword);
  if (/\bnear me\b/.test(text)) return "yes";
  for (const loc of locationTerms) {
    if (loc && text.includes(loc)) return "yes";
  }
  if (!localMode) return "no";
  if (/\b(city|county|state|metro|neighborhood)\b/.test(text)) return "weak";
  return "weak";
}

function classifyPageType(
  intent: "informational" | "commercial" | "transactional" | "navigational",
  siteType: string,
  localIntent: "yes" | "weak" | "no",
  keyword: string
): Step1KeywordCandidate["page_type"] {
  const text = toKeywordNorm(keyword);
  if (intent === "informational") {
    if (/\b(vs|comparison)\b/.test(text)) return "comparison";
    if (/\b(checklist|steps)\b/.test(text)) return "checklist";
    if (/\b(faq|questions)\b/.test(text)) return "faq";
    return "guide";
  }
  if (siteType === "woo") return "product";
  if (siteType === "local" || localIntent === "yes") return "location landing";
  if (/\b(category|types|best)\b/.test(text)) return "category";
  return "service";
}

function clusterForKeyword(keyword: string, offerTerms: string[]): string {
  const text = toKeywordNorm(keyword);
  for (const term of offerTerms) {
    const norm = toKeywordNorm(term);
    if (!norm) continue;
    if (text.includes(norm)) return norm;
  }
  const tokens = tokenizeText(text).filter((v) => !["near", "best", "cost", "price", "in", "for"].includes(v));
  return tokens.slice(0, 2).join(" ") || "general";
}

function stripGarbageKeyword(keyword: string, siteType: string): boolean {
  const text = toKeywordNorm(keyword);
  if (!text) return true;
  if (/\b(torrent|pirate|free download|crack|nulled)\b/.test(text)) return true;
  if (siteType !== "publisher" && /\b(definition|meaning|dictionary)\b/.test(text)) return true;
  if (!/\bhiring|jobs|careers?\b/.test(siteType) && /\b(jobs?|careers?|salary)\b/.test(text)) return true;
  return false;
}

function buildVariantGroup(keyword: string, locationTerms: string[]): string {
  const stop = new Set([
    "the",
    "a",
    "an",
    "in",
    "for",
    "to",
    "near",
    "me",
    "best",
    "top",
    "cost",
    "price",
    "services",
    "service",
  ]);
  const locationWords = new Set(locationTerms.flatMap((v) => tokenizeText(v)));
  const tokens = tokenizeText(keyword).filter((v) => !stop.has(v) && !locationWords.has(v));
  return tokens.slice(0, 3).join(" ") || toKeywordNorm(keyword);
}

function generateCandidateKeywords(input: {
  offerTerms: string[];
  locationTerms: string[];
  siteType: string;
  localMode: boolean;
  semrushMetrics: Step1SemrushMetric[];
}): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const push = (kw: string) => {
    const cleaned = toKeywordNorm(kw);
    if (!cleaned || seen.has(cleaned)) return;
    seen.add(cleaned);
    out.push(cleaned);
  };

  const baseTerms = input.offerTerms.map((v) => toKeywordNorm(v)).filter(Boolean).slice(0, 120);
  const city = input.locationTerms[0] || "";
  const templateMods = [
    "{term}",
    "best {term}",
    "{term} cost",
    "{term} pricing",
    "{term} near me",
    "{term} company",
    "{term} services",
  ];
  const localMods = ["{term} {city}", "best {term} in {city}", "{term} {city} ca", "{term} in {city}"];
  const saasMods = ["{term} software", "{term} for small business", "{term} platform"];
  const ecomMods = ["buy {term} online", "{term} online", "{term} deals", "{term} for sale"];
  const infoMods = ["how to choose {term}", "{term} checklist", "{term} faq", "{term} comparison"];

  for (const term of baseTerms) {
    for (const t of templateMods) push(t.replace("{term}", term));
    for (const t of infoMods) push(t.replace("{term}", term));
    if (input.localMode && city) {
      for (const t of localMods) push(t.replace("{term}", term).replace("{city}", city));
    }
    if (input.siteType === "saas") {
      for (const t of saasMods) push(t.replace("{term}", term));
    }
    if (input.siteType === "woo") {
      for (const t of ecomMods) push(t.replace("{term}", term));
    }
  }

  for (const metric of input.semrushMetrics) {
    push(metric.keyword);
  }

  return out.slice(0, 2000);
}

function buildDeterministicFallbackKeywords(input: {
  siteUrl: string;
  siteName: string;
  industry: string;
  locationTerms: string[];
}): string[] {
  const base = new Set<string>();
  const host = normalizeRootDomain(input.siteUrl);
  const hostTokens = tokenizeText(host.replace(/\./g, " "));
  for (const token of hostTokens) {
    if (token.length >= 3) base.add(token);
  }
  for (const token of tokenizeText(input.siteName)) {
    if (token.length >= 3) base.add(token);
  }
  for (const token of tokenizeText(input.industry)) {
    if (token.length >= 3) base.add(token);
  }
  if (base.size < 1) {
    base.add("local service");
    base.add("professional service");
  }
  const city = input.locationTerms[0] ?? "";
  const out: string[] = [];
  const push = (v: string) => {
    const kw = toKeywordNorm(v);
    if (!kw || out.includes(kw)) return;
    out.push(kw);
  };
  for (const term of [...base].slice(0, 12)) {
    push(term);
    push(`${term} services`);
    push(`best ${term}`);
    push(`${term} cost`);
    push(`${term} near me`);
    if (city) {
      push(`${term} ${city}`);
      push(`best ${term} in ${city}`);
    }
    push(`how to choose ${term}`);
    push(`${term} faq`);
    push(`${term} comparison`);
  }
  return out.slice(0, 200);
}

function findSupportingTerms(keyword: string, offerTerms: string[], localMode: boolean): string[] {
  const defaults = localMode
    ? ["near me", "same day", "licensed", "insured", "cost", "quote", "reviews", "best", "local", "service"]
    : ["pricing", "features", "benefits", "reviews", "best", "guide", "comparison", "cost", "setup", "support"];
  const merged = [...tokenizeText(keyword), ...offerTerms.flatMap((v) => tokenizeText(v).slice(0, 2)), ...defaults];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const term of merged) {
    const value = cleanString(term, 40).toLowerCase();
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push(value);
    if (out.length >= 15) break;
  }
  return out.slice(0, 15);
}

function computeWinnabilityScore(metric: Step1SemrushMetric | undefined): number {
  if (!metric) return 0.45;
  const domains = metric.top_domains.map((v) => normalizeRootDomain(v));
  const giantDomains = new Set([
    "amazon.com",
    "wikipedia.org",
    "youtube.com",
    "reddit.com",
    "facebook.com",
    "linkedin.com",
    "yelp.com",
  ]);
  let giantCount = 0;
  for (const domain of domains.slice(0, 10)) {
    if (giantDomains.has(domain)) giantCount += 1;
  }
  const giantPenalty = clamp01(giantCount / 10);

  const serpFeatures = metric.serp_features.map((v) => v.toLowerCase());
  const featurePenalty = clamp01(
    (serpFeatures.some((v) => v.includes("ads")) ? 0.15 : 0) +
      (serpFeatures.some((v) => v.includes("shopping")) ? 0.15 : 0) +
      (serpFeatures.some((v) => v.includes("answer")) ? 0.08 : 0)
  );

  const kdPenalty = clamp01(metric.kd / 100);
  return clamp01(1 - 0.45 * giantPenalty - 0.3 * featurePenalty - 0.25 * kdPenalty);
}

function scoreKeywordCandidates(input: {
  keywords: string[];
  semrushMetrics: Step1SemrushMetric[];
  offerTerms: string[];
  locationTerms: string[];
  siteType: string;
  localMode: boolean;
}): Step1KeywordCandidate[] {
  const metricByKeyword = new Map(input.semrushMetrics.map((m) => [toKeywordNorm(m.keyword), m]));
  const maxVolume = Math.max(
    1,
    ...input.semrushMetrics.map((m) => Math.max(0, Number(m.volume_us || 0)))
  );
  const maxCpc = Math.max(0.1, ...input.semrushMetrics.map((m) => Math.max(0, Number(m.cpc || 0))));
  const offerVocab = new Set(input.offerTerms.flatMap((term) => tokenizeText(term)));

  const rows: Step1KeywordCandidate[] = [];
  for (const keyword of input.keywords) {
    if (stripGarbageKeyword(keyword, input.siteType)) continue;
    const keywordNorm = toKeywordNorm(keyword);
    const metric = metricByKeyword.get(keywordNorm);
    const intent = classifyIntent(keywordNorm);
    const localIntent = classifyLocalIntent(keywordNorm, input.locationTerms, input.localMode);
    const pageType = classifyPageType(intent, input.siteType, localIntent, keywordNorm);
    const cluster = clusterForKeyword(keywordNorm, input.offerTerms);
    const recommendedSlugBase = slugify(keywordNorm, 80);
    const recommendedPageType = pageType === "location landing" ? "location landing" : pageType;
    const recommendedSlug =
      pageType === "location landing" ? `locations/${recommendedSlugBase}` : recommendedSlugBase || slugify(cluster, 80);

    const volume = Math.max(0, Number(metric?.volume_us ?? 0) || 0);
    const kd = Math.max(0, Math.min(100, Number(metric?.kd ?? 55) || 55));
    const cpc = Math.max(0, Number(metric?.cpc ?? 0) || 0);
    const competitiveDensity = clamp01(Number(metric?.competitive_density ?? 0) || 0);
    const serpFeatures = metric?.serp_features ?? [];

    const keywordTokens = tokenizeText(keywordNorm);
    let overlap = 0;
    for (const token of keywordTokens) {
      if (offerVocab.has(token)) overlap += 1;
    }
    const relevanceScore = clamp01(keywordTokens.length > 0 ? overlap / keywordTokens.length : 0.2);
    const intentScore =
      intent === "transactional" ? 1 : intent === "commercial" ? 0.8 : intent === "informational" ? 0.45 : 0.25;
    const volumeScore = clamp01(Math.log10(volume + 1) / Math.log10(maxVolume + 1));
    const cpcProxyScore = clamp01(cpc / maxCpc);
    const winnabilityScore = computeWinnabilityScore(metric);
    const localBoost =
      input.localMode === false
        ? 0.25
        : localIntent === "yes"
          ? 1
          : localIntent === "weak"
            ? 0.6
            : 0.15;
    const difficultyPenalty = clamp01(kd / 100);
    const featurePenaltyParts = serpFeatures.map((feature) => feature.toLowerCase());
    const serpFeaturePenalty = clamp01(
      (featurePenaltyParts.some((f) => f.includes("ads")) ? 0.12 : 0) +
        (featurePenaltyParts.some((f) => f.includes("shopping")) ? 0.12 : 0) +
        (featurePenaltyParts.some((f) => f.includes("local")) ? 0.1 : 0)
    );

    const score =
      100 *
      (0.35 * relevanceScore +
        0.2 * intentScore +
        0.2 * volumeScore +
        0.1 * cpcProxyScore +
        0.1 * localBoost +
        0.05 * winnabilityScore) *
      (1 - 0.6 * difficultyPenalty) *
      (1 - serpFeaturePenalty);

    rows.push({
      keyword: keywordNorm,
      keyword_norm: keywordNorm,
      intent,
      local_intent: localIntent,
      page_type: pageType,
      cluster,
      recommended_slug: recommendedSlug,
      recommended_page_type: recommendedPageType,
      volume_us: Math.round(volume),
      kd: Number(kd.toFixed(2)),
      cpc: Number(cpc.toFixed(2)),
      competitive_density: Number(competitiveDensity.toFixed(3)),
      serp_features: serpFeatures,
      relevance_score: Number(relevanceScore.toFixed(4)),
      intent_score: Number(intentScore.toFixed(4)),
      volume_score: Number(volumeScore.toFixed(4)),
      cpc_proxy_score: Number(cpcProxyScore.toFixed(4)),
      winnability_score: Number(winnabilityScore.toFixed(4)),
      local_boost: Number(localBoost.toFixed(4)),
      difficulty_penalty: Number(difficultyPenalty.toFixed(4)),
      serp_feature_penalty: Number(serpFeaturePenalty.toFixed(4)),
      opportunity_score: Number(score.toFixed(2)),
      supporting_terms: findSupportingTerms(keywordNorm, input.offerTerms, input.localMode),
    });
  }

  rows.sort((a, b) => b.opportunity_score - a.opportunity_score);
  return rows;
}

function pickPrimaryKeywords(scored: Step1KeywordCandidate[], locationTerms: string[]): Step1KeywordCandidate[] {
  const primaryPool = scored.filter((row) => row.intent === "transactional" || row.intent === "commercial");
  const selected: Step1KeywordCandidate[] = [];
  const variantCount = new Map<string, number>();
  const clusterSet = new Set<string>();
  const slugSet = new Set<string>();

  // Pass 1: seed cluster diversity.
  const byCluster = new Map<string, Step1KeywordCandidate[]>();
  for (const row of primaryPool) {
    const bucket = byCluster.get(row.cluster) ?? [];
    bucket.push(row);
    byCluster.set(row.cluster, bucket);
  }
  const clusterCandidates = [...byCluster.entries()]
    .map(([cluster, rows]) => ({ cluster, best: rows[0] }))
    .sort((a, b) => b.best.opportunity_score - a.best.opportunity_score)
    .slice(0, 3);
  for (const item of clusterCandidates) {
    const group = buildVariantGroup(item.best.keyword, locationTerms);
    variantCount.set(group, 1);
    clusterSet.add(item.cluster);
    slugSet.add(item.best.recommended_slug);
    selected.push(item.best);
  }

  for (const row of primaryPool) {
    if (selected.length >= 10) break;
    if (selected.some((v) => v.keyword_norm === row.keyword_norm)) continue;
    const variantGroup = buildVariantGroup(row.keyword, locationTerms);
    const count = variantCount.get(variantGroup) ?? 0;
    if (count >= 2) continue;

    // Before reaching 5 unique slugs, prefer rows introducing new target pages.
    if (slugSet.size < 5 && slugSet.has(row.recommended_slug)) {
      continue;
    }
    selected.push(row);
    variantCount.set(variantGroup, count + 1);
    clusterSet.add(row.cluster);
    slugSet.add(row.recommended_slug);
  }

  if (selected.length < 10) {
    for (const row of primaryPool) {
      if (selected.length >= 10) break;
      if (selected.some((v) => v.keyword_norm === row.keyword_norm)) continue;
      const variantGroup = buildVariantGroup(row.keyword, locationTerms);
      const count = variantCount.get(variantGroup) ?? 0;
      if (count >= 2) continue;
      selected.push(row);
      variantCount.set(variantGroup, count + 1);
      clusterSet.add(row.cluster);
      slugSet.add(row.recommended_slug);
    }
  }

  const isMonetizable = (row: Step1KeywordCandidate): boolean => {
    if (!(row.intent === "commercial" || row.intent === "transactional")) return false;
    return (
      row.page_type === "service" ||
      row.page_type === "category" ||
      row.page_type === "product" ||
      row.page_type === "location landing"
    );
  };

  const monetizableTarget = Math.min(8, Math.max(6, selected.length - 2));
  let monetizableCount = selected.filter(isMonetizable).length;
  if (monetizableCount < monetizableTarget) {
    const replacements = primaryPool.filter(
      (row) => isMonetizable(row) && !selected.some((v) => v.keyword_norm === row.keyword_norm)
    );
    for (const candidate of replacements) {
      if (monetizableCount >= monetizableTarget) break;
      const idx = selected.findIndex((row) => !isMonetizable(row));
      if (idx < 0) break;
      selected[idx] = candidate;
      monetizableCount += 1;
    }
  }

  return selected.slice(0, 10).sort((a, b) => b.opportunity_score - a.opportunity_score);
}

function chooseSecondaryContentType(keyword: string): "FAQ" | "guide" | "comparison" | "checklist" {
  const text = toKeywordNorm(keyword);
  if (/\b(vs|comparison|alternative)\b/.test(text)) return "comparison";
  if (/\b(checklist|steps|template)\b/.test(text)) return "checklist";
  if (/\b(faq|questions|what|why)\b/.test(text)) return "FAQ";
  return "guide";
}

function buildOutlineBullets(keyword: string, type: "FAQ" | "guide" | "comparison" | "checklist"): string[] {
  if (type === "FAQ") {
    return [
      `What ${keyword} means`,
      `Who needs ${keyword}`,
      `Typical pricing and timeline`,
      "Common mistakes to avoid",
      "How to choose a provider",
      "Next steps",
    ];
  }
  if (type === "comparison") {
    return [
      `${keyword}: key options compared`,
      "Feature-by-feature breakdown",
      "Pricing and value trade-offs",
      "Best use cases",
      "Decision framework",
      "Recommended next step",
    ];
  }
  if (type === "checklist") {
    return [
      `${keyword} pre-work checklist`,
      "Required inputs and documents",
      "Quality and compliance checks",
      "Cost/time planning checklist",
      "Launch checklist",
      "Post-launch checks",
    ];
  }
  return [
    `Introduction to ${keyword}`,
    "Core concepts and definitions",
    "Process and best practices",
    "Cost, effort, and timelines",
    "Common pitfalls and fixes",
    "When to hire an expert",
  ];
}

function pickSecondaryKeywords(
  scored: Step1KeywordCandidate[],
  primary: Step1KeywordCandidate[],
  locationTerms: string[]
): Step1SecondarySelection[] {
  const primarySet = new Set(primary.map((row) => row.keyword_norm));
  const byClusterPrimary = new Map<string, Step1KeywordCandidate[]>();
  for (const row of primary) {
    const list = byClusterPrimary.get(row.cluster) ?? [];
    list.push(row);
    byClusterPrimary.set(row.cluster, list);
  }

  const pool = scored.filter((row) => !primarySet.has(row.keyword_norm) && row.intent !== "navigational");
  const selected: Step1SecondarySelection[] = [];
  const variantCount = new Map<string, number>();

  for (const row of pool) {
    if (selected.length >= 10) break;
    if (!(row.intent === "informational" || row.intent === "commercial" || row.intent === "transactional")) continue;
    const variantGroup = buildVariantGroup(row.keyword, locationTerms);
    const count = variantCount.get(variantGroup) ?? 0;
    if (count >= 2) continue;

    const primaryClusterRows = byClusterPrimary.get(row.cluster) ?? primary;
    const support = primaryClusterRows[0];
    const contentType = chooseSecondaryContentType(row.keyword);
    const internalLinkTo = support?.recommended_slug ?? primary[0]?.recommended_slug ?? "";
    selected.push({
      keyword: row.keyword,
      intent: row.intent,
      cluster: row.cluster,
      supports_primary_keyword: support?.keyword ?? "",
      recommended_content_type: contentType,
      recommended_slug: `learn/${slugify(row.keyword, 80)}`,
      outline_bullets: buildOutlineBullets(row.keyword, contentType).slice(0, 10),
      internal_link_to: internalLinkTo,
      volume_us: row.volume_us,
      kd: row.kd,
      cpc: row.cpc,
      serp_features: row.serp_features,
      opportunity_score: row.opportunity_score,
    });
    variantCount.set(variantGroup, count + 1);
  }

  return selected.slice(0, 10);
}

function buildClusterMap(
  primary: Step1KeywordCandidate[],
  secondary: Array<{ keyword: string; cluster: string }>
): Array<{
  cluster: string;
  primary_keywords: string[];
  secondary_keywords: string[];
  suggested_pillar_page: string;
}> {
  const map = new Map<
    string,
    {
      cluster: string;
      primary_keywords: string[];
      secondary_keywords: string[];
      suggested_pillar_page: string;
      score: number;
    }
  >();
  for (const row of primary) {
    const key = row.cluster || "general";
    const slot = map.get(key) ?? {
      cluster: key,
      primary_keywords: [],
      secondary_keywords: [],
      suggested_pillar_page: row.recommended_slug,
      score: row.opportunity_score,
    };
    slot.primary_keywords.push(row.keyword);
    slot.score = Math.max(slot.score, row.opportunity_score);
    if (!slot.suggested_pillar_page) {
      slot.suggested_pillar_page = row.recommended_slug;
    }
    map.set(key, slot);
  }
  for (const row of secondary) {
    const key = row.cluster || "general";
    const slot = map.get(key);
    if (!slot) continue;
    slot.secondary_keywords.push(row.keyword);
  }
  return [...map.values()]
    .sort((a, b) => b.score - a.score)
    .slice(0, 8)
    .map((row) => ({
      cluster: row.cluster,
      primary_keywords: row.primary_keywords.slice(0, 6),
      secondary_keywords: row.secondary_keywords.slice(0, 12),
      suggested_pillar_page: row.suggested_pillar_page,
    }));
}

function fillPrimaryToTop10(
  current: Step1KeywordCandidate[],
  scored: Step1KeywordCandidate[],
  locationTerms: string[]
): Step1KeywordCandidate[] {
  const out = [...current];
  const seen = new Set(out.map((row) => row.keyword_norm));
  const variantCount = new Map<string, number>();
  for (const row of out) {
    const key = buildVariantGroup(row.keyword, locationTerms);
    variantCount.set(key, (variantCount.get(key) ?? 0) + 1);
  }
  for (const row of scored) {
    if (out.length >= 10) break;
    if (seen.has(row.keyword_norm)) continue;
    if (!(row.intent === "commercial" || row.intent === "transactional")) continue;
    const key = buildVariantGroup(row.keyword, locationTerms);
    const count = variantCount.get(key) ?? 0;
    if (count >= 2) continue;
    out.push(row);
    seen.add(row.keyword_norm);
    variantCount.set(key, count + 1);
  }
  for (const row of scored) {
    if (out.length >= 10) break;
    if (seen.has(row.keyword_norm)) continue;
    out.push(row);
    seen.add(row.keyword_norm);
  }
  while (out.length < 10) {
    const base = out[0] ?? scored[0];
    if (!base) break;
    const suffix = out.length + 1;
    const fallbackKeyword = toKeywordNorm(`${base.cluster} service ${suffix}`);
    if (seen.has(fallbackKeyword)) break;
    out.push({
      ...base,
      keyword: fallbackKeyword,
      keyword_norm: fallbackKeyword,
      recommended_slug: slugify(fallbackKeyword, 80),
      volume_us: 0,
      kd: base.kd,
      cpc: 0,
      opportunity_score: Math.max(1, base.opportunity_score - suffix),
      supporting_terms: findSupportingTerms(fallbackKeyword, [base.cluster], false),
    });
    seen.add(fallbackKeyword);
  }
  return out.slice(0, 10);
}

type Step1SecondarySelection = {
  keyword: string;
  intent: string;
  cluster: string;
  supports_primary_keyword: string;
  recommended_content_type: "FAQ" | "guide" | "comparison" | "checklist";
  recommended_slug: string;
  outline_bullets: string[];
  internal_link_to: string;
  volume_us: number;
  kd: number;
  cpc: number;
  serp_features: string[];
  opportunity_score: number;
};

function fillSecondaryToTop10(
  current: Step1SecondarySelection[],
  scored: Step1KeywordCandidate[],
  primary: Step1KeywordCandidate[],
  locationTerms: string[]
): Step1SecondarySelection[] {
  const out = [...current];
  const seen = new Set(out.map((row) => toKeywordNorm(row.keyword)));
  const primarySeen = new Set(primary.map((row) => row.keyword_norm));
  const variantCount = new Map<string, number>();
  for (const row of out) {
    const key = buildVariantGroup(row.keyword, locationTerms);
    variantCount.set(key, (variantCount.get(key) ?? 0) + 1);
  }
  for (const row of scored) {
    if (out.length >= 10) break;
    if (primarySeen.has(row.keyword_norm)) continue;
    if (seen.has(row.keyword_norm)) continue;
    if (row.intent === "navigational") continue;
    const key = buildVariantGroup(row.keyword, locationTerms);
    const count = variantCount.get(key) ?? 0;
    if (count >= 2) continue;
    const contentType = chooseSecondaryContentType(row.keyword);
    out.push({
      keyword: row.keyword,
      intent: row.intent,
      cluster: row.cluster,
      supports_primary_keyword: primary[0]?.keyword ?? "",
      recommended_content_type: contentType,
      recommended_slug: `learn/${slugify(row.keyword, 80)}`,
      outline_bullets: buildOutlineBullets(row.keyword, contentType).slice(0, 10),
      internal_link_to: primary[0]?.recommended_slug ?? "",
      volume_us: row.volume_us,
      kd: row.kd,
      cpc: row.cpc,
      serp_features: row.serp_features,
      opportunity_score: row.opportunity_score,
    });
    seen.add(row.keyword_norm);
    variantCount.set(key, count + 1);
  }
  while (out.length < 10) {
    const base = out[0];
    if (!base) break;
    const suffix = out.length + 1;
    const fallbackKeyword = toKeywordNorm(`${base.cluster} guide ${suffix}`);
    if (seen.has(fallbackKeyword)) break;
    const contentType = chooseSecondaryContentType(fallbackKeyword);
    out.push({
      keyword: fallbackKeyword,
      intent: "informational",
      cluster: base.cluster,
      supports_primary_keyword: base.supports_primary_keyword,
      recommended_content_type: contentType,
      recommended_slug: `learn/${slugify(fallbackKeyword, 80)}`,
      outline_bullets: buildOutlineBullets(fallbackKeyword, contentType).slice(0, 10),
      internal_link_to: base.internal_link_to,
      volume_us: 0,
      kd: base.kd,
      cpc: 0,
      serp_features: [],
      opportunity_score: Math.max(1, base.opportunity_score - suffix),
    });
    seen.add(fallbackKeyword);
  }
  return out.slice(0, 10);
}

function resolveTargetUrlForSlug(topPages: Array<{ url: string }>, slug: string): string | null {
  const candidate = cleanString(slug, 200).replace(/^\/+/, "");
  if (!candidate) return null;
  for (const page of topPages) {
    const url = cleanString(page.url, 2000);
    if (!url) continue;
    if (url.toLowerCase().includes(candidate.toLowerCase())) {
      return url;
    }
  }
  return null;
}

function buildStep1Step2Contract(
  primary: Step1KeywordCandidate[],
  secondary: Step1SecondarySelection[],
  topPages: Array<{ url: string }>
): Array<Record<string, unknown>> {
  const rows: Array<Record<string, unknown>> = [];
  for (const row of primary) {
    rows.push({
      keyword: row.keyword,
      cluster: row.cluster,
      intent: row.intent,
      is_local_intent: row.local_intent === "yes" || row.local_intent === "weak",
      target_page_type: row.recommended_page_type,
      target_url: resolveTargetUrlForSlug(topPages, row.recommended_slug),
      target_slug: row.recommended_slug,
      priority: "primary",
    });
  }
  for (const row of secondary) {
    rows.push({
      keyword: row.keyword,
      cluster: row.cluster,
      intent: row.intent,
      is_local_intent: false,
      target_page_type: row.recommended_content_type,
      target_url: resolveTargetUrlForSlug(topPages, row.recommended_slug),
      target_slug: row.recommended_slug,
      priority: "secondary",
    });
  }
  return rows.slice(0, 20);
}

async function saveStep1KeywordResearchResults(
  env: Env,
  input: {
    siteId: string;
    researchRunId: string;
    primary: Step1KeywordCandidate[];
    secondary: Step1SecondarySelection[];
    clusters: Array<{
      cluster: string;
      primary_keywords: string[];
      secondary_keywords: string[];
      suggested_pillar_page: string;
    }>;
    step2Contract: Array<Record<string, unknown>>;
  }
): Promise<void> {
  const ts = nowMs();

  await env.DB.prepare("DELETE FROM wp_ai_seo_keywords WHERE site_id = ?").bind(input.siteId).run();

  const selectedTierByKeyword = new Map<string, "primary" | "secondary" | "none">();
  for (const row of input.primary) selectedTierByKeyword.set(row.keyword_norm, "primary");
  for (const row of input.secondary) {
    if (!selectedTierByKeyword.has(toKeywordNorm(row.keyword))) {
      selectedTierByKeyword.set(toKeywordNorm(row.keyword), "secondary");
    }
  }

  const allKeywordRows: Array<{
    keyword: string;
    keyword_norm: string;
    cluster: string;
    intent: string;
    local_intent: string;
    page_type: string;
    recommended_slug: string;
    opportunity_score: number;
    selected_tier: "primary" | "secondary" | "none";
    volume_us: number;
    kd: number;
    cpc: number;
    competitive_density: number;
    serp_features: string[];
    data_source: string;
  }> = [
    ...input.primary.map((row) => ({
      keyword: row.keyword,
      keyword_norm: row.keyword_norm,
      cluster: row.cluster,
      intent: row.intent,
      local_intent: row.local_intent,
      page_type: row.page_type,
      recommended_slug: row.recommended_slug,
      opportunity_score: row.opportunity_score,
      selected_tier: "primary" as const,
      volume_us: row.volume_us,
      kd: row.kd,
      cpc: row.cpc,
      competitive_density: row.competitive_density,
      serp_features: row.serp_features,
      data_source: "semrush_or_heuristic",
    })),
    ...input.secondary.map((row) => ({
      keyword: row.keyword,
      keyword_norm: toKeywordNorm(row.keyword),
      cluster: row.cluster,
      intent: row.intent,
      local_intent: "weak",
      page_type: row.recommended_content_type.toLowerCase(),
      recommended_slug: row.recommended_slug,
      opportunity_score: row.opportunity_score,
      selected_tier: "secondary" as const,
      volume_us: row.volume_us,
      kd: row.kd,
      cpc: row.cpc,
      competitive_density: 0,
      serp_features: row.serp_features,
      data_source: "semrush_or_heuristic",
    })),
  ];

  for (const row of allKeywordRows) {
    const keywordId = uuid("kw");
    await env.DB.prepare(
      `INSERT INTO wp_ai_seo_keywords (
        keyword_id, site_id, research_run_id, keyword, keyword_norm, cluster,
        intent, local_intent, page_type, recommended_slug, opportunity_score,
        selected_tier, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        keywordId,
        input.siteId,
        input.researchRunId,
        row.keyword,
        row.keyword_norm,
        row.cluster,
        row.intent,
        row.local_intent,
        row.page_type,
        row.recommended_slug,
        row.opportunity_score,
        row.selected_tier,
        ts,
        ts
      )
      .run();

    await env.DB.prepare(
      `INSERT INTO wp_ai_seo_keyword_metrics (
        metric_id, keyword_id, volume_us, kd, cpc, competitive_density, serp_features_json, data_source, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        uuid("met"),
        keywordId,
        row.volume_us,
        row.kd,
        row.cpc,
        row.competitive_density,
        safeJsonStringify(row.serp_features, 4000),
        row.data_source,
        ts
      )
      .run();
  }

  const primaryOutput = input.primary.map((row) => ({
    keyword: row.keyword,
    intent: row.intent,
    cluster: row.cluster,
    volume_us: row.volume_us,
    kd: row.kd,
    cpc: row.cpc,
    serp_features: row.serp_features,
    recommended_page_type: row.recommended_page_type,
    recommended_slug: row.recommended_slug,
    title_tag_suggestion: `${row.keyword} | ${row.cluster} Experts`,
    h1_suggestion: row.keyword.replace(/\b\w/g, (v) => v.toUpperCase()),
    supporting_terms: row.supporting_terms.slice(0, 15),
    winnability_score: row.winnability_score,
    opportunity_score: row.opportunity_score,
  }));

  const secondaryOutput = input.secondary.map((row) => ({
    keyword: row.keyword,
    intent: row.intent,
    cluster: row.cluster,
    supports_primary_keyword: row.supports_primary_keyword,
    recommended_content_type: row.recommended_content_type,
    recommended_slug: row.recommended_slug,
    outline_bullets: row.outline_bullets.slice(0, 10),
    internal_link_to: row.internal_link_to,
    volume_us: row.volume_us,
    kd: row.kd,
    cpc: row.cpc,
    serp_features: row.serp_features,
    opportunity_score: row.opportunity_score,
  }));

  const resultPayload = {
    site_id: input.siteId,
    research_run_id: input.researchRunId,
    generated_at: ts,
    primary_top_10: primaryOutput,
    secondary_top_10: secondaryOutput,
    cluster_map: input.clusters,
    step2_contract: input.step2Contract,
  };

  await env.DB.prepare(
    `INSERT INTO wp_ai_seo_selections (
      selection_id, site_id, research_run_id, selection_type, payload_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("sel"),
      input.siteId,
      input.researchRunId,
      "keyword_research_results",
      safeJsonStringify(resultPayload, 1_000_000),
      ts
    )
    .run();

  await env.DB.prepare("UPDATE wp_ai_seo_sites SET last_research_at = ?, updated_at = ? WHERE site_id = ?")
    .bind(ts, ts, input.siteId)
    .run();
}

async function loadLatestStep1KeywordResearchResults(env: Env, siteId: string): Promise<Record<string, unknown> | null> {
  const row = await env.DB.prepare(
    `SELECT payload_json
     FROM wp_ai_seo_selections
     WHERE site_id = ? AND selection_type = 'keyword_research_results'
     ORDER BY created_at DESC
     LIMIT 1`
  )
    .bind(siteId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  const parsed = safeJsonParseObject(cleanString(row.payload_json, 1_000_000));
  return parsed;
}

async function runStep1KeywordResearch(
  env: Env,
  site: Step1SiteRecord,
  requestBody: Record<string, unknown> | null
): Promise<{
  researchRunId: string;
  result: Record<string, unknown>;
}> {
  const siteInput = safeJsonParseObject(site.input_json) ?? {};
  const payload = requestBody ?? {};
  const semrushMetrics = parseSemrushMetrics(payload.semrush_keywords ?? siteInput.semrush_keywords);
  const siteProfile = safeJsonParseObject(site.site_profile_json) ?? {};
  const offerTerms = extractOfferTerms(siteInput);
  const locationTerms = extractLocationTerms(siteInput);
  const siteType = cleanString(siteProfile.site_type_detected, 60) || "general";
  const localMode = site.local_mode || parseBoolUnknown(siteProfile.local_mode, false);

  const candidateKeywords = generateCandidateKeywords({
    offerTerms,
    locationTerms,
    siteType,
    localMode,
    semrushMetrics,
  });
  if (candidateKeywords.length < 40) {
    const fallback = buildDeterministicFallbackKeywords({
      siteUrl: cleanString(siteInput.site_url, 2000),
      siteName: cleanString(siteInput.site_name, 200),
      industry: cleanString(parseJsonObject(siteProfile.site_brief)?.industry, 120),
      locationTerms,
    });
    for (const kw of fallback) {
      if (!candidateKeywords.includes(kw)) {
        candidateKeywords.push(kw);
      }
      if (candidateKeywords.length >= 2000) break;
    }
  }
  const scored = scoreKeywordCandidates({
    keywords: candidateKeywords,
    semrushMetrics,
    offerTerms,
    locationTerms,
    siteType,
    localMode,
  });
  const primaryBase = pickPrimaryKeywords(scored, locationTerms);
  const primary = fillPrimaryToTop10(primaryBase, scored, locationTerms);
  const secondaryBase = pickSecondaryKeywords(scored, primary, locationTerms);
  const secondary = fillSecondaryToTop10(secondaryBase, scored, primary, locationTerms);
  const clusters = buildClusterMap(
    primary,
    secondary.map((row) => ({ keyword: row.keyword, cluster: row.cluster }))
  );
  const topPages = parseTopPages(siteInput.top_pages);
  const step2Contract = buildStep1Step2Contract(primary, secondary, topPages);

  const researchRunId = uuid("krr");
  await saveStep1KeywordResearchResults(env, {
    siteId: site.site_id,
    researchRunId,
    primary,
    secondary,
    clusters,
    step2Contract,
  });

  const result = (await loadLatestStep1KeywordResearchResults(env, site.site_id)) ?? {};
  return {
    researchRunId,
    result,
  };
}

type Step2KeywordTarget = {
  keyword: string;
  cluster: string;
  intent: string;
  is_local_intent: boolean;
  target_page_type: string;
  target_url: string | null;
  target_slug: string | null;
  priority: "primary" | "secondary";
};

function getSitePlanFromInput(siteInput: Record<string, unknown>): { metro_proxy: boolean; metro: string | null } {
  const plan = parseJsonObject(siteInput.plan) ?? {};
  return {
    metro_proxy: parseBoolUnknown(plan.metro_proxy, false),
    metro: cleanString(plan.metro, 120) || null,
  };
}

function parseStep2KeywordTargets(result: Record<string, unknown>): Step2KeywordTarget[] {
  const raw = Array.isArray(result.step2_contract) ? result.step2_contract : [];
  const out: Step2KeywordTarget[] = [];
  for (const item of raw) {
    const row = parseJsonObject(item);
    if (!row) continue;
    const keyword = cleanString(row.keyword, 300);
    if (!keyword) continue;
    const priority = cleanString(row.priority, 20).toLowerCase() === "secondary" ? "secondary" : "primary";
    out.push({
      keyword,
      cluster: cleanString(row.cluster, 120) || "general",
      intent: cleanString(row.intent, 40) || "commercial",
      is_local_intent: parseBoolUnknown(row.is_local_intent, false),
      target_page_type: cleanString(row.target_page_type, 80) || "service",
      target_url: cleanString(row.target_url, 2000) || null,
      target_slug: cleanString(row.target_slug, 120) || null,
      priority,
    });
    if (out.length >= SITE_KEYWORD_CAP) break;
  }
  return out;
}

function classifySerpResultPageType(url: string, title: string | null, h1: string | null): string {
  const text = `${cleanString(url, 2000)} ${cleanString(title, 300)} ${cleanString(h1, 300)}`.toLowerCase();
  if (/\b(product|shop|sku|buy|cart)\b/.test(text)) return "product";
  if (/\b(category|collections?)\b/.test(text)) return "category";
  if (/\b(forum|reddit|quora)\b/.test(text)) return "forum";
  if (/\b(yelp|angi|tripadvisor|directory)\b/.test(text)) return "directory";
  if (/\b(blog|guide|how-to|article|news)\b/.test(text)) return "blog";
  if (/\b(service|location|near-me|quote|book)\b/.test(text)) return "service";
  return "other";
}

function extractHtmlTagText(html: string, tag: string): string[] {
  const pattern = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "gi");
  const out: string[] = [];
  let match: RegExpExecArray | null = null;
  while ((match = pattern.exec(html)) != null) {
    const text = cleanString(match[1], 4000).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (text) out.push(text);
    if (out.length >= 200) break;
  }
  return out;
}

function extractMetaContent(html: string, name: string): string | null {
  const pattern = new RegExp(
    `<meta[^>]+(?:name|property)=["']${name}["'][^>]*content=["']([^"']*)["'][^>]*>`,
    "i"
  );
  const m = html.match(pattern);
  return m ? cleanString(m[1], 800) || null : null;
}

function extractLinkHref(html: string, relValue: string): string | null {
  const pattern = new RegExp(
    `<link[^>]+rel=["'][^"']*${relValue}[^"']*["'][^>]+href=["']([^"']+)["'][^>]*>`,
    "i"
  );
  const m = html.match(pattern);
  return m ? cleanString(m[1], 2000) || null : null;
}

async function fetchHtmlWithTimeout(
  url: string,
  timeoutMs = 8000,
  etag: string | null = null,
  lastModified: string | null = null
): Promise<{ status: number; html: string | null; etag: string | null; last_modified: string | null }> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const headers: Record<string, string> = {
      "user-agent": "SEO-Agent/1.0 (+https://example.invalid)",
    };
    if (etag) headers["if-none-match"] = etag;
    if (lastModified) headers["if-modified-since"] = lastModified;
    const res = await fetch(url, {
      signal: controller.signal,
      headers,
    });
    if (res.status === 304) {
      return { status: 304, html: null, etag: etag ?? null, last_modified: lastModified ?? null };
    }
    if (!res.ok) {
      return { status: res.status, html: null, etag: null, last_modified: null };
    }
    return {
      status: res.status,
      html: await res.text(),
      etag: cleanString(res.headers.get("etag"), 500) || null,
      last_modified: cleanString(res.headers.get("last-modified"), 200) || null,
    };
  } catch {
    return { status: 0, html: null, etag: null, last_modified: null };
  } finally {
    clearTimeout(timer);
  }
}

type Step2PageFetchResult = {
  status: number;
  html: string | null;
  etag: string | null;
  last_modified: string | null;
  extractor_mode: "direct_fetch" | "decodo_web_api";
  fallback_reason: string | null;
  raw_payload_sha256: string | null;
  provider_task_id: string | null;
};

async function fetchPageHtmlByProvider(
  env: Env,
  input: {
    pageProvider: PageProvider | null;
    fallbackPageProvider?: PageProvider | null;
    geoProvider: GeoProvider;
    geoLabel: string;
    url: string;
    timeoutMs: number;
    cache: { etag: string | null; last_modified: string | null; html: string | null } | null;
  }
): Promise<Step2PageFetchResult> {
  const primaryProvider = resolvePagePrimary(env, input.pageProvider);
  const fallbackProvider = input.fallbackPageProvider ?? resolvePageFallback(env);
  if (primaryProvider === "decodo_web_api") {
    try {
      const decodo = await fetchPageViaDecodo(env, {
        url: input.url,
        geoLabel: input.geoProvider === "decodo_geo" ? cleanString(input.geoLabel, 120) || null : null,
      });
      const html = cleanString(decodo.html, 3_000_000) || cleanString(decodo.markdown, 3_000_000) || null;
      if (html) {
        return {
          status: 200,
          html,
          etag: null,
          last_modified: null,
          extractor_mode: "decodo_web_api",
          fallback_reason: null,
          raw_payload_sha256: decodo.rawPayloadSha256,
          provider_task_id: decodo.taskId,
        };
      }
      if (fallbackProvider !== "direct_fetch") {
        throw new Error("page_provider_fallback_not_supported");
      }
      const direct = await fetchHtmlWithTimeout(
        input.url,
        input.timeoutMs,
        input.cache?.etag ?? null,
        input.cache?.last_modified ?? null
      );
      return {
        status: direct.status,
        html: direct.status === 304 ? input.cache?.html ?? null : direct.html,
        etag: direct.etag,
        last_modified: direct.last_modified,
        extractor_mode: "direct_fetch",
        fallback_reason: "decodo_empty_response",
        raw_payload_sha256: decodo.rawPayloadSha256,
        provider_task_id: decodo.taskId,
      };
    } catch (error) {
      if (fallbackProvider !== "direct_fetch") {
        throw error;
      }
      const direct = await fetchHtmlWithTimeout(
        input.url,
        input.timeoutMs,
        input.cache?.etag ?? null,
        input.cache?.last_modified ?? null
      );
      return {
        status: direct.status,
        html: direct.status === 304 ? input.cache?.html ?? null : direct.html,
        etag: direct.etag,
        last_modified: direct.last_modified,
        extractor_mode: "direct_fetch",
        fallback_reason: `decodo_failed:${cleanString((error as Error)?.message ?? error, 180)}`,
        raw_payload_sha256: null,
        provider_task_id: null,
      };
    }
  }

  const direct = await fetchHtmlWithTimeout(
    input.url,
    input.timeoutMs,
    input.cache?.etag ?? null,
    input.cache?.last_modified ?? null
  );
  return {
    status: direct.status,
    html: direct.status === 304 ? input.cache?.html ?? null : direct.html,
    etag: direct.etag,
    last_modified: direct.last_modified,
    extractor_mode: "direct_fetch",
    fallback_reason: null,
    raw_payload_sha256: null,
    provider_task_id: null,
  };
}

async function persistCanonicalPageSnapshot(
  env: Env,
  input: {
    url: string;
    html: string;
    parserVersion: string;
    rawSha256: string | null;
  }
): Promise<{ urlId: string | null; pageSnapshotId: string | null; contentHash: string }> {
  const urlId = await getOrCreateUrlId(env, input.url);
  const contentHash = await sha256Hex(input.html);
  if (!urlId) {
    return { urlId: null, pageSnapshotId: null, contentHash };
  }
  const pageExtract = buildStep2PageExtract(input.url, input.html, "");
  const pageSnapshotId = uuid("pagesnap");
  await env.DB.prepare(
    `INSERT INTO page_snapshots (
      id, url_id, fetched_at, http_status, content_hash, extracted_json, raw_r2_key, raw_sha256, parser_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(url_id, content_hash) DO UPDATE SET
      fetched_at = excluded.fetched_at,
      extracted_json = excluded.extracted_json,
      raw_sha256 = excluded.raw_sha256,
      parser_version = excluded.parser_version`
  )
    .bind(
      pageSnapshotId,
      urlId,
      Math.floor(nowMs() / 1000),
      200,
      contentHash,
      safeJsonStringify(pageExtract, 120000),
      null,
      input.rawSha256,
      input.parserVersion
    )
    .run();
  return { urlId, pageSnapshotId, contentHash };
}

type Step2PageExtract = {
  url: string;
  domain: string;
  title: string | null;
  meta_description: string | null;
  robots_meta: string | null;
  canonical_url: string | null;
  hreflang_count: number;
  h1_text: string | null;
  h2_json: string;
  h3_json: string;
  word_count: number;
  schema_types_json: string;
  internal_links_out_count: number;
  internal_anchors_json: string;
  external_links_out_count: number;
  external_anchors_json: string;
  image_count: number;
  alt_coverage_rate: number;
  keyword_placement_flags_json: string;
  faq_section_present: number;
  pricing_section_present: number;
  testimonials_present: number;
  location_refs_present: number;
  how_it_works_present: number;
  internal_edges: Array<{ to_url: string; anchor: string }>;
};

function buildStep2PageExtract(url: string, html: string, keyword: string): Step2PageExtract {
  const domain = normalizeDomain(url);
  const title = extractHtmlTagText(html, "title")[0] ?? null;
  const metaDescription = extractMetaContent(html, "description");
  const robotsMeta = extractMetaContent(html, "robots");
  const canonicalUrl = extractLinkHref(html, "canonical");
  const hreflangMatches = html.match(/<link[^>]+hreflang=/gi) ?? [];
  const h1List = extractHtmlTagText(html, "h1");
  const h2List = extractHtmlTagText(html, "h2");
  const h3List = extractHtmlTagText(html, "h3");
  const bodyText = cleanString(html, 2_000_000).replace(/<script[\s\S]*?<\/script>/gi, " ").replace(/<style[\s\S]*?<\/style>/gi, " ").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  const words = bodyText ? bodyText.split(/\s+/).filter(Boolean) : [];

  const schemaTypes = new Set<string>();
  const ldJsonPattern = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let ldMatch: RegExpExecArray | null = null;
  while ((ldMatch = ldJsonPattern.exec(html)) != null) {
    const raw = cleanString(ldMatch[1], 100_000);
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw);
      const scan = (value: unknown) => {
        if (Array.isArray(value)) {
          for (const row of value) scan(row);
          return;
        }
        const obj = parseJsonObject(value);
        if (!obj) return;
        const typeValue = obj["@type"];
        if (typeof typeValue === "string") schemaTypes.add(cleanString(typeValue, 80));
        if (Array.isArray(typeValue)) {
          for (const row of typeValue) {
            const t = cleanString(row, 80);
            if (t) schemaTypes.add(t);
          }
        }
      };
      scan(parsed);
    } catch {
      continue;
    }
  }

  const linkPattern = /<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let linkMatch: RegExpExecArray | null = null;
  let internalOut = 0;
  let externalOut = 0;
  const internalAnchors: string[] = [];
  const externalAnchors: string[] = [];
  const internalEdges: Array<{ to_url: string; anchor: string }> = [];
  while ((linkMatch = linkPattern.exec(html)) != null) {
    const hrefRaw = cleanString(linkMatch[1], 2000);
    if (!hrefRaw || hrefRaw.startsWith("#") || hrefRaw.startsWith("mailto:") || hrefRaw.startsWith("tel:")) continue;
    const anchor = cleanString(linkMatch[2], 300).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    try {
      const absolute = new URL(hrefRaw, url);
      if (absolute.hostname === domain || absolute.hostname.endsWith(`.${domain}`)) {
        internalOut += 1;
        if (anchor) {
          internalAnchors.push(anchor);
          internalEdges.push({ to_url: absolute.toString(), anchor });
        }
      } else {
        externalOut += 1;
        if (anchor) externalAnchors.push(anchor);
      }
    } catch {
      continue;
    }
  }

  const imageTags = html.match(/<img\b[^>]*>/gi) ?? [];
  let imageCount = 0;
  let altCount = 0;
  for (const img of imageTags) {
    imageCount += 1;
    if (/\balt=["'][^"']*["']/i.test(img)) altCount += 1;
  }
  const altCoverage = imageCount > 0 ? altCount / imageCount : 1;
  const keywordNorm = toKeywordNorm(keyword);
  const first100 = words.slice(0, 100).join(" ").toLowerCase();
  const placements = {
    in_title: toKeywordNorm(title ?? "").includes(keywordNorm),
    in_h1: toKeywordNorm(h1List[0] ?? "").includes(keywordNorm),
    in_first_100_words: first100.includes(keywordNorm),
    in_url: toKeywordNorm(url).includes(keywordNorm),
    in_alt_text: false,
  };
  if (keywordNorm) {
    const imgAltPattern = /<img\b[^>]*\balt=["']([^"']+)["'][^>]*>/gi;
    let altMatch: RegExpExecArray | null = null;
    while ((altMatch = imgAltPattern.exec(html)) != null) {
      const alt = toKeywordNorm(altMatch[1]);
      if (alt.includes(keywordNorm)) {
        placements.in_alt_text = true;
        break;
      }
    }
  }
  const pageLower = bodyText.toLowerCase();

  return {
    url,
    domain,
    title,
    meta_description: metaDescription,
    robots_meta: robotsMeta,
    canonical_url: canonicalUrl,
    hreflang_count: hreflangMatches.length,
    h1_text: h1List[0] ?? null,
    h2_json: safeJsonStringify(h2List.slice(0, 40), 12000),
    h3_json: safeJsonStringify(h3List.slice(0, 40), 12000),
    word_count: words.length,
    schema_types_json: safeJsonStringify([...schemaTypes], 4000),
    internal_links_out_count: internalOut,
    internal_anchors_json: safeJsonStringify(internalAnchors.slice(0, 80), 12000),
    external_links_out_count: externalOut,
    external_anchors_json: safeJsonStringify(externalAnchors.slice(0, 80), 12000),
    image_count: imageCount,
    alt_coverage_rate: Number(altCoverage.toFixed(4)),
    keyword_placement_flags_json: safeJsonStringify(placements, 2000),
    faq_section_present: /\bfaq|frequently asked/i.test(pageLower) ? 1 : 0,
    pricing_section_present: /\bpricing|cost|price|quote\b/i.test(pageLower) ? 1 : 0,
    testimonials_present: /\btestimonial|reviews?|case study\b/i.test(pageLower) ? 1 : 0,
    location_refs_present: /\bnear me|in [a-z]{3,}|serving\b/i.test(pageLower) ? 1 : 0,
    how_it_works_present: /\bhow it works|steps|process\b/i.test(pageLower) ? 1 : 0,
    internal_edges: internalEdges.slice(0, 120),
  };
}

async function loadHtmlCache(
  env: Env,
  urlHash: string,
  now: number
): Promise<{ html_snapshot: string; etag: string | null; last_modified: string | null } | null> {
  const row = await env.DB.prepare(
    `SELECT html_snapshot, etag, last_modified
     FROM step2_html_cache
     WHERE url_hash = ? AND expires_at > ?
     LIMIT 1`
  )
    .bind(urlHash, now)
    .first<Record<string, unknown>>();
  if (!row) return null;
  const html = cleanString(row.html_snapshot, 1_500_000);
  if (!html) return null;
  return {
    html_snapshot: html,
    etag: cleanString(row.etag, 500) || null,
    last_modified: cleanString(row.last_modified, 200) || null,
  };
}

async function upsertHtmlCache(
  env: Env,
  input: {
    urlHash: string;
    url: string;
    etag: string | null;
    lastModified: string | null;
    html: string;
    now: number;
    ttlMs: number;
  }
): Promise<void> {
  const contentHash = await sha256Hex(input.html);
  const expiresAt = input.now + input.ttlMs;
  await env.DB.prepare(
    `INSERT INTO step2_html_cache (
      cache_id, url_hash, url, etag, last_modified, content_hash, html_snapshot, updated_at, expires_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(url_hash) DO UPDATE SET
      url = excluded.url,
      etag = excluded.etag,
      last_modified = excluded.last_modified,
      content_hash = excluded.content_hash,
      html_snapshot = excluded.html_snapshot,
      updated_at = excluded.updated_at,
      expires_at = excluded.expires_at`
  )
    .bind(
      uuid("s2hc"),
      input.urlHash,
      input.url,
      input.etag,
      input.lastModified,
      contentHash,
      input.html.slice(0, 1_500_000),
      input.now,
      expiresAt
    )
    .run();
}

async function canFetchByRobots(
  env: Env,
  url: string,
  robotsCache: Map<string, { allowedAll: boolean; disallowAll: boolean }>
): Promise<boolean> {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  const domain = parsed.hostname.toLowerCase();
  const cached = robotsCache.get(domain);
  if (cached) {
    return cached.allowedAll || !cached.disallowAll;
  }

  const cacheKey = `robots:${domain}`;
  const now = nowMs();
  const dbCached = await env.DB.prepare(
    `SELECT data_json
     FROM step2_backlink_cache
     WHERE cache_key = ? AND expires_at > ?
     LIMIT 1`
  )
    .bind(cacheKey, now)
    .first<Record<string, unknown>>();
  if (dbCached) {
    const data = safeJsonParseObject(cleanString(dbCached.data_json, 4000)) ?? {};
    const value = {
      allowedAll: parseBoolUnknown(data.allowed_all, false),
      disallowAll: parseBoolUnknown(data.disallow_all, false),
    };
    robotsCache.set(domain, value);
    return value.allowedAll || !value.disallowAll;
  }

  const robotsUrl = `${parsed.protocol}//${domain}/robots.txt`;
  const fetched = await fetchHtmlWithTimeout(robotsUrl, 5000);
  let disallowAll = false;
  let allowedAll = true;
  if (fetched.status >= 200 && fetched.status < 300 && fetched.html) {
    const lines = fetched.html.split(/\r?\n/).map((v) => v.trim());
    let inGlobal = false;
    disallowAll = false;
    allowedAll = true;
    for (const line of lines) {
      if (!line || line.startsWith("#")) continue;
      const low = line.toLowerCase();
      if (low.startsWith("user-agent:")) {
        const ua = cleanString(line.split(":").slice(1).join(":"), 200).toLowerCase();
        inGlobal = ua === "*" || ua === "";
        continue;
      }
      if (!inGlobal) continue;
      if (low.startsWith("disallow:")) {
        const path = cleanString(line.split(":").slice(1).join(":"), 300);
        if (path === "/") {
          disallowAll = true;
          allowedAll = false;
        }
      }
      if (low.startsWith("allow:")) {
        const path = cleanString(line.split(":").slice(1).join(":"), 300);
        if (path === "/" || path.length > 1) {
          allowedAll = true;
        }
      }
    }
  }

  const summary = { allowed_all: allowedAll, disallow_all: disallowAll };
  await env.DB.prepare(
    `INSERT INTO step2_backlink_cache (
      cache_id, cache_key, scope, provider, data_json, updated_at, expires_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(cache_key) DO UPDATE SET
      scope = excluded.scope,
      provider = excluded.provider,
      data_json = excluded.data_json,
      updated_at = excluded.updated_at,
      expires_at = excluded.expires_at`
  )
    .bind(
      uuid("s2bc"),
      cacheKey,
      "robots",
      "internal",
      safeJsonStringify(summary, 4000),
      now,
      now + 24 * 60 * 60 * 1000
    )
    .run();

  const value = { allowedAll, disallowAll };
  robotsCache.set(domain, value);
  return value.allowedAll || !value.disallowAll;
}

async function getBacklinkMetricsCached(
  env: Env,
  input: { urlHash: string; url: string; domain: string }
): Promise<{ backlinks: number; ref_domains: number; authority_metric: number; provider: string; top_anchors: string[] }> {
  const now = nowMs();
  const key = `bl:url:${input.urlHash}`;
  const cached = await env.DB.prepare(
    `SELECT data_json
     FROM step2_backlink_cache
     WHERE cache_key = ? AND expires_at > ?
     LIMIT 1`
  )
    .bind(key, now)
    .first<Record<string, unknown>>();
  if (cached) {
    const data = safeJsonParseObject(cleanString(cached.data_json, 4000)) ?? {};
    return {
      backlinks: clampInt(data.backlinks, 0, 10_000_000, 0),
      ref_domains: clampInt(data.ref_domains, 0, 1_000_000, 0),
      authority_metric: Math.max(0, Number(data.authority_metric ?? 0) || 0),
      provider: cleanString(data.provider, 40) || "semrush",
      top_anchors: parseStringArray(data.top_anchors, 20, 120),
    };
  }

  const placeholder = {
    backlinks: 0,
    ref_domains: 0,
    authority_metric: 0,
    provider: "semrush",
    top_anchors: [] as string[],
  };
  await env.DB.prepare(
    `INSERT INTO step2_backlink_cache (
      cache_id, cache_key, scope, provider, data_json, updated_at, expires_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(cache_key) DO UPDATE SET
      scope = excluded.scope,
      provider = excluded.provider,
      data_json = excluded.data_json,
      updated_at = excluded.updated_at,
      expires_at = excluded.expires_at`
  )
    .bind(
      uuid("s2bc"),
      key,
      "url",
      "semrush",
      safeJsonStringify({ ...placeholder, url: input.url, domain: input.domain }, 4000),
      now,
      now + 7 * 24 * 60 * 60 * 1000
    )
    .run();
  return placeholder;
}

async function saveStep2SerpSnapshot(
  env: Env,
  input: { siteId: string; keyword: string; cluster: string; intent: string; geo: string; serpFeatures: string[]; scrapedAt: number }
): Promise<string> {
  const serpId = uuid("s2serp");
  await env.DB.prepare(
    `INSERT INTO step2_serp_snapshots (
      serp_id, site_id, keyword, cluster, intent, geo, date_yyyymmdd, serp_features_json, scraped_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      serpId,
      input.siteId,
      cleanString(input.keyword, 300),
      cleanString(input.cluster, 120),
      cleanString(input.intent, 40),
      cleanString(input.geo, 120),
      toDateYYYYMMDD(input.scrapedAt),
      safeJsonStringify(input.serpFeatures, 4000),
      input.scrapedAt
    )
    .run();
  return serpId;
}

async function loadLatestStep2SerpSnapshotBeforeDate(
  env: Env,
  input: { siteId: string; keyword: string; geo: string; beforeDate: string }
): Promise<{ serp_id: string; date_yyyymmdd: string } | null> {
  const row = await env.DB.prepare(
    `SELECT serp_id, date_yyyymmdd
     FROM step2_serp_snapshots
     WHERE site_id = ? AND keyword = ? AND geo = ? AND date_yyyymmdd < ?
     ORDER BY date_yyyymmdd DESC, scraped_at DESC
     LIMIT 1`
  )
    .bind(input.siteId, input.keyword, input.geo, input.beforeDate)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    serp_id: cleanString(row.serp_id, 120),
    date_yyyymmdd: cleanString(row.date_yyyymmdd, 20),
  };
}

async function loadStep2SerpResultsForSnapshot(
  env: Env,
  serpId: string
): Promise<Array<{ rank: number; url: string; url_hash: string; domain: string; page_type: string }>> {
  const rows = await env.DB.prepare(
    `SELECT rank, url, url_hash, domain, page_type
     FROM step2_serp_results
     WHERE serp_id = ?
     ORDER BY rank ASC`
  )
    .bind(serpId)
    .all<Record<string, unknown>>();
  return (rows.results ?? []).map((row) => ({
    rank: clampInt(row.rank, 1, 1000, 999),
    url: cleanString(row.url, 2000),
    url_hash: cleanString(row.url_hash, 128),
    domain: cleanString(row.domain, 255),
    page_type: cleanString(row.page_type, 80) || "other",
  }));
}

async function loadLatestStep2PageExtract(
  env: Env,
  urlHash: string,
  beforeDate: string
): Promise<
  | {
      date_yyyymmdd: string;
      title: string | null;
      meta_description: string | null;
      h1_text: string | null;
      word_count: number;
      schema_types_json: string;
      faq_section_present: number;
      pricing_section_present: number;
      testimonials_present: number;
      how_it_works_present: number;
      location_refs_present: number;
    }
  | null
> {
  const row = await env.DB.prepare(
    `SELECT
       date_yyyymmdd, title, meta_description, h1_text, word_count,
       schema_types_json, faq_section_present, pricing_section_present,
       testimonials_present, how_it_works_present, location_refs_present
     FROM step2_page_extracts
     WHERE url_hash = ? AND date_yyyymmdd < ?
     ORDER BY date_yyyymmdd DESC
     LIMIT 1`
  )
    .bind(urlHash, beforeDate)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    date_yyyymmdd: cleanString(row.date_yyyymmdd, 20),
    title: cleanString(row.title, 800) || null,
    meta_description: cleanString(row.meta_description, 800) || null,
    h1_text: cleanString(row.h1_text, 800) || null,
    word_count: clampInt(row.word_count, 0, 2_000_000, 0),
    schema_types_json: cleanString(row.schema_types_json, 12000) || "[]",
    faq_section_present: clampInt(row.faq_section_present, 0, 1, 0),
    pricing_section_present: clampInt(row.pricing_section_present, 0, 1, 0),
    testimonials_present: clampInt(row.testimonials_present, 0, 1, 0),
    how_it_works_present: clampInt(row.how_it_works_present, 0, 1, 0),
    location_refs_present: clampInt(row.location_refs_present, 0, 1, 0),
  };
}

async function loadLatestStep2BacklinkRow(
  env: Env,
  urlHash: string,
  beforeDate: string
): Promise<{ ref_domains: number } | null> {
  const row = await env.DB.prepare(
    `SELECT ref_domains
     FROM step2_url_backlinks
     WHERE url_hash = ? AND date_yyyymmdd < ?
     ORDER BY date_yyyymmdd DESC
     LIMIT 1`
  )
    .bind(urlHash, beforeDate)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    ref_domains: clampInt(row.ref_domains, 0, 1_000_000, 0),
  };
}

async function loadLatestBaselineForSite(
  env: Env,
  siteId: string
): Promise<{ baseline_snapshot_id: string; date_yyyymmdd: string; created_at: number } | null> {
  const row = await env.DB.prepare(
    `SELECT baseline_snapshot_id, date_yyyymmdd, created_at
     FROM step2_baselines
     WHERE site_id = ?
     ORDER BY created_at DESC
     LIMIT 1`
  )
    .bind(siteId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    baseline_snapshot_id: cleanString(row.baseline_snapshot_id, 120),
    date_yyyymmdd: cleanString(row.date_yyyymmdd, 20),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function saveStep2BaselineSnapshot(
  env: Env,
  input: { siteId: string; date: string; parentJobId: string | null; summary: unknown }
): Promise<string> {
  const id = uuid("s2base");
  await env.DB.prepare(
    `INSERT INTO step2_baselines (
      baseline_snapshot_id, site_id, date_yyyymmdd, run_job_id, summary_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)`
  )
    .bind(
      id,
      input.siteId,
      input.date,
      cleanString(input.parentJobId, 120) || null,
      safeJsonStringify(input.summary, 32000),
      nowMs()
    )
    .run();
  return id;
}

function shouldRefreshUrlForDelta(input: {
  rank: number;
  isNewEntrant: boolean;
  movedIntoTop10: boolean;
  movedIntoTop5: boolean;
  movedIntoTop3: boolean;
  lastExtractDate: string | null;
  todayDate: string;
}): boolean {
  if (input.rank <= 5) return true;
  if (input.isNewEntrant) return true;
  if (input.movedIntoTop10 || input.movedIntoTop5 || input.movedIntoTop3) return true;
  if (!input.lastExtractDate) return true;
  return input.lastExtractDate < toDateYYYYMMDD(nowMs() - 7 * 24 * 60 * 60 * 1000);
}

function diffSchemaTypes(prevJson: string, currJson: string): { added: string[]; removed: string[] } {
  const prev = new Set(parseStringArray(safeJsonParseObject(`{"a":${prevJson}}`)?.a, 50, 100).map((v) => v.toLowerCase()));
  const curr = new Set(parseStringArray(safeJsonParseObject(`{"a":${currJson}}`)?.a, 50, 100).map((v) => v.toLowerCase()));
  const added: string[] = [];
  const removed: string[] = [];
  for (const v of curr) if (!prev.has(v)) added.push(v);
  for (const v of prev) if (!curr.has(v)) removed.push(v);
  return { added, removed };
}

async function saveStep2SerpDiff(
  env: Env,
  input: {
    siteId: string;
    date: string;
    keyword: string;
    geo: string;
    previous: Array<{ rank: number; url: string; url_hash: string; page_type: string }>;
    current: Array<{ rank: number; url: string; url_hash: string; page_type: string }>;
    baseline: Array<{ rank: number; url: string; url_hash: string; page_type: string }>;
    serpFeatureDelta: Record<string, unknown>;
  }
): Promise<void> {
  const prevByHash = new Map(input.previous.map((row) => [row.url_hash, row]));
  const currByHash = new Map(input.current.map((row) => [row.url_hash, row]));
  const baselineByHash = new Map(input.baseline.map((row) => [row.url_hash, row]));

  const entered = input.current.filter((row) => !prevByHash.has(row.url_hash)).map((row) => row.url);
  const dropped = input.previous.filter((row) => !currByHash.has(row.url_hash)).map((row) => row.url);
  const rankDelta = input.current
    .filter((row) => prevByHash.has(row.url_hash))
    .map((row) => ({
      url: row.url,
      from_rank: prevByHash.get(row.url_hash)?.rank ?? null,
      to_rank: row.rank,
      delta: (prevByHash.get(row.url_hash)?.rank ?? row.rank) - row.rank,
    }));

  const formatDelta = {
    previous: input.previous.reduce<Record<string, number>>((acc, row) => {
      acc[row.page_type] = (acc[row.page_type] ?? 0) + 1;
      return acc;
    }, {}),
    current: input.current.reduce<Record<string, number>>((acc, row) => {
      acc[row.page_type] = (acc[row.page_type] ?? 0) + 1;
      return acc;
    }, {}),
  };

  const baselineDelta = input.current
    .filter((row) => baselineByHash.has(row.url_hash))
    .map((row) => ({
      url: row.url,
      baseline_rank: baselineByHash.get(row.url_hash)?.rank ?? null,
      current_rank: row.rank,
      delta: (baselineByHash.get(row.url_hash)?.rank ?? row.rank) - row.rank,
    }));

  await env.DB.prepare(
    `INSERT INTO step2_serp_diffs (
      diff_id, site_id, date_yyyymmdd, keyword, geo,
      entered_urls_json, dropped_urls_json, rank_delta_json,
      serp_feature_delta_json, format_delta_json, baseline_delta_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("s2sd"),
      input.siteId,
      input.date,
      input.keyword,
      input.geo,
      safeJsonStringify(entered, 12000),
      safeJsonStringify(dropped, 12000),
      safeJsonStringify(rankDelta, 32000),
      safeJsonStringify(input.serpFeatureDelta, 4000),
      safeJsonStringify(formatDelta, 16000),
      safeJsonStringify(baselineDelta, 32000),
      nowMs()
    )
    .run();
}

async function saveStep2UrlDiff(
  env: Env,
  input: {
    siteId: string;
    date: string;
    keyword: string;
    url: string;
    urlHash: string;
    previousExtract: Awaited<ReturnType<typeof loadLatestStep2PageExtract>>;
    currentExtract: Step2PageExtract;
    previousRefDomains: number;
    currentRefDomains: number;
    previousInbound: number;
    currentInbound: number;
  }
): Promise<void> {
  const prev = input.previousExtract;
  if (!prev) return;
  const fieldChanges: Record<string, unknown> = {};
  if ((prev.title ?? "") !== (input.currentExtract.title ?? "")) {
    fieldChanges.title = { from: prev.title, to: input.currentExtract.title };
  }
  if ((prev.meta_description ?? "") !== (input.currentExtract.meta_description ?? "")) {
    fieldChanges.meta_description = { from: prev.meta_description, to: input.currentExtract.meta_description };
  }
  if ((prev.h1_text ?? "") !== (input.currentExtract.h1_text ?? "")) {
    fieldChanges.h1 = { from: prev.h1_text, to: input.currentExtract.h1_text };
  }
  const schemaDelta = diffSchemaTypes(prev.schema_types_json, input.currentExtract.schema_types_json);
  const moduleChanges = {
    faq: { from: prev.faq_section_present, to: input.currentExtract.faq_section_present },
    pricing: { from: prev.pricing_section_present, to: input.currentExtract.pricing_section_present },
    testimonials: { from: prev.testimonials_present, to: input.currentExtract.testimonials_present },
    how_it_works: { from: prev.how_it_works_present, to: input.currentExtract.how_it_works_present },
    location_refs: { from: prev.location_refs_present, to: input.currentExtract.location_refs_present },
    schema_delta: schemaDelta,
  };

  await env.DB.prepare(
    `INSERT INTO step2_url_diffs (
      diff_id, site_id, date_yyyymmdd, keyword, url, url_hash,
      field_changes_json, module_changes_json,
      word_count_delta, internal_inbound_delta, ref_domains_delta, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("s2ud"),
      input.siteId,
      input.date,
      input.keyword,
      input.url,
      input.urlHash,
      safeJsonStringify(fieldChanges, 16000),
      safeJsonStringify(moduleChanges, 16000),
      input.currentExtract.word_count - prev.word_count,
      input.currentInbound - input.previousInbound,
      input.currentRefDomains - input.previousRefDomains,
      nowMs()
    )
    .run();
}

async function upsertStep2PageExtract(env: Env, extract: Step2PageExtract, scrapedAt: number): Promise<void> {
  const urlHash = await sha256Hex(extract.url.toLowerCase());
  const date = toDateYYYYMMDD(scrapedAt);
  await env.DB.prepare(
    `INSERT INTO step2_page_extracts (
      extract_id, url_hash, url, domain, date_yyyymmdd,
      title, meta_description, robots_meta, canonical_url, hreflang_count,
      h1_text, h2_json, h3_json, word_count, schema_types_json,
      internal_links_out_count, internal_anchors_json, external_links_out_count, external_anchors_json,
      image_count, alt_coverage_rate, keyword_placement_flags_json,
      faq_section_present, pricing_section_present, testimonials_present, location_refs_present, how_it_works_present,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(url_hash, date_yyyymmdd) DO UPDATE SET
      title = excluded.title,
      meta_description = excluded.meta_description,
      robots_meta = excluded.robots_meta,
      canonical_url = excluded.canonical_url,
      hreflang_count = excluded.hreflang_count,
      h1_text = excluded.h1_text,
      h2_json = excluded.h2_json,
      h3_json = excluded.h3_json,
      word_count = excluded.word_count,
      schema_types_json = excluded.schema_types_json,
      internal_links_out_count = excluded.internal_links_out_count,
      internal_anchors_json = excluded.internal_anchors_json,
      external_links_out_count = excluded.external_links_out_count,
      external_anchors_json = excluded.external_anchors_json,
      image_count = excluded.image_count,
      alt_coverage_rate = excluded.alt_coverage_rate,
      keyword_placement_flags_json = excluded.keyword_placement_flags_json,
      faq_section_present = excluded.faq_section_present,
      pricing_section_present = excluded.pricing_section_present,
      testimonials_present = excluded.testimonials_present,
      location_refs_present = excluded.location_refs_present,
      how_it_works_present = excluded.how_it_works_present,
      updated_at = excluded.updated_at`
  )
    .bind(
      uuid("s2ext"),
      urlHash,
      extract.url,
      extract.domain,
      date,
      extract.title,
      extract.meta_description,
      extract.robots_meta,
      extract.canonical_url,
      extract.hreflang_count,
      extract.h1_text,
      extract.h2_json,
      extract.h3_json,
      extract.word_count,
      extract.schema_types_json,
      extract.internal_links_out_count,
      extract.internal_anchors_json,
      extract.external_links_out_count,
      extract.external_anchors_json,
      extract.image_count,
      extract.alt_coverage_rate,
      extract.keyword_placement_flags_json,
      extract.faq_section_present,
      extract.pricing_section_present,
      extract.testimonials_present,
      extract.location_refs_present,
      extract.how_it_works_present,
      scrapedAt,
      scrapedAt
    )
    .run();
}

function rankBand(rank: number): "top_3" | "top_4_5" | "top_6_10" | "rank_11_20" {
  if (rank <= 3) return "top_3";
  if (rank <= 5) return "top_4_5";
  if (rank <= 10) return "top_6_10";
  return "rank_11_20";
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const s = [...values].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  if (s.length % 2 === 1) return s[mid];
  return (s[mid - 1] + s[mid]) / 2;
}

async function buildAndStoreStep2Analyses(
  env: Env,
  input: {
    siteId: string;
    date: string;
    keyword: string;
    rows: Array<{ rank: number; word_count: number; schema_types: string[]; page_type: string; internal_inbound_count: number; ref_domains: number }>;
  }
): Promise<void> {
  const byBand = new Map<string, Array<{ word_count: number; hasFaqSchema: boolean; page_type: string; internal_inbound_count: number; ref_domains: number }>>();
  for (const row of input.rows) {
    const band = rankBand(row.rank);
    const list = byBand.get(band) ?? [];
    list.push({
      word_count: row.word_count,
      hasFaqSchema: row.schema_types.some((v) => v.toLowerCase().includes("faq")),
      page_type: row.page_type,
      internal_inbound_count: row.internal_inbound_count,
      ref_domains: row.ref_domains,
    });
    byBand.set(band, list);
  }
  const summarizeBand = (band: string) => {
    const list = byBand.get(band) ?? [];
    return {
      count: list.length,
      median_word_count: median(list.map((v) => v.word_count)),
      faq_schema_rate: list.length > 0 ? Number((list.filter((v) => v.hasFaqSchema).length / list.length).toFixed(3)) : 0,
      median_internal_inbound_links: median(list.map((v) => v.internal_inbound_count)),
      median_ref_domains: median(list.map((v) => v.ref_domains)),
      page_type_distribution: list.reduce<Record<string, number>>((acc, row) => {
        acc[row.page_type] = (acc[row.page_type] ?? 0) + 1;
        return acc;
      }, {}),
    };
  };
  const findings = {
    keyword: input.keyword,
    rank_bands: {
      top_3: summarizeBand("top_3"),
      top_4_5: summarizeBand("top_4_5"),
      top_6_10: summarizeBand("top_6_10"),
      rank_11_20: summarizeBand("rank_11_20"),
    },
  };
  const recommendations = {
    requirements: [
      "Match winning page type for the keyword intent",
      "Add FAQ + pricing modules when top bands show those patterns",
      "Increase internal links from relevant cluster pages",
      "Expand entity coverage where top bands have higher word-count and richer schema",
    ],
    page_checklist: [
      "Title and H1 aligned with keyword + intent",
      "FAQ and How-it-works sections where applicable",
      "Schema coverage for LocalBusiness/Product/FAQ as relevant",
      "Internal link anchors aligned to cluster terms",
    ],
  };
  await env.DB.prepare(
    `INSERT INTO step2_ai_analyses (
      analysis_id, site_id, date_yyyymmdd, scope, scope_key, rank_band, findings_json, recommendations_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("s2an"),
      input.siteId,
      input.date,
      "keyword",
      input.keyword,
      "all",
      safeJsonStringify(findings, 32000),
      safeJsonStringify(recommendations, 32000),
      nowMs()
    )
    .run();
}

async function runStep2DailyHarvest(
  env: Env,
  site: Step1SiteRecord,
  opts: { maxKeywords: number; maxResults: number; geo: string; parentJobId?: string | null; runType?: "baseline" | "delta" | "auto" }
): Promise<{
  run_type: "baseline" | "delta";
  baseline_snapshot_id: string | null;
  site_id: string;
  date: string;
  keyword_count: number;
  serp_rows: number;
  page_extracts: number;
  keyword_summaries: Array<Record<string, unknown>>;
  limits: {
    keywords_cap: number;
    serp_results_cap: number;
    url_fetches_cap_per_day: number;
    backlink_enrich_cap_per_day: number;
    graph_domains_cap_per_day: number;
  };
}> {
  const result = await loadLatestStep1KeywordResearchResults(env, site.site_id);
  if (!result) {
    throw new Error("keyword_research_results_not_found");
  }
  const targets = parseStep2KeywordTargets(result).slice(
    0,
    Math.max(1, Math.min(SITE_KEYWORD_CAP, opts.maxKeywords))
  );
  if (targets.length < 1) {
    throw new Error("step2_contract_empty");
  }
  const siteInput = safeJsonParseObject(site.input_json) ?? {};
  const plan = getSitePlanFromInput(siteInput);
  const providerProfile = await loadSiteProviderProfile(env, site.site_id);
  const geos = [opts.geo];
  if (plan.metro_proxy && plan.metro) {
    geos.push(`metro:${plan.metro}`);
  }
  const date = toDateYYYYMMDD(nowMs());
  const latestBaseline = await loadLatestBaselineForSite(env, site.site_id);
  const defaultRunType: "baseline" | "delta" =
    !latestBaseline || latestBaseline.date_yyyymmdd < toDateYYYYMMDD(nowMs() - 30 * 24 * 60 * 60 * 1000)
      ? "baseline"
      : "delta";
  const requestedRunType = opts.runType ?? "auto";
  const runType: "baseline" | "delta" =
    requestedRunType === "baseline" || requestedRunType === "delta" ? requestedRunType : defaultRunType;
  let serpRowsInserted = 0;
  let pageExtracts = 0;
  let urlFetches = 0;
  let backlinkEnriches = 0;
  const graphDomainsTouched = new Set<string>();
  const keywordSummaries: Array<Record<string, unknown>> = [];
  const robotsCache = new Map<string, { allowedAll: boolean; disallowAll: boolean }>();

  for (const target of targets) {
    const childJobId = await createJobRecord(env, {
      siteId: site.site_id,
      type: "site_daily_keyword_run",
      request: {
        parent_job_id: cleanString(opts.parentJobId, 120) || null,
        date,
        keyword: target.keyword,
        cluster: target.cluster,
        priority: target.priority,
      },
    });
    let keywordSerpRows = 0;
    let keywordPageExtracts = 0;
    let keywordErrors = 0;
    for (const geo of geos) {
      const prevSnapshot = await loadLatestStep2SerpSnapshotBeforeDate(env, {
        siteId: site.site_id,
        keyword: target.keyword,
        geo,
        beforeDate: date,
      });
      const previousRows = prevSnapshot ? await loadStep2SerpResultsForSnapshot(env, prevSnapshot.serp_id) : [];
      const baselineRows =
        latestBaseline && latestBaseline.date_yyyymmdd < date
          ? (
              await env.DB.prepare(
                `SELECT r.rank, r.url, r.url_hash, r.domain, r.page_type
                 FROM step2_serp_results r
                 JOIN step2_serp_snapshots s ON s.serp_id = r.serp_id
                 WHERE s.site_id = ? AND s.keyword = ? AND s.geo = ? AND s.date_yyyymmdd = ?
                 ORDER BY r.rank ASC`
              )
                .bind(site.site_id, target.keyword, geo, latestBaseline.date_yyyymmdd)
                .all<Record<string, unknown>>()
            ).results?.map((row) => ({
              rank: clampInt(row.rank, 1, 1000, 999),
              url: cleanString(row.url, 2000),
              url_hash: cleanString(row.url_hash, 128),
              domain: cleanString(row.domain, 255),
              page_type: cleanString(row.page_type, 80) || "other",
            })) ?? []
          : [];

      const serp = await collectAndPersistSerpTop20(env, {
        userId: site.site_id,
        phrase: target.keyword,
        region: mapGeoForProvider(
          geo,
          normalizeRegion({ country: "US", language: "en", metro: geo.startsWith("metro:") ? geo.slice(6) : "" }),
          providerProfile.geo_provider
        ).region,
        device: "desktop",
        maxResults: Math.max(1, Math.min(SITE_SERP_RESULTS_CAP, opts.maxResults)),
        proxyUrl: null,
        provider: providerProfile.serp_provider,
        geoProvider: providerProfile.geo_provider,
        geoLabel: geo,
        auditJobId: childJobId,
      });
      if (serp.ok === false) {
        keywordErrors += 1;
        continue;
      }

      const step2SerpId = await saveStep2SerpSnapshot(env, {
        siteId: site.site_id,
        keyword: target.keyword,
        cluster: target.cluster,
        intent: target.intent,
        geo,
        serpFeatures: [],
        scrapedAt: nowMs(),
      });

      const analysisRows: Array<{
        rank: number;
        word_count: number;
        schema_types: string[];
        page_type: string;
        internal_inbound_count: number;
        ref_domains: number;
      }> = [];
      const currentRowsForDiff: Array<{ rank: number; url: string; url_hash: string; page_type: string }> = [];
      const prevByHash = new Map(previousRows.map((r) => [r.url_hash, r]));

      for (const row of serp.rows) {
        const pageType = classifySerpResultPageType(row.url, row.title, null);
        const urlHash = await sha256Hex(cleanString(row.url, 2000).toLowerCase());
        currentRowsForDiff.push({ rank: row.rank, url: row.url, url_hash: urlHash, page_type: pageType });
        await env.DB.prepare(
          `INSERT INTO step2_serp_results (
            result_id, serp_id, rank, url, url_hash, domain, page_type, title_snippet, desc_snippet, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
          .bind(
            uuid("s2res"),
            step2SerpId,
            row.rank,
            row.url,
            urlHash,
            normalizeRootDomain(row.domain || normalizeDomain(row.url)),
            pageType,
            cleanString(row.title, 500) || null,
            cleanString(row.snippet, 1000) || null,
            nowMs()
        )
          .run();
        serpRowsInserted += 1;
        keywordSerpRows += 1;
      }

      await saveStep2SerpDiff(env, {
        siteId: site.site_id,
        date,
        keyword: target.keyword,
        geo,
        previous: previousRows.map((r) => ({ rank: r.rank, url: r.url, url_hash: r.url_hash, page_type: r.page_type })),
        current: currentRowsForDiff,
        baseline: baselineRows.map((r) => ({ rank: r.rank, url: r.url, url_hash: r.url_hash, page_type: r.page_type })),
        serpFeatureDelta: {},
      });

      const refreshCandidates = currentRowsForDiff.filter((row) => {
        if (runType === "baseline") return true;
        const prev = prevByHash.get(row.url_hash);
        const isNewEntrant = !prev;
        const movedIntoTop10 = !!prev && prev.rank > 10 && row.rank <= 10;
        const movedIntoTop5 = !!prev && prev.rank > 5 && row.rank <= 5;
        const movedIntoTop3 = !!prev && prev.rank > 3 && row.rank <= 3;
        return row.rank <= 5 || isNewEntrant || movedIntoTop10 || movedIntoTop5 || movedIntoTop3;
      });

      for (const row of refreshCandidates) {
        if (urlFetches >= SITE_MAX_URL_FETCHES_PER_DAY) {
          break;
        }
        if (!(await canFetchByRobots(env, row.url, robotsCache))) {
          continue;
        }
        urlFetches += 1;

        const pageType = row.page_type;
        const urlHash = row.url_hash;
        const previousExtract = await loadLatestStep2PageExtract(env, urlHash, date);

        const cachedHtml = await loadHtmlCache(env, urlHash, nowMs());
        const fetched = await fetchPageHtmlByProvider(env, {
          pageProvider: providerProfile.page_provider,
          geoProvider: providerProfile.geo_provider,
          geoLabel: geo,
          url: row.url,
          timeoutMs: 8000,
          cache: cachedHtml
            ? {
                etag: cachedHtml.etag,
                last_modified: cachedHtml.last_modified,
                html: cachedHtml.html_snapshot,
              }
            : null,
        });
        const html = fetched.html;
        if (!html) continue;
        if (fetched.status !== 304 && fetched.extractor_mode === "direct_fetch") {
          await upsertHtmlCache(env, {
            urlHash,
            url: row.url,
            etag: fetched.etag,
            lastModified: fetched.last_modified,
            html,
            now: nowMs(),
            ttlMs: 7 * 24 * 60 * 60 * 1000,
          });
        }
        await createArtifactRecord(env, {
          jobId: childJobId,
          kind: "step2.page.fetch.audit",
          payload: {
            keyword: target.keyword,
            geo,
            url: row.url,
            extractor_mode: fetched.extractor_mode,
            fallback_reason: fetched.fallback_reason,
            decodo_raw_payload_sha256: fetched.raw_payload_sha256,
            provider_task_id: fetched.provider_task_id,
            status: fetched.status,
          },
        });

        const pageExtract = buildStep2PageExtract(row.url, html, target.keyword);
        await upsertStep2PageExtract(env, pageExtract, nowMs());
        pageExtracts += 1;
        keywordPageExtracts += 1;

        if (graphDomainsTouched.has(pageExtract.domain) || graphDomainsTouched.size < SITE_MAX_GRAPH_DOMAINS_PER_DAY) {
          graphDomainsTouched.add(pageExtract.domain);
          for (const edge of pageExtract.internal_edges.slice(0, 120)) {
            await env.DB.prepare(
              `INSERT INTO step2_internal_graph_edges (
                edge_id, domain, date_yyyymmdd, from_url, to_url, anchor, created_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?)`
            )
              .bind(
                uuid("s2edge"),
                pageExtract.domain,
                date,
                pageExtract.url,
                edge.to_url,
                edge.anchor,
                nowMs()
              )
              .run();
          }
        }

        const bl =
          backlinkEnriches < SITE_MAX_BACKLINK_ENRICH_PER_DAY
            ? await getBacklinkMetricsCached(env, {
                urlHash,
                url: row.url,
                domain: pageExtract.domain,
              })
            : { backlinks: 0, ref_domains: 0, authority_metric: 0, provider: "semrush", top_anchors: [] };
        if (backlinkEnriches < SITE_MAX_BACKLINK_ENRICH_PER_DAY) {
          backlinkEnriches += 1;
        }
        await env.DB.prepare(
          `INSERT INTO step2_url_backlinks (
            backlink_id, url_hash, url, domain, date_yyyymmdd,
            backlinks, ref_domains, follow_nofollow_json, link_types_json, top_anchors_json, authority_provider, authority_metric, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(url_hash, date_yyyymmdd) DO NOTHING`
        )
          .bind(
            uuid("s2ubl"),
            urlHash,
            row.url,
            pageExtract.domain,
            date,
            bl.backlinks,
            bl.ref_domains,
            safeJsonStringify({ follow: 0, nofollow: 0 }),
            safeJsonStringify({ text: 0, image: 0, redirect: 0, form: 0 }),
            safeJsonStringify(bl.top_anchors),
            bl.provider,
            bl.authority_metric,
            nowMs()
          )
          .run();

        await env.DB.prepare(
          `INSERT INTO step2_domain_backlinks (
            domain_backlink_id, domain, date_yyyymmdd, authority_provider, authority_metric, ref_domains, topical_categories_json, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(domain, date_yyyymmdd) DO NOTHING`
        )
          .bind(uuid("s2dbl"), pageExtract.domain, date, "semrush", 0, 0, safeJsonStringify([]), nowMs())
          .run();

        const inboundRow = await env.DB.prepare(
          `SELECT COUNT(*) AS c
           FROM step2_internal_graph_edges
           WHERE date_yyyymmdd = ? AND to_url = ?`
        )
          .bind(date, pageExtract.url)
          .first<Record<string, unknown>>();
        const inboundCount = clampInt(inboundRow?.c, 0, 1_000_000, 0);
        const prevInboundRow =
          previousExtract == null
            ? null
            : await env.DB.prepare(
                `SELECT COUNT(*) AS c
                 FROM step2_internal_graph_edges
                 WHERE date_yyyymmdd = ? AND to_url = ?`
              )
                .bind(previousExtract.date_yyyymmdd, pageExtract.url)
                .first<Record<string, unknown>>();
        const prevInboundCount = clampInt(prevInboundRow?.c, 0, 1_000_000, 0);
        const prevBacklinks = await loadLatestStep2BacklinkRow(env, urlHash, date);

        analysisRows.push({
          rank: row.rank,
          word_count: pageExtract.word_count,
          schema_types: parseStringArray(safeJsonParseObject(`{"a":${pageExtract.schema_types_json}}`)?.a, 20, 80),
          page_type: pageType,
          internal_inbound_count: inboundCount,
          ref_domains: bl.ref_domains,
        });

        await saveStep2UrlDiff(env, {
          siteId: site.site_id,
          date,
          keyword: target.keyword,
          url: row.url,
          urlHash,
          previousExtract,
          currentExtract: pageExtract,
          previousRefDomains: prevBacklinks?.ref_domains ?? 0,
          currentRefDomains: bl.ref_domains,
          previousInbound: prevInboundCount,
          currentInbound: inboundCount,
        });
      }

      await buildAndStoreStep2Analyses(env, {
        siteId: site.site_id,
        date,
        keyword: target.keyword,
        rows: analysisRows,
      });
    }
    if (keywordSerpRows > 0) {
      await finalizeJobSuccess(env, childJobId, Math.max(1, keywordPageExtracts));
    } else {
      await finalizeJobFailure(env, childJobId, "keyword_daily_run_failed", {
        keyword: target.keyword,
        geo_count: geos.length,
        errors: keywordErrors,
      });
    }
    keywordSummaries.push({
      keyword: target.keyword,
      priority: target.priority,
      child_job_id: childJobId,
      serp_rows: keywordSerpRows,
      page_extracts: keywordPageExtracts,
      errors: keywordErrors,
    });
  }

  const baselineSnapshotId =
    runType === "baseline"
      ? await saveStep2BaselineSnapshot(env, {
          siteId: site.site_id,
          date,
          parentJobId: cleanString(opts.parentJobId, 120) || null,
          summary: {
            keyword_count: targets.length,
            serp_rows: serpRowsInserted,
            page_extracts: pageExtracts,
          },
        })
      : null;

  await env.DB.prepare(
    `INSERT INTO step2_daily_reports (
      report_id, site_id, date_yyyymmdd, run_type, baseline_snapshot_id, summary_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      uuid("s2rep"),
      site.site_id,
      date,
      runType,
      baselineSnapshotId,
      safeJsonStringify(
        {
          keyword_count: targets.length,
          serp_rows: serpRowsInserted,
          page_extracts: pageExtracts,
          keyword_summaries: keywordSummaries,
          limits: {
            keywords_cap: SITE_KEYWORD_CAP,
            serp_results_cap: SITE_SERP_RESULTS_CAP,
            url_fetches_cap_per_day: SITE_MAX_URL_FETCHES_PER_DAY,
            backlink_enrich_cap_per_day: SITE_MAX_BACKLINK_ENRICH_PER_DAY,
            graph_domains_cap_per_day: SITE_MAX_GRAPH_DOMAINS_PER_DAY,
          },
        },
        128000
      ),
      nowMs()
    )
    .run();

  return {
    run_type: runType,
    baseline_snapshot_id: baselineSnapshotId,
    site_id: site.site_id,
    date,
    keyword_count: targets.length,
    serp_rows: serpRowsInserted,
    page_extracts: pageExtracts,
    keyword_summaries: keywordSummaries,
    limits: {
      keywords_cap: SITE_KEYWORD_CAP,
      serp_results_cap: SITE_SERP_RESULTS_CAP,
      url_fetches_cap_per_day: SITE_MAX_URL_FETCHES_PER_DAY,
      backlink_enrich_cap_per_day: SITE_MAX_BACKLINK_ENRICH_PER_DAY,
      graph_domains_cap_per_day: SITE_MAX_GRAPH_DOMAINS_PER_DAY,
    },
  };
}

async function listStep2EligibleSites(env: Env, limit = 30): Promise<Step1SiteRecord[]> {
  const rows = await env.DB.prepare(
    `SELECT
      site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
      local_mode, input_json, site_profile_json, last_analysis_at, last_research_at, created_at, updated_at
     FROM wp_ai_seo_sites
     WHERE last_research_at IS NOT NULL
     ORDER BY updated_at DESC
     LIMIT ?`
  )
    .bind(Math.max(1, Math.min(limit, 100)))
    .all<Record<string, unknown>>();
  return (rows.results ?? []).map((row) => ({
    site_id: cleanString(row.site_id, 64),
    site_url: cleanString(row.site_url, 2000),
    site_name: cleanString(row.site_name, 200) || null,
    business_address: cleanString(row.business_address, 400) || null,
    primary_location_hint: cleanString(row.primary_location_hint, 200) || null,
    site_type_hint: cleanString(row.site_type_hint, 60) || null,
    local_mode: clampInt(row.local_mode, 0, 1, 0) === 1,
    input_json: cleanString(row.input_json, 320000),
    site_profile_json: cleanString(row.site_profile_json, 32000),
    last_analysis_at: clampInt(row.last_analysis_at, 0, Number.MAX_SAFE_INTEGER, 0),
    last_research_at:
      row.last_research_at == null ? null : clampInt(row.last_research_at, 0, Number.MAX_SAFE_INTEGER, 0),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    updated_at: clampInt(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
  }));
}

async function loadLatestStep2Report(env: Env, siteId: string, dateFilter: string): Promise<Record<string, unknown>> {
  const daily = await env.DB.prepare(
    `SELECT run_type, baseline_snapshot_id, summary_json, created_at
     FROM step2_daily_reports
     WHERE site_id = ? AND (? = '' OR date_yyyymmdd = ?)
     ORDER BY created_at DESC
     LIMIT 1`
  )
    .bind(siteId, dateFilter, dateFilter)
    .first<Record<string, unknown>>();
  const analyses = await env.DB.prepare(
    `SELECT scope_key, findings_json, recommendations_json, created_at
     FROM step2_ai_analyses
     WHERE site_id = ? AND scope = 'keyword' AND (? = '' OR date_yyyymmdd = ?)
     ORDER BY created_at DESC
     LIMIT 400`
  )
    .bind(siteId, dateFilter, dateFilter)
    .all<Record<string, unknown>>();
  const rows = analyses.results ?? [];
  return {
    site_id: siteId,
    date: dateFilter || null,
    daily_report:
      daily == null
        ? null
        : {
            run_type: cleanString(daily.run_type, 20),
            baseline_snapshot_id: cleanString(daily.baseline_snapshot_id, 120) || null,
            summary: safeJsonParseObject(cleanString(daily.summary_json, 128000)) ?? {},
            created_at: clampInt(daily.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
          },
    analysis_count: rows.length,
    keyword_reports: rows.map((row) => ({
      keyword: cleanString(row.scope_key, 300),
      findings: safeJsonParseObject(cleanString(row.findings_json, 32000)) ?? {},
      recommendations: safeJsonParseObject(cleanString(row.recommendations_json, 32000)) ?? {},
      created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    })),
  };
}

type Step3TaskCategory =
  | "ON_PAGE"
  | "TECHNICAL_SEO"
  | "LOCAL_SEO"
  | "CONTENT"
  | "AUTHORITY"
  | "SOCIAL"
  | "MEASUREMENT";

type Step3TaskMode = "AUTO" | "DIY" | "TEAM";
type Step3TaskPriority = "P0" | "P1" | "P2" | "P3";
type Step3TaskEffort = "S" | "M" | "L";
type Step3TaskStatus = "NEW" | "READY" | "BLOCKED" | "IN_PROGRESS" | "DONE" | "SKIPPED" | "FAILED";

type Step3TaskType =
  | "SERVICE_PAGE_UPGRADE"
  | "LOCATION_PAGE_CREATE"
  | "LOCATION_PAGE_UPGRADE"
  | "FAQ_ADD"
  | "FAQ_SCHEMA_ADD"
  | "TITLE_META_TEST"
  | "CONTENT_MODULE_ADD"
  | "MEDIA_OPTIMIZE"
  | "INTERNAL_LINKING_SILO_BUILD"
  | "INTERNAL_LINKING_BOOST_MONEY_PAGE"
  | "INDEXATION_CLEANUP"
  | "DUPLICATE_CONTENT_REDUCE"
  | "CWV_OPTIMIZE"
  | "REDIRECT_FIX"
  | "BROKEN_LINK_FIX"
  | "SITEMAP_HEALTH"
  | "GBP_OPTIMIZE"
  | "GBP_POST_PLAN_WEEKLY"
  | "GBP_QA_SEED"
  | "REVIEW_REQUEST_CAMPAIGN_SETUP"
  | "REVIEW_RESPONSE_TEMPLATES"
  | "CITATION_AUDIT"
  | "CITATION_CREATE"
  | "CITATION_CLEANUP"
  | "LOCAL_LANDING_PAGES_STRATEGY"
  | "BLOG_POST_DRAFT"
  | "SERVICE_FAQ_POST_DRAFT"
  | "COST_GUIDE_DRAFT"
  | "COMPARISON_PAGE_DRAFT"
  | "SEASONAL_PAGE_DRAFT"
  | "CONTENT_REFRESH"
  | "LINK_RECLAMATION"
  | "AUTHORITY_GAP_PLAN"
  | "LINK_RISK_AUDIT"
  | "OUTREACH_TARGET_LIST"
  | "DIGITAL_PR_IDEA"
  | "LOCAL_PARTNERSHIP_OPPORTUNITIES"
  | "SPONSORSHIP_OPPORTUNITIES"
  | "NICHE_DIRECTORY_TARGETS"
  | "COMPETITOR_SOCIAL_PROFILE_DISCOVERY"
  | "SOCIAL_AUDIT_COMPETITOR_THEMES"
  | "SOCIAL_PLAN_WEEKLY"
  | "SOCIAL_POST_DRAFT"
  | "SOCIAL_CREATIVE_BRIEF"
  | "SOCIAL_PUBLISH_SCHEDULE"
  | "SOCIAL_PERFORMANCE_REVIEW"
  | "GSC_CONNECT"
  | "GA4_CONNECT"
  | "CALL_TRACKING_SETUP"
  | "CONVERSION_EVENTS_SETUP"
  | "KPI_DASHBOARD_UPDATE"
  | "ALERT_RULE_CREATE";

type Step3TaskV1 = {
  schema_version: "task.v1";
  task_id: string;
  site_id: string;
  site_run_id: string | null;
  created_at: string;
  updated_at: string;
  category: Step3TaskCategory;
  type: Step3TaskType;
  title: string;
  summary: string;
  priority: Step3TaskPriority;
  effort: Step3TaskEffort;
  confidence: number;
  estimated_impact: {
    seo: "low" | "med" | "high";
    leads: "low" | "med" | "high";
    time_to_effect_days: number;
  };
  mode: Step3TaskMode;
  requires: {
    access: string[];
    inputs: string[];
    approvals: string[];
  };
  status: Step3TaskStatus;
  blockers: Array<{ code: string; message: string }>;
  scope: {
    keyword_id: string | null;
    keyword: string | null;
    cluster: string | null;
    target_url: string | null;
    target_slug: string | null;
    geo: string | null;
  };
  evidence: {
    based_on: string[];
    citations: Array<{ kind: string; text: string; data: Record<string, unknown> }>;
  };
  instructions: {
    steps: string[];
    acceptance_criteria: string[];
    guardrails: string[];
  };
  automation: {
    can_auto_apply: boolean;
    auto_apply_default: boolean;
    actions: Array<{ action_type: string; payload: Record<string, unknown> }>;
  };
  outputs: {
    artifacts: Array<{ kind: string; data: Record<string, unknown> }>;
  };
  dependencies: {
    depends_on_task_ids: string[];
    supersedes_task_ids: string[];
  };
};

type Step3RiskFlag = {
  risk_type: string;
  severity: "low" | "medium" | "high";
  message: string;
  blocked_action: string | null;
};

type Step3CompetitorCandidate = {
  domain: string;
  appearance_count: number;
  avg_rank: number;
  sample_urls: string[];
  is_directory: boolean;
};

type Step3KeywordSignal = {
  keyword: string;
  top3_faq_schema_rate: number;
  top3_median_inbound_links: number;
  top3_median_ref_domains: number;
  top6_10_median_ref_domains: number;
  top3_page_types: Record<string, number>;
};

type Step3OwnPageSignal = {
  target_url: string;
  target_slug: string | null;
  inbound_internal_links: number;
  ref_domains: number;
  has_faq_schema: boolean;
  has_pricing_module: boolean;
};

type Step3DiffSignal = {
  keyword: string;
  new_top3_entrant: boolean;
};

type Step3MozKeywordSignal = {
  keyword: string;
  top3_median_pa: number;
  top3_median_da: number;
  top3_median_spam: number;
  own_pa: number | null;
  own_da: number | null;
  own_spam: number | null;
  top3_median_ref_domains: number;
  own_ref_domains: number | null;
  available: boolean;
};

function isDirectoryDomain(domain: string): boolean {
  const d = cleanString(domain, 255).toLowerCase();
  if (!d) return false;
  const known = [
    "yelp.com",
    "angi.com",
    "angieslist.com",
    "homeadvisor.com",
    "thumbtack.com",
    "mapquest.com",
    "yellowpages.com",
    "bbb.org",
    "nextdoor.com",
    "tripadvisor.com",
    "foursquare.com",
    "superpages.com",
  ];
  return known.some((value) => d === value || d.endsWith(`.${value}`));
}

function parseStep1PrimaryFromResults(result: Record<string, unknown>): Array<Record<string, unknown>> {
  const rows = Array.isArray(result.primary_top_10) ? result.primary_top_10 : [];
  const out: Array<Record<string, unknown>> = [];
  for (const raw of rows) {
    const row = parseJsonObject(raw);
    if (!row) continue;
    const keyword = cleanString(row.keyword, 300);
    if (!keyword) continue;
    out.push({
      keyword,
      cluster: cleanString(row.cluster, 120) || "general",
      recommended_slug: cleanString(row.recommended_slug, 160) || null,
      recommended_page_type: cleanString(row.recommended_page_type, 80) || "service",
      intent: cleanString(row.intent, 40) || "commercial",
      supporting_terms: parseStringArray(row.supporting_terms, 20, 120),
    });
    if (out.length >= 10) break;
  }
  return out;
}

function inferCompetitorSocialProfiles(domain: string): string[] {
  const clean = normalizeRootDomain(domain);
  if (!clean) return [];
  return [
    `https://www.facebook.com/${clean}`,
    `https://www.instagram.com/${clean}`,
    `https://www.youtube.com/@${clean.replace(/\./g, "")}`,
  ];
}

function isoFromMs(ms: number): string {
  return new Date(ms).toISOString();
}

function priorityToDbWeight(priority: Step3TaskPriority): 1 | 2 | 3 | 4 {
  if (priority === "P0") return 1;
  if (priority === "P1") return 2;
  if (priority === "P2") return 3;
  return 4;
}

function modeToExecutionMode(mode: Step3TaskMode): "auto_safe" | "assisted" | "team_only" {
  if (mode === "AUTO") return "auto_safe";
  if (mode === "DIY") return "assisted";
  return "team_only";
}

function statusToDbStatus(status: Step3TaskStatus): "planned" | "applied" | "draft" | "blocked" {
  if (status === "BLOCKED" || status === "FAILED") return "blocked";
  if (status === "DONE") return "applied";
  if (status === "IN_PROGRESS") return "draft";
  return "planned";
}

function makeStep3Task(input: {
  siteId: string;
  siteRunId: string;
  category: Step3TaskCategory;
  type: Step3TaskType;
  title: string;
  summary: string;
  priority: Step3TaskPriority;
  effort: Step3TaskEffort;
  confidence: number;
  estimatedImpact: { seo: "low" | "med" | "high"; leads: "low" | "med" | "high"; timeToEffectDays: number };
  mode: Step3TaskMode;
  requiresAccess?: string[];
  requiresInputs?: string[];
  requiresApprovals?: string[];
  status?: Step3TaskStatus;
  blockers?: Array<{ code: string; message: string }>;
  scope?: Partial<Step3TaskV1["scope"]>;
  evidence?: Step3TaskV1["evidence"];
  instructions?: Step3TaskV1["instructions"];
  automation?: Step3TaskV1["automation"];
  outputs?: Step3TaskV1["outputs"];
  dependencies?: Step3TaskV1["dependencies"];
}): Step3TaskV1 {
  const now = isoFromMs(nowMs());
  const hasAccessNeeds = (input.requiresAccess ?? []).some((v) => v !== "NONE");
  const status = input.status ?? (hasAccessNeeds && input.mode !== "AUTO" ? "BLOCKED" : "READY");
  const blockers =
    input.blockers ??
    (status === "BLOCKED" && hasAccessNeeds
      ? [{ code: "MISSING_ACCESS", message: "Required account access is not connected yet." }]
      : []);

  return {
    schema_version: "task.v1",
    task_id: crypto.randomUUID(),
    site_id: input.siteId,
    site_run_id: input.siteRunId,
    created_at: now,
    updated_at: now,
    category: input.category,
    type: input.type,
    title: input.title,
    summary: input.summary,
    priority: input.priority,
    effort: input.effort,
    confidence: Number(clamp01(input.confidence).toFixed(3)),
    estimated_impact: {
      seo: input.estimatedImpact.seo,
      leads: input.estimatedImpact.leads,
      time_to_effect_days: Math.max(0, Math.round(input.estimatedImpact.timeToEffectDays)),
    },
    mode: input.mode,
    requires: {
      access: (input.requiresAccess ?? ["NONE"]).slice(0, 10),
      inputs: (input.requiresInputs ?? ["NONE"]).slice(0, 10),
      approvals: (input.requiresApprovals ?? ["NONE"]).slice(0, 10),
    },
    status,
    blockers,
    scope: {
      keyword_id: null,
      keyword: input.scope?.keyword ?? null,
      cluster: input.scope?.cluster ?? null,
      target_url: input.scope?.target_url ?? null,
      target_slug: input.scope?.target_slug ?? null,
      geo: input.scope?.geo ?? null,
    },
    evidence: input.evidence ?? {
      based_on: ["BEST_PRACTICE"],
      citations: [
        {
          kind: "BEST_PRACTICE",
          text: "Generated from Step 3 default local-service playbook.",
          data: {},
        },
      ],
    },
    instructions: input.instructions ?? {
      steps: ["Review task context.", "Execute change in controlled rollout.", "Validate outcome."],
      acceptance_criteria: ["Task output created and validated."],
      guardrails: ["Avoid template-level spam patterns."],
    },
    automation: input.automation ?? {
      can_auto_apply: input.mode === "AUTO",
      auto_apply_default: false,
      actions: [],
    },
    outputs: input.outputs ?? {
      artifacts: [],
    },
    dependencies: input.dependencies ?? {
      depends_on_task_ids: [],
      supersedes_task_ids: [],
    },
  };
}

function buildStep3KeywordSignals(report: Record<string, unknown>): Map<string, Step3KeywordSignal> {
  const out = new Map<string, Step3KeywordSignal>();
  const rows = Array.isArray(report.keyword_reports) ? report.keyword_reports : [];
  for (const raw of rows) {
    const row = parseJsonObject(raw);
    if (!row) continue;
    const keyword = cleanString(row.keyword, 300);
    const findings = parseJsonObject(row.findings) ?? {};
    const rankBands = parseJsonObject(findings.rank_bands) ?? {};
    const top3 = parseJsonObject(rankBands.top_3) ?? {};
    const top6_10 = parseJsonObject(rankBands.top_6_10) ?? {};
    const pageDist = parseJsonObject(top3.page_type_distribution) ?? {};
    const normalizedDist: Record<string, number> = {};
    for (const [k, v] of Object.entries(pageDist)) {
      normalizedDist[cleanString(k, 80)] = Math.max(0, Number(v ?? 0) || 0);
    }
    if (!keyword) continue;
    out.set(toKeywordNorm(keyword), {
      keyword,
      top3_faq_schema_rate: clamp01(Number(top3.faq_schema_rate ?? 0) || 0),
      top3_median_inbound_links: Math.max(0, Number(top3.median_internal_inbound_links ?? 0) || 0),
      top3_median_ref_domains: Math.max(0, Number(top3.median_ref_domains ?? 0) || 0),
      top6_10_median_ref_domains: Math.max(0, Number(top6_10.median_ref_domains ?? 0) || 0),
      top3_page_types: normalizedDist,
    });
  }
  return out;
}

async function loadStep3OwnPageSignal(
  env: Env,
  input: { targetUrl: string; targetSlug: string | null; date: string }
): Promise<Step3OwnPageSignal | null> {
  const urlHash = await sha256Hex(cleanString(input.targetUrl, 2000).toLowerCase());
  const extract = await env.DB.prepare(
    `SELECT schema_types_json, pricing_section_present
     FROM step2_page_extracts
     WHERE url_hash = ? AND date_yyyymmdd = ?
     LIMIT 1`
  )
    .bind(urlHash, input.date)
    .first<Record<string, unknown>>();

  const inbound = await env.DB.prepare(
    `SELECT COUNT(*) AS c
     FROM step2_internal_graph_edges
     WHERE date_yyyymmdd = ? AND to_url = ?`
  )
    .bind(input.date, input.targetUrl)
    .first<Record<string, unknown>>();

  const backlinks = await env.DB.prepare(
    `SELECT ref_domains
     FROM step2_url_backlinks
     WHERE url_hash = ? AND date_yyyymmdd = ?
     LIMIT 1`
  )
    .bind(urlHash, input.date)
    .first<Record<string, unknown>>();

  if (!extract && !inbound && !backlinks) return null;
  const schemaParsed = safeJsonParseObject(`{"a":${cleanString(extract?.schema_types_json, 4000) || "[]"}}`);
  const schemaTypes = parseStringArray(schemaParsed?.a, 50, 80).map((v) => v.toLowerCase());
  return {
    target_url: input.targetUrl,
    target_slug: input.targetSlug,
    inbound_internal_links: clampInt(inbound?.c, 0, 1_000_000, 0),
    ref_domains: clampInt(backlinks?.ref_domains, 0, 1_000_000, 0),
    has_faq_schema: schemaTypes.some((v) => v.includes("faq")),
    has_pricing_module: clampInt(extract?.pricing_section_present, 0, 1, 0) === 1,
  };
}

async function loadStep3Top3ModuleRates(
  env: Env,
  input: { siteId: string; date: string; keyword: string }
): Promise<{ faq_rate: number; pricing_rate: number }> {
  const row = await env.DB.prepare(
    `SELECT
      AVG(CAST(pe.faq_section_present AS REAL)) AS faq_rate,
      AVG(CAST(pe.pricing_section_present AS REAL)) AS pricing_rate
     FROM step2_serp_results r
     JOIN step2_serp_snapshots s ON s.serp_id = r.serp_id
     JOIN step2_page_extracts pe ON pe.url_hash = r.url_hash AND pe.date_yyyymmdd = s.date_yyyymmdd
     WHERE s.site_id = ? AND s.date_yyyymmdd = ? AND s.keyword = ? AND r.rank <= 3`
  )
    .bind(input.siteId, input.date, input.keyword)
    .first<Record<string, unknown>>();
  return {
    faq_rate: clamp01(Number(row?.faq_rate ?? 0) || 0),
    pricing_rate: clamp01(Number(row?.pricing_rate ?? 0) || 0),
  };
}

async function loadStep3DiffSignals(
  env: Env,
  input: { siteId: string; date: string }
): Promise<Map<string, Step3DiffSignal>> {
  const rows = await env.DB.prepare(
    `SELECT keyword, entered_urls_json
     FROM step2_serp_diffs
     WHERE site_id = ? AND date_yyyymmdd = ?`
  )
    .bind(input.siteId, input.date)
    .all<Record<string, unknown>>();
  const out = new Map<string, Step3DiffSignal>();
  for (const raw of rows.results ?? []) {
    const keyword = cleanString(raw.keyword, 300);
    if (!keyword) continue;
    const parsed = safeJsonParseObject(`{"a":${cleanString(raw.entered_urls_json, 64000) || "[]"}}`) ?? {};
    const entrants = Array.isArray(parsed.a) ? parsed.a : [];
    let newTop3 = false;
    for (const item of entrants) {
      const row = parseJsonObject(item);
      if (!row) continue;
      const rank = clampInt(row.rank ?? row.position, 1, 1000, 999);
      if (rank <= 3) {
        newTop3 = true;
        break;
      }
    }
    out.set(toKeywordNorm(keyword), {
      keyword,
      new_top3_entrant: newTop3,
    });
  }
  return out;
}

async function loadStep3PreviousSocialCadence(env: Env, siteId: string): Promise<number> {
  const row = await env.DB.prepare(
    `SELECT AVG(cadence_per_week) AS avg_cadence
     FROM step3_social_signals
     WHERE site_id = ?
       AND created_at < ?
       AND cadence_per_week IS NOT NULL`
  )
    .bind(siteId, nowMs())
    .first<Record<string, unknown>>();
  return Math.max(0, Number(row?.avg_cadence ?? 0) || 0);
}

function parseMozDomainTotal(totalsJsonRaw: string, fallbackRows: number): number {
  const totals = safeJsonParseObject(cleanString(totalsJsonRaw, 16000)) ?? {};
  const candidates = [
    totals.total_domains,
    totals.total_root_domains,
    totals.total_rows,
    totals.total,
    totals.rows,
  ];
  for (const value of candidates) {
    const n = Number(value ?? NaN);
    if (Number.isFinite(n) && n >= 0) {
      return Math.max(0, Math.round(n));
    }
  }
  return Math.max(0, fallbackRows);
}

async function loadStep3MozKeywordSignal(
  env: Env,
  input: { siteId: string; date: string; keyword: string; targetUrl: string | null }
): Promise<Step3MozKeywordSignal | null> {
  const topRows = await env.DB.prepare(
    `SELECT
      m.page_authority AS pa,
      m.domain_authority AS da,
      m.spam_score AS spam,
      (
        SELECT lr.total_domain_rows
        FROM moz_linking_root_domains_snapshots lr
        WHERE lr.target_url_id = u.id
          AND lr.collected_day = s.date_yyyymmdd
        ORDER BY lr.created_at DESC
        LIMIT 1
      ) AS moz_rd_rows,
      (
        SELECT lr.totals_json
        FROM moz_linking_root_domains_snapshots lr
        WHERE lr.target_url_id = u.id
          AND lr.collected_day = s.date_yyyymmdd
        ORDER BY lr.created_at DESC
        LIMIT 1
      ) AS moz_rd_totals_json
     FROM step2_serp_results r
     JOIN step2_serp_snapshots s ON s.serp_id = r.serp_id
     LEFT JOIN urls u ON u.url_hash = r.url_hash
     LEFT JOIN moz_url_metrics_snapshots m
       ON m.url_id = u.id
      AND m.collected_day = s.date_yyyymmdd
     WHERE s.site_id = ?
       AND s.date_yyyymmdd = ?
       AND s.keyword = ?
       AND r.rank <= 3`
  )
    .bind(input.siteId, input.date, input.keyword)
    .all<Record<string, unknown>>();

  const paValues: number[] = [];
  const daValues: number[] = [];
  const spamValues: number[] = [];
  const rdValues: number[] = [];
  for (const raw of topRows.results ?? []) {
    const pa = Number(raw.pa ?? NaN);
    const da = Number(raw.da ?? NaN);
    const spam = Number(raw.spam ?? NaN);
    if (Number.isFinite(pa) && pa >= 0) paValues.push(pa);
    if (Number.isFinite(da) && da >= 0) daValues.push(da);
    if (Number.isFinite(spam) && spam >= 0) spamValues.push(spam);
    const rdRows = clampInt(raw.moz_rd_rows, 0, 1_000_000_000, 0);
    const rdTotal = parseMozDomainTotal(cleanString(raw.moz_rd_totals_json, 16000), rdRows);
    if (rdTotal > 0) rdValues.push(rdTotal);
  }

  let ownPa: number | null = null;
  let ownDa: number | null = null;
  let ownSpam: number | null = null;
  let ownRefDomains: number | null = null;
  if (input.targetUrl) {
    const targetHash = await sha256Hex(cleanString(input.targetUrl, 2000).toLowerCase());
    const own = await env.DB.prepare(
      `SELECT
        m.page_authority AS pa,
        m.domain_authority AS da,
        m.spam_score AS spam,
        (
          SELECT lr.total_domain_rows
          FROM moz_linking_root_domains_snapshots lr
          WHERE lr.target_url_id = u.id
            AND lr.collected_day = ?
          ORDER BY lr.created_at DESC
          LIMIT 1
        ) AS moz_rd_rows,
        (
          SELECT lr.totals_json
          FROM moz_linking_root_domains_snapshots lr
          WHERE lr.target_url_id = u.id
            AND lr.collected_day = ?
          ORDER BY lr.created_at DESC
          LIMIT 1
        ) AS moz_rd_totals_json
       FROM urls u
       LEFT JOIN moz_url_metrics_snapshots m
         ON m.url_id = u.id
        AND m.collected_day = ?
       WHERE u.url_hash = ?
       ORDER BY m.created_at DESC
       LIMIT 1`
    )
      .bind(input.date, input.date, input.date, targetHash)
      .first<Record<string, unknown>>();
    if (own) {
      const pa = Number(own.pa ?? NaN);
      const da = Number(own.da ?? NaN);
      const spam = Number(own.spam ?? NaN);
      if (Number.isFinite(pa) && pa >= 0) ownPa = pa;
      if (Number.isFinite(da) && da >= 0) ownDa = da;
      if (Number.isFinite(spam) && spam >= 0) ownSpam = spam;
      ownRefDomains = parseMozDomainTotal(
        cleanString(own.moz_rd_totals_json, 16000),
        clampInt(own.moz_rd_rows, 0, 1_000_000_000, 0)
      );
    }
  }

  const available = paValues.length > 0 || daValues.length > 0 || spamValues.length > 0 || rdValues.length > 0;
  if (!available && ownPa == null && ownDa == null && ownSpam == null && ownRefDomains == null) {
    return null;
  }
  return {
    keyword: input.keyword,
    top3_median_pa: median(paValues),
    top3_median_da: median(daValues),
    top3_median_spam: median(spamValues),
    own_pa: ownPa,
    own_da: ownDa,
    own_spam: ownSpam,
    top3_median_ref_domains: median(rdValues),
    own_ref_domains: ownRefDomains,
    available,
  };
}

function buildStep3Tasks(input: {
  site: Step1SiteRecord;
  siteRunId: string;
  primaryRows: Array<Record<string, unknown>>;
  competitors: Step3CompetitorCandidate[];
  keywordSignals: Map<string, Step3KeywordSignal>;
  ownSignals: Map<string, Step3OwnPageSignal>;
  mozSignals: Map<string, Step3MozKeywordSignal>;
  top3Modules: Map<string, { faq_rate: number; pricing_rate: number }>;
  diffSignals: Map<string, Step3DiffSignal>;
  socialCadenceIncreased: boolean;
  step2Date: string;
}): Step3TaskV1[] {
  const tasks: Step3TaskV1[] = [];
  const created = new Set<string>();
  const add = (task: Step3TaskV1) => {
    const key = `${task.category}:${task.type}:${task.scope.keyword ?? ""}:${task.scope.cluster ?? ""}:${task.scope.target_slug ?? ""}`;
    if (created.has(key)) return;
    created.add(key);
    tasks.push(task);
  };

  const primaryRows = input.primaryRows.slice(0, 6);
  for (const row of primaryRows) {
    const keyword = cleanString(row.keyword, 300);
    if (!keyword) continue;
    const cluster = cleanString(row.cluster, 120) || "general";
    const slug = cleanString(row.recommended_slug, 160) || null;
    const targetUrl =
      slug != null
        ? `${cleanString(input.site.site_url, 2000).replace(/\/+$/, "")}/${slug.replace(/^\/+/, "")}`
        : null;
    const signal = input.keywordSignals.get(toKeywordNorm(keyword));
    const mozSignal = input.mozSignals.get(toKeywordNorm(keyword));
    const ownSignal = targetUrl ? input.ownSignals.get(toKeywordNorm(keyword)) ?? null : null;
    const top3Modules = input.top3Modules.get(toKeywordNorm(keyword)) ?? { faq_rate: 0, pricing_rate: 0 };
    const diffSignal = input.diffSignals.get(toKeywordNorm(keyword));

    add(
      makeStep3Task({
        siteId: input.site.site_id,
        siteRunId: input.siteRunId,
        category: "ON_PAGE",
        type: cleanString(row.recommended_page_type, 80).toLowerCase().includes("location")
          ? "LOCATION_PAGE_UPGRADE"
          : "SERVICE_PAGE_UPGRADE",
        title: `Upgrade primary page for "${keyword}"`,
        summary: "Align money page modules, trust blocks, and schema to local top-rank patterns.",
        priority: "P1",
        effort: "M",
        confidence: 0.83,
        estimatedImpact: { seo: "high", leads: "med", timeToEffectDays: 14 },
        mode: "AUTO",
        requiresAccess: ["WP_ADMIN"],
        scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
        evidence: {
          based_on: ["SERP", "ON_PAGE"],
          citations: [
            {
              kind: "SERP_PATTERN",
              text: "Top local results consistently include service-area, trust, and conversion modules.",
              data: { source_step2_date: input.step2Date, keyword },
            },
          ],
        },
        instructions: {
          steps: [
            "Update service/location page module structure.",
            "Add trust and conversion modules near the fold.",
            "Validate content uniqueness for local modifiers.",
          ],
          acceptance_criteria: [
            "Page includes pricing/reviews/FAQ/service-area blocks as applicable.",
            "No thin or duplicated location-page footprint.",
          ],
          guardrails: ["No doorway-style near-duplicate pages.", "No template-level keyword stuffing."],
        },
        automation: {
          can_auto_apply: true,
          auto_apply_default: false,
          actions: [
            {
              action_type: "WP_UPDATE_PAGE",
              payload: { target_slug: slug, modules: ["pricing", "reviews", "faq", "service_areas", "cta"] },
            },
          ],
        },
        outputs: {
          artifacts: [{ kind: "DRAFT_HTML", data: { target_slug: slug } }],
        },
      })
    );

    if ((signal?.top3_faq_schema_rate ?? 0) >= 0.5 && !(ownSignal?.has_faq_schema ?? false)) {
      add(
        makeStep3Task({
          siteId: input.site.site_id,
          siteRunId: input.siteRunId,
          category: "ON_PAGE",
          type: "FAQ_SCHEMA_ADD",
          title: `Add FAQ schema for "${keyword}" target page`,
          summary: "Top 3 FAQ schema prevalence is high and the target page is missing it.",
          priority: "P1",
          effort: "S",
          confidence: 0.86,
          estimatedImpact: { seo: "med", leads: "low", timeToEffectDays: 7 },
          mode: "AUTO",
          requiresAccess: ["WP_ADMIN"],
          scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
          evidence: {
            based_on: ["SERP", "ON_PAGE"],
            citations: [
              {
                kind: "METRIC_GAP",
                text: "FAQ schema prevalence in top 3 exceeds threshold while target page is missing FAQ schema.",
                data: {
                  top3_faq_schema_rate: signal?.top3_faq_schema_rate ?? 0,
                  target_has_faq_schema: ownSignal?.has_faq_schema ?? false,
                },
              },
            ],
          },
          instructions: {
            steps: [
              "Add a concise FAQ section mapped to search intent.",
              "Attach valid FAQPage schema JSON-LD.",
              "Validate schema syntax and indexability.",
            ],
            acceptance_criteria: ["FAQ section published.", "FAQ schema validates without errors."],
            guardrails: ["No fabricated claims in FAQ answers.", "Keep FAQ content original."],
          },
          automation: {
            can_auto_apply: true,
            auto_apply_default: false,
            actions: [
              { action_type: "WP_ADD_SCHEMA", payload: { schema_type: "FAQPage", target_slug: slug } },
              { action_type: "WP_UPDATE_PAGE", payload: { target_slug: slug, add_section: "faq" } },
            ],
          },
          outputs: {
            artifacts: [{ kind: "SCHEMA_JSONLD", data: { schema_type: "FAQPage", target_slug: slug } }],
          },
        })
      );
    }

    const top3Inbound = signal?.top3_median_inbound_links ?? 0;
    const ownInbound = ownSignal?.inbound_internal_links ?? 0;
    if (top3Inbound > 0 && ownInbound < top3Inbound * 0.5) {
      add(
        makeStep3Task({
          siteId: input.site.site_id,
          siteRunId: input.siteRunId,
          category: "ON_PAGE",
          type: "INTERNAL_LINKING_BOOST_MONEY_PAGE",
          title: `Boost internal links to "${keyword}" money page`,
          summary: "Internal inbound links are materially below top-3 median for this keyword.",
          priority: "P1",
          effort: "M",
          confidence: 0.84,
          estimatedImpact: { seo: "high", leads: "med", timeToEffectDays: 14 },
          mode: "AUTO",
          requiresAccess: ["WP_ADMIN"],
          scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
          evidence: {
            based_on: ["SERP", "ON_PAGE"],
            citations: [
              {
                kind: "METRIC_GAP",
                text: "Target internal inbound links are below 50% of top-3 median.",
                data: { top3_median_inbound_links: top3Inbound, target_inbound_links: ownInbound },
              },
            ],
          },
          instructions: {
            steps: [
              "Select relevant supporting pages in same cluster.",
              "Insert contextual links with diversified anchor themes.",
              "Avoid sitewide footer/sidebar link blasts.",
            ],
            acceptance_criteria: [
              `Increase inbound links toward ${Math.max(10, Math.round(top3Inbound * 0.8))}.`,
              "No anchor text exceeds 30% share.",
            ],
            guardrails: ["Only contextually relevant placements.", "No repetitive exact-match pattern."],
          },
          automation: {
            can_auto_apply: true,
            auto_apply_default: true,
            actions: [
              {
                action_type: "WP_ADD_INTERNAL_LINKS",
                payload: {
                  target_slug: slug,
                  min_new_links: Math.max(10, Math.round(top3Inbound * 0.8) - ownInbound),
                  anchor_themes: parseStringArray(row.supporting_terms, 8, 80),
                },
              },
            ],
          },
          outputs: {
            artifacts: [
              {
                kind: "LINK_PLAN",
                data: {
                  target_slug: slug,
                  target_links: Math.max(10, Math.round(top3Inbound * 0.8)),
                },
              },
            ],
          },
        })
      );
    }

    if (
      (top3Modules.pricing_rate >= 0.5 ||
        ((signal?.top3_page_types.service ?? 0) >= 2 && diffSignal?.new_top3_entrant === true)) &&
      !(ownSignal?.has_pricing_module ?? false)
    ) {
      add(
        makeStep3Task({
          siteId: input.site.site_id,
          siteRunId: input.siteRunId,
          category: "ON_PAGE",
          type: "CONTENT_MODULE_ADD",
          title: `Add pricing module for "${keyword}" page`,
          summary: "Pricing visibility is common in top local pages and missing on target page.",
          priority: "P1",
          effort: "S",
          confidence: 0.78,
          estimatedImpact: { seo: "med", leads: "high", timeToEffectDays: 10 },
          mode: "AUTO",
          requiresAccess: ["WP_ADMIN"],
          scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
          evidence: {
            based_on: ["SERP", "ON_PAGE"],
            citations: [
              {
                kind: diffSignal?.new_top3_entrant ? "COMPETITOR_CHANGE" : "SERP_PATTERN",
                text: diffSignal?.new_top3_entrant
                  ? "A new top-3 competitor entered and pricing modules are common among top pages."
                  : "Top-ranked competitors frequently include pricing-related sections.",
                data: { top3_pricing_rate: top3Modules.pricing_rate, new_top3_entrant: diffSignal?.new_top3_entrant ?? false },
              },
            ],
          },
          instructions: {
            steps: ["Insert pricing or starting-price block.", "Add quote CTA adjacent to pricing block."],
            acceptance_criteria: ["Pricing module rendered and linked to conversion CTA."],
            guardrails: ["No misleading price promises.", "Keep pricing language compliant."],
          },
          automation: {
            can_auto_apply: true,
            auto_apply_default: false,
            actions: [{ action_type: "WP_UPDATE_PAGE", payload: { target_slug: slug, add_module: "pricing" } }],
          },
          outputs: { artifacts: [{ kind: "DRAFT_MD", data: { section: "pricing", target_slug: slug } }] },
        })
      );
    }

    const top3RD = mozSignal?.top3_median_ref_domains || signal?.top3_median_ref_domains || 0;
    const ownRD = mozSignal?.own_ref_domains ?? ownSignal?.ref_domains ?? 0;
    if (top3RD > 0 && ownRD < top3RD * 0.6) {
      add(
        makeStep3Task({
          siteId: input.site.site_id,
          siteRunId: input.siteRunId,
          category: "AUTHORITY",
          type: "OUTREACH_TARGET_LIST",
          title: `Build outreach target list for "${cluster}"`,
          summary: "Ref-domain gap indicates authority deficit against top local performers.",
          priority: "P1",
          effort: "M",
          confidence: 0.66,
          estimatedImpact: { seo: "high", leads: "med", timeToEffectDays: 30 },
          mode: "TEAM",
          requiresAccess: ["NONE"],
          requiresInputs: ["SERVICE_LIST"],
          requiresApprovals: ["OUTREACH_SEND"],
          status: "READY",
          scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
          evidence: {
            based_on: ["LINKS", "SERP"],
            citations: [
              {
                kind: "METRIC_GAP",
                text: mozSignal
                  ? "Moz linking root domains indicate a material ref-domain gap vs top-3 competitors."
                  : "Target referring domains are materially below top-3 median.",
                data: { top3_median_ref_domains: top3RD, target_ref_domains: ownRD, source: mozSignal ? "moz_linking_root_domains_snapshots" : "step2_estimate" },
              },
            ],
          },
          instructions: {
            steps: [
              "Extract local/niche backlink themes from top competitors.",
              "Generate 30 outreach targets with pitch angles.",
              "Prioritize local organizations and relevant partnerships.",
            ],
            acceptance_criteria: [
              "30 vetted targets produced.",
              "At least 10 local/relevant targets.",
            ],
            guardrails: ["No link farms.", "No mass outreach without review."],
          },
          automation: { can_auto_apply: false, auto_apply_default: false, actions: [] },
          outputs: { artifacts: [{ kind: "OUTREACH_LIST", data: { target_count: 30, cluster } }] },
        })
      );
    }

    if (mozSignal && mozSignal.own_da != null && mozSignal.own_pa != null) {
      const daGap = mozSignal.top3_median_da - mozSignal.own_da;
      const paGap = mozSignal.top3_median_pa - mozSignal.own_pa;
      if (daGap >= 12 || paGap >= 12) {
        add(
          makeStep3Task({
            siteId: input.site.site_id,
            siteRunId: input.siteRunId,
            category: "AUTHORITY",
            type: "AUTHORITY_GAP_PLAN",
            title: `Build authority gap plan for "${cluster}"`,
            summary: "Moz authority metrics show the target page/domain trails top-3 competitors.",
            priority: "P1",
            effort: "M",
            confidence: 0.79,
            estimatedImpact: { seo: "high", leads: "med", timeToEffectDays: 21 },
            mode: "TEAM",
            requiresAccess: ["NONE"],
            requiresInputs: ["SERVICE_LIST"],
            requiresApprovals: ["OUTREACH_SEND"],
            scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
            evidence: {
              based_on: ["LINKS"],
              citations: [
                {
                  kind: "METRIC_GAP",
                  text: "Top-3 Moz PA/DA median exceeds owned page authority by threshold.",
                  data: {
                    top3_median_pa: mozSignal.top3_median_pa,
                    own_pa: mozSignal.own_pa,
                    top3_median_da: mozSignal.top3_median_da,
                    own_da: mozSignal.own_da,
                    pa_gap: Number(paGap.toFixed(2)),
                    da_gap: Number(daGap.toFixed(2)),
                  },
                },
              ],
            },
            instructions: {
              steps: [
                "Build a 60-day authority sprint plan tied to this cluster.",
                "Prioritize domains linking to multiple top competitors.",
                "Sequence on-page trust assets before outreach pushes.",
              ],
              acceptance_criteria: [
                "Authority gap plan includes monthly RD goals and anchor mix.",
                "Plan references at least 20 realistic target domains.",
              ],
              guardrails: ["No paid link farms.", "No exact-match anchor over-optimization."],
            },
            outputs: { artifacts: [{ kind: "OUTREACH_LIST", data: { strategy: "authority_gap", cluster } }] },
          })
        );
      }
    }

    if (mozSignal && mozSignal.own_spam != null) {
      const topSpam = mozSignal.top3_median_spam;
      const ownSpam = mozSignal.own_spam;
      if (ownSpam >= 40 && ownSpam >= topSpam + 15 && topSpam <= 30) {
        add(
          makeStep3Task({
            siteId: input.site.site_id,
            siteRunId: input.siteRunId,
            category: "AUTHORITY",
            type: "LINK_RISK_AUDIT",
            title: `Run link risk audit for "${cluster}"`,
            summary: "Moz spam profile is elevated versus top-3 competitors and needs remediation review.",
            priority: "P1",
            effort: "M",
            confidence: 0.74,
            estimatedImpact: { seo: "med", leads: "low", timeToEffectDays: 14 },
            mode: "AUTO",
            requiresAccess: ["NONE"],
            scope: { keyword, cluster, target_slug: slug, target_url: targetUrl, geo: "both" },
            evidence: {
              based_on: ["LINKS"],
              citations: [
                {
                  kind: "METRIC_GAP",
                  text: "Owned spam score materially exceeds competitor baseline.",
                  data: {
                    own_spam_score: ownSpam,
                    top3_median_spam_score: topSpam,
                  },
                },
              ],
            },
            instructions: {
              steps: [
                "Review risky anchor and referring-domain patterns.",
                "Identify cleanup/disavow candidates for manual review.",
                "Shift outreach plan toward higher-trust local domains.",
              ],
              acceptance_criteria: [
                "Risk report produced with prioritized remediation actions.",
                "Future outreach anchor guardrails updated.",
              ],
              guardrails: ["No automated disavow submission without review."],
            },
            outputs: { artifacts: [{ kind: "OUTREACH_LIST", data: { strategy: "link_risk_audit", cluster } }] },
          })
        );
      }
    }
  }

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "ON_PAGE",
      type: "INTERNAL_LINKING_SILO_BUILD",
      title: "Build cluster-wide internal linking silos",
      summary: "Strengthen contextual clusters and money-page flow using Step 2 keyword map.",
      priority: "P1",
      effort: "M",
      confidence: 0.82,
      estimatedImpact: { seo: "high", leads: "med", timeToEffectDays: 14 },
      mode: "AUTO",
      requiresAccess: ["WP_ADMIN"],
      scope: { geo: "both" },
      automation: {
        can_auto_apply: true,
        auto_apply_default: true,
        actions: [{ action_type: "WP_ADD_INTERNAL_LINKS", payload: { strategy: "cluster_silo" } }],
      },
      outputs: { artifacts: [{ kind: "LINK_PLAN", data: { strategy: "cluster_silo" } }] },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "TECHNICAL_SEO",
      type: "INDEXATION_CLEANUP",
      title: "Run indexation and canonical cleanup",
      summary: "Fix indexability drift, canonical conflicts, and thin indexable pages.",
      priority: "P0",
      effort: "M",
      confidence: 0.8,
      estimatedImpact: { seo: "high", leads: "med", timeToEffectDays: 7 },
      mode: "AUTO",
      requiresAccess: ["WP_ADMIN"],
      scope: { geo: "us" },
      automation: {
        can_auto_apply: true,
        auto_apply_default: false,
        actions: [{ action_type: "WP_UPDATE_META", payload: { checks: ["noindex", "canonical"] } }],
      },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "TECHNICAL_SEO",
      type: "DUPLICATE_CONTENT_REDUCE",
      title: "Reduce duplicate content and template footprint",
      summary: "Control builder/template repetition to avoid local doorway-like patterns.",
      priority: "P1",
      effort: "M",
      confidence: 0.77,
      estimatedImpact: { seo: "med", leads: "low", timeToEffectDays: 21 },
      mode: "AUTO",
      requiresAccess: ["WP_ADMIN"],
      scope: { geo: "us" },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "LOCAL_SEO",
      type: "GBP_OPTIMIZE",
      title: "Optimize Google Business Profile categories/services",
      summary: "Align GBP categories, services, and profile details to winning local intent.",
      priority: "P1",
      effort: "M",
      confidence: 0.74,
      estimatedImpact: { seo: "high", leads: "high", timeToEffectDays: 10 },
      mode: "DIY",
      requiresAccess: ["GBP"],
      requiresInputs: ["SERVICE_LIST", "BUSINESS_HOURS"],
      requiresApprovals: ["GBP_PUBLISH"],
      scope: { geo: "both" },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "LOCAL_SEO",
      type: "GBP_POST_PLAN_WEEKLY",
      title: "Generate weekly GBP post plan",
      summary: "Create local-trust and service-theme GBP post drafts for weekly cadence.",
      priority: "P2",
      effort: "S",
      confidence: 0.73,
      estimatedImpact: { seo: "low", leads: "med", timeToEffectDays: 7 },
      mode: "AUTO",
      requiresAccess: ["GBP"],
      requiresApprovals: ["GBP_PUBLISH"],
      scope: { geo: "both" },
      automation: {
        can_auto_apply: false,
        auto_apply_default: false,
        actions: [{ action_type: "GBP_CREATE_POST", payload: { cadence: "weekly", posts: 4 } }],
      },
      outputs: { artifacts: [{ kind: "DRAFT_MD", data: { channel: "GBP", cadence: "weekly" } }] },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "LOCAL_SEO",
      type: "REVIEW_REQUEST_CAMPAIGN_SETUP",
      title: "Set up review request campaign",
      summary: "Deploy review request flow and templates to improve review velocity.",
      priority: "P1",
      effort: "M",
      confidence: 0.75,
      estimatedImpact: { seo: "med", leads: "high", timeToEffectDays: 14 },
      mode: "DIY",
      requiresAccess: ["EMAIL", "SMS"],
      requiresInputs: ["BRAND_VOICE"],
      requiresApprovals: ["PUBLISH"],
      scope: { geo: "both" },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "LOCAL_SEO",
      type: "CITATION_AUDIT",
      title: "Audit citations and NAP consistency",
      summary: "Find NAP mismatches, duplicate profiles, and cleanup opportunities.",
      priority: "P1",
      effort: "S",
      confidence: 0.8,
      estimatedImpact: { seo: "med", leads: "med", timeToEffectDays: 21 },
      mode: "AUTO",
      requiresAccess: ["NONE"],
      scope: { geo: "both" },
      outputs: { artifacts: [{ kind: "CITATION_LIST", data: { mode: "audit" } }] },
    })
  );

  const directoryRatio =
    input.competitors.length > 0
      ? input.competitors.filter((row) => row.is_directory).length / input.competitors.length
      : 0;
  if (directoryRatio >= 0.6) {
    add(
      makeStep3Task({
        siteId: input.site.site_id,
        siteRunId: input.siteRunId,
        category: "AUTHORITY",
        type: "LOCAL_PARTNERSHIP_OPPORTUNITIES",
        title: "Build local partnership opportunities",
        summary: "Directory-heavy SERP indicates stronger local trust and relationship signals are needed.",
        priority: "P1",
        effort: "M",
        confidence: 0.7,
        estimatedImpact: { seo: "med", leads: "med", timeToEffectDays: 30 },
        mode: "TEAM",
        scope: { geo: "both" },
        evidence: {
          based_on: ["SERP"],
          citations: [
            {
              kind: "SERP_PATTERN",
              text: "Directory-heavy SERP composition detected.",
              data: { directory_ratio: Number(directoryRatio.toFixed(3)) },
            },
          ],
        },
        outputs: { artifacts: [{ kind: "OUTREACH_LIST", data: { focus: "local_partnerships" } }] },
      })
    );
  }

  const competitorDomains = input.competitors.map((row) => row.domain).slice(0, 8);
  if (input.socialCadenceIncreased) {
    add(
      makeStep3Task({
        siteId: input.site.site_id,
        siteRunId: input.siteRunId,
        category: "SOCIAL",
        type: "SOCIAL_PLAN_WEEKLY",
        title: "Increase weekly social cadence due to competitor acceleration",
        summary: "Competitor social cadence increased versus previous baseline while owned social signal remains low/unknown.",
        priority: "P2",
        effort: "S",
        confidence: 0.69,
        estimatedImpact: { seo: "low", leads: "med", timeToEffectDays: 7 },
        mode: "DIY",
        requiresAccess: ["SOCIAL"],
        requiresInputs: ["PHOTOS", "SERVICE_LIST", "BRAND_VOICE"],
        requiresApprovals: ["PUBLISH"],
        scope: { geo: "both" },
        evidence: {
          based_on: ["SOCIAL"],
          citations: [
            {
              kind: "COMPETITOR_CHANGE",
              text: "Competitor social cadence is rising compared with prior runs.",
              data: { social_cadence_increased: true },
            },
          ],
        },
      })
    );
  }

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "SOCIAL",
      type: "SOCIAL_PLAN_WEEKLY",
      title: "Generate competitor-reactive weekly social plan",
      summary: "Use competitor cadence/themes as features while generating original creative and copy.",
      priority: "P2",
      effort: "S",
      confidence: 0.72,
      estimatedImpact: { seo: "low", leads: "med", timeToEffectDays: 7 },
      mode: "DIY",
      requiresAccess: ["SOCIAL"],
      requiresInputs: ["PHOTOS", "SERVICE_LIST", "BRAND_VOICE"],
      requiresApprovals: ["PUBLISH"],
      scope: { geo: "both" },
      evidence: {
        based_on: ["SOCIAL", "SERP"],
        citations: [
          {
            kind: "SERP_PATTERN",
            text: "Top competitors in local SERPs show persistent social proof and recurring service themes.",
            data: { competitor_set: competitorDomains },
          },
        ],
      },
      instructions: {
        steps: [
          "Create 5 original posts mapped to service, trust, and differentiator themes.",
          "Include localized proof points and lead-gen CTA in each draft.",
          "Schedule drafts after account connection.",
        ],
        acceptance_criteria: ["5 post drafts + creative briefs are generated.", "No competitor copy reuse."],
        guardrails: ["No competitor phrasing reuse.", "No image reuse.", "No keyword stuffing."],
      },
      automation: { can_auto_apply: false, auto_apply_default: false, actions: [] },
      outputs: {
        artifacts: [{ kind: "SOCIAL_CALENDAR", data: { cadence: "weekly", competitors: competitorDomains } }],
      },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "SOCIAL",
      type: "SOCIAL_CREATIVE_BRIEF",
      title: "Generate social creative briefs",
      summary: "Produce hooks, captions, CTA patterns, and visual direction per weekly post.",
      priority: "P2",
      effort: "S",
      confidence: 0.71,
      estimatedImpact: { seo: "low", leads: "med", timeToEffectDays: 7 },
      mode: "AUTO",
      requiresAccess: ["NONE"],
      outputs: { artifacts: [{ kind: "SOCIAL_CALENDAR", data: { include_briefs: true } }] },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "MEASUREMENT",
      type: "KPI_DASHBOARD_UPDATE",
      title: "Update SEO + lead KPI dashboard",
      summary: "Consolidate rank, SERP-change, and lead proxy metrics for weekly decisions.",
      priority: "P2",
      effort: "S",
      confidence: 0.79,
      estimatedImpact: { seo: "med", leads: "med", timeToEffectDays: 7 },
      mode: "AUTO",
      requiresAccess: ["NONE"],
      outputs: { artifacts: [{ kind: "EXPORT_CSV", data: { scope: "weekly_kpis" } }] },
    })
  );

  add(
    makeStep3Task({
      siteId: input.site.site_id,
      siteRunId: input.siteRunId,
      category: "MEASUREMENT",
      type: "ALERT_RULE_CREATE",
      title: "Create alert rules for rank drops and competitor spikes",
      summary: "Notify when rank volatility or new top-3 competitors signal potential revenue risk.",
      priority: "P1",
      effort: "S",
      confidence: 0.82,
      estimatedImpact: { seo: "med", leads: "med", timeToEffectDays: 1 },
      mode: "AUTO",
      requiresAccess: ["NONE"],
      outputs: { artifacts: [{ kind: "DRAFT_MD", data: { alert_rules: ["rank_drop", "new_top3_competitor"] } }] },
    })
  );

  return tasks.slice(0, 80);
}

function buildStep3RiskFlags(input: {
  site: Step1SiteRecord;
  primaryRows: Array<Record<string, unknown>>;
  competitors: Step3CompetitorCandidate[];
  tasks: Step3TaskV1[];
}): Step3RiskFlag[] {
  const flags: Step3RiskFlag[] = [];
  const locationPages = input.primaryRows.filter((row) =>
    cleanString(row.recommended_page_type, 80).toLowerCase().includes("location")
  ).length;
  if (locationPages >= 8) {
    flags.push({
      risk_type: "doorway_pattern",
      severity: "high",
      message: "High volume of location-landing targets detected. Enforce uniqueness and avoid doorway-page expansion.",
      blocked_action: "bulk_location_page_autopublish",
    });
  }

  const uniqueClusters = new Set(input.primaryRows.map((row) => cleanString(row.cluster, 120).toLowerCase()));
  if (uniqueClusters.size < 3) {
    flags.push({
      risk_type: "cluster_concentration",
      severity: "medium",
      message: "Primary keyword set is tightly concentrated. Expand coverage to reduce repetitive footprint.",
      blocked_action: "single-cluster_overproduction",
    });
  }

  const directoryCount = input.competitors.filter((row) => row.is_directory).length;
  if (input.competitors.length > 0 && directoryCount / input.competitors.length >= 0.6) {
    flags.push({
      risk_type: "directory_heavy_serp",
      severity: "low",
      message: "SERPs are directory-heavy; prioritize brand/local trust signals rather than thin location expansion.",
      blocked_action: null,
    });
  }

  const teamOnlyCount = input.tasks.filter((task) => task.mode === "TEAM").length;
  if (teamOnlyCount >= 4) {
    flags.push({
      risk_type: "execution_dependency",
      severity: "medium",
      message: "Plan includes multiple team-only tasks. Ensure assisted capacity before promising timelines.",
      blocked_action: null,
    });
  }

  return flags;
}

async function loadLatestStep2DateForSite(env: Env, siteId: string): Promise<string | null> {
  const row = await env.DB.prepare(
    `SELECT date_yyyymmdd
     FROM step2_daily_reports
     WHERE site_id = ?
     ORDER BY date_yyyymmdd DESC, created_at DESC
     LIMIT 1`
  )
    .bind(siteId)
    .first<Record<string, unknown>>();
  return row ? cleanString(row.date_yyyymmdd, 20) || null : null;
}

async function loadStep3CompetitorCandidatesFromStep2(
  env: Env,
  siteId: string,
  step2Date: string,
  limit = 24
): Promise<Step3CompetitorCandidate[]> {
  const rows = await env.DB.prepare(
    `SELECT
      r.domain AS domain,
      COUNT(*) AS appearances,
      AVG(r.rank) AS avg_rank,
      GROUP_CONCAT(r.url) AS sample_urls_csv
    FROM step2_serp_results r
    JOIN step2_serp_snapshots s ON s.serp_id = r.serp_id
    WHERE s.site_id = ?
      AND s.date_yyyymmdd = ?
      AND r.rank <= 5
    GROUP BY r.domain
    ORDER BY appearances DESC, avg_rank ASC
    LIMIT ?`
  )
    .bind(siteId, step2Date, Math.max(1, Math.min(limit, 100)))
    .all<Record<string, unknown>>();

  const out: Step3CompetitorCandidate[] = [];
  for (const raw of rows.results ?? []) {
    const domain = normalizeRootDomain(cleanString(raw.domain, 255));
    if (!domain) continue;
    const sampleUrls = cleanString(raw.sample_urls_csv, 40_000)
      .split(",")
      .map((v) => cleanString(v, 2000))
      .filter(Boolean)
      .slice(0, 10);
    out.push({
      domain,
      appearance_count: clampInt(raw.appearances, 0, 5000, 0),
      avg_rank: Math.max(0, Number(raw.avg_rank ?? 0) || 0),
      sample_urls: sampleUrls,
      is_directory: isDirectoryDomain(domain),
    });
  }
  return out;
}

async function loadLatestStep3Report(env: Env, siteId: string, dateFilter: string): Promise<Record<string, unknown>> {
  const row = await env.DB.prepare(
    `SELECT
      r.run_id,
      r.date_yyyymmdd,
      r.report_json,
      r.created_at,
      sr.status,
      sr.source_step2_date,
      sr.summary_json
     FROM step3_reports r
     JOIN step3_runs sr ON sr.run_id = r.run_id
     WHERE r.site_id = ?
       AND (? = '' OR r.date_yyyymmdd = ?)
     ORDER BY r.created_at DESC
     LIMIT 1`
  )
    .bind(siteId, dateFilter, dateFilter)
    .first<Record<string, unknown>>();

  if (!row) {
    return {
      site_id: siteId,
      date: dateFilter || null,
      report: null,
    };
  }

  return {
    site_id: siteId,
    date: cleanString(row.date_yyyymmdd, 20),
    run_id: cleanString(row.run_id, 120),
    status: cleanString(row.status, 20),
    source_step2_date: cleanString(row.source_step2_date, 20) || null,
    summary: safeJsonParseObject(cleanString(row.summary_json, 64_000)) ?? {},
    report: safeJsonParseObject(cleanString(row.report_json, 512_000)) ?? {},
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function loadStep3RunMeta(
  env: Env,
  input: { siteId: string; runId?: string | null }
): Promise<{ run_id: string; date_yyyymmdd: string; created_at: number } | null> {
  if (cleanString(input.runId, 120)) {
    const row = await env.DB.prepare(
      `SELECT run_id, date_yyyymmdd, created_at
       FROM step3_runs
       WHERE site_id = ? AND run_id = ?
       LIMIT 1`
    )
      .bind(input.siteId, cleanString(input.runId, 120))
      .first<Record<string, unknown>>();
    if (!row) return null;
    return {
      run_id: cleanString(row.run_id, 120),
      date_yyyymmdd: cleanString(row.date_yyyymmdd, 20),
      created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    };
  }
  const latest = await env.DB.prepare(
    `SELECT run_id, date_yyyymmdd, created_at
     FROM step3_runs
     WHERE site_id = ?
     ORDER BY created_at DESC
     LIMIT 1`
  )
    .bind(input.siteId)
    .first<Record<string, unknown>>();
  if (!latest) return null;
  return {
    run_id: cleanString(latest.run_id, 120),
    date_yyyymmdd: cleanString(latest.date_yyyymmdd, 20),
    created_at: clampInt(latest.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function loadStep3TasksForRun(
  env: Env,
  runId: string
): Promise<Array<{ task_id: string; details_json: string; status: string; created_at: number }>> {
  const rows = await env.DB.prepare(
    `SELECT task_id, details_json, status, created_at
     FROM step3_tasks
     WHERE run_id = ?
     ORDER BY priority ASC, created_at ASC`
  )
    .bind(runId)
    .all<Record<string, unknown>>();
  return (rows.results ?? []).map((row) => ({
    task_id: cleanString(row.task_id, 120),
    details_json: cleanString(row.details_json, 128000),
    status: cleanString(row.status, 20),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  }));
}

async function loadStep3TasksFiltered(
  env: Env,
  input: {
    siteId: string;
    runId: string;
    executionModes: string[];
    taskGroups: string[];
    statuses: string[];
  }
): Promise<Array<{ task_id: string; details_json: string; status: string; created_at: number }>> {
  const where: string[] = ["site_id = ?", "run_id = ?"];
  const binds: Array<string | number> = [input.siteId, input.runId];

  if (input.executionModes.length > 0) {
    where.push(`execution_mode IN (${input.executionModes.map(() => "?").join(",")})`);
    binds.push(...input.executionModes);
  }
  if (input.taskGroups.length > 0) {
    where.push(`task_group IN (${input.taskGroups.map(() => "?").join(",")})`);
    binds.push(...input.taskGroups);
  }
  if (input.statuses.length > 0) {
    where.push(`status IN (${input.statuses.map(() => "?").join(",")})`);
    binds.push(...input.statuses);
  }

  const query = `
    SELECT task_id, details_json, status, created_at
    FROM step3_tasks
    WHERE ${where.join(" AND ")}
    ORDER BY priority ASC, created_at ASC
  `;
  const rows = await env.DB.prepare(query).bind(...binds).all<Record<string, unknown>>();
  return (rows.results ?? []).map((row) => ({
    task_id: cleanString(row.task_id, 120),
    details_json: cleanString(row.details_json, 128000),
    status: cleanString(row.status, 20),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  }));
}

function mapDbTaskStatusToV1(status: string): Step3TaskStatus {
  const s = cleanString(status, 20).toLowerCase();
  if (s === "blocked") return "BLOCKED";
  if (s === "applied") return "DONE";
  if (s === "draft") return "IN_PROGRESS";
  return "READY";
}

function mapQueryStatusToDb(status: string): string | null {
  const s = cleanString(status, 30).toUpperCase();
  if (!s) return null;
  if (s === "NEW" || s === "READY") return "planned";
  if (s === "BLOCKED" || s === "FAILED") return "blocked";
  if (s === "IN_PROGRESS") return "draft";
  if (s === "DONE") return "applied";
  if (s === "SKIPPED") return "planned";
  const direct = cleanString(status, 30).toLowerCase();
  if (["planned", "blocked", "draft", "applied"].includes(direct)) return direct;
  return null;
}

function mapQueryExecutionMode(mode: string): string | null {
  const m = cleanString(mode, 30).toUpperCase();
  if (!m) return null;
  if (m === "AUTO" || m === "AUTO_SAFE") return "auto_safe";
  if (m === "DIY" || m === "ASSISTED") return "assisted";
  if (m === "TEAM" || m === "TEAM_ONLY") return "team_only";
  const direct = cleanString(mode, 30).toLowerCase();
  if (["auto_safe", "assisted", "team_only"].includes(direct)) return direct;
  return null;
}

function parseCsvQueryValues(url: URL, key: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const push = (value: string) => {
    const clean = cleanString(value, 80);
    if (!clean) return;
    const lowered = clean.toLowerCase();
    if (seen.has(lowered)) return;
    seen.add(lowered);
    out.push(clean);
  };
  for (const v of url.searchParams.getAll(key)) {
    for (const part of v.split(",")) {
      push(part);
    }
  }
  return out;
}

function mapTaskToCard(task: Step3TaskV1): Record<string, unknown> {
  return {
    task_id: task.task_id,
    title: task.title,
    category: task.category,
    type: task.type,
    priority: task.priority,
    mode: task.mode,
    effort: task.effort,
    confidence: task.confidence,
    impact: task.estimated_impact,
    scope: {
      cluster: task.scope.cluster,
      keyword: task.scope.keyword,
      target_slug: task.scope.target_slug,
      geo: task.scope.geo,
    },
    requires_access: task.requires.access,
    status: task.status,
    blocker_codes: task.blockers.map((row) => cleanString(row.code, 60)).filter(Boolean),
    updated_at: task.updated_at,
  };
}

function parseTaskV1FromStored(
  row: { task_id: string; details_json: string; status: string; created_at: number },
  siteId: string,
  runId: string
): Step3TaskV1 {
  const parsed = safeJsonParseObject(row.details_json) ?? {};
  const task = parsed as unknown as Partial<Step3TaskV1>;
  const createdIso = isoFromMs(row.created_at);
  return {
    schema_version: "task.v1",
    task_id: cleanString(task.task_id, 120) || row.task_id,
    site_id: cleanString(task.site_id, 120) || siteId,
    site_run_id: cleanString(task.site_run_id, 120) || runId,
    created_at: cleanString(task.created_at, 40) || createdIso,
    updated_at: cleanString(task.updated_at, 40) || createdIso,
    category: (cleanString(task.category, 40) as Step3TaskCategory) || "ON_PAGE",
    type: (cleanString(task.type, 80) as Step3TaskType) || "CONTENT_REFRESH",
    title: cleanString(task.title, 300) || "Untitled task",
    summary: cleanString(task.summary, 2000) || "",
    priority: (cleanString(task.priority, 4) as Step3TaskPriority) || "P2",
    effort: (cleanString(task.effort, 1) as Step3TaskEffort) || "M",
    confidence: clamp01(Number(task.confidence ?? 0.5) || 0.5),
    estimated_impact: {
      seo: (cleanString(task.estimated_impact?.seo, 8) as "low" | "med" | "high") || "med",
      leads: (cleanString(task.estimated_impact?.leads, 8) as "low" | "med" | "high") || "med",
      time_to_effect_days: clampInt(task.estimated_impact?.time_to_effect_days, 0, 3650, 14),
    },
    mode: (cleanString(task.mode, 8) as Step3TaskMode) || "AUTO",
    requires: {
      access: parseStringArray(task.requires?.access, 20, 30).length > 0 ? parseStringArray(task.requires?.access, 20, 30) : ["NONE"],
      inputs: parseStringArray(task.requires?.inputs, 20, 40).length > 0 ? parseStringArray(task.requires?.inputs, 20, 40) : ["NONE"],
      approvals:
        parseStringArray(task.requires?.approvals, 20, 40).length > 0
          ? parseStringArray(task.requires?.approvals, 20, 40)
          : ["NONE"],
    },
    status: (cleanString(task.status, 20) as Step3TaskStatus) || mapDbTaskStatusToV1(row.status),
    blockers: Array.isArray(task.blockers)
      ? task.blockers
          .map((raw) => {
            const b = parseJsonObject(raw);
            if (!b) return null;
            const code = cleanString(b.code, 60);
            const message = cleanString(b.message, 300);
            if (!code && !message) return null;
            return { code: code || "MISSING_INPUT", message: message || "Missing requirement." };
          })
          .filter((v): v is { code: string; message: string } => !!v)
      : [],
    scope: {
      keyword_id: cleanString(task.scope?.keyword_id, 120) || null,
      keyword: cleanString(task.scope?.keyword, 300) || null,
      cluster: cleanString(task.scope?.cluster, 120) || null,
      target_url: cleanString(task.scope?.target_url, 2000) || null,
      target_slug: cleanString(task.scope?.target_slug, 160) || null,
      geo: cleanString(task.scope?.geo, 120) || null,
    },
    evidence: {
      based_on: parseStringArray(task.evidence?.based_on, 20, 40),
      citations: Array.isArray(task.evidence?.citations)
        ? task.evidence.citations
            .map((raw) => {
              const c = parseJsonObject(raw);
              if (!c) return null;
              return {
                kind: cleanString(c.kind, 80) || "BEST_PRACTICE",
                text: cleanString(c.text, 500),
                data: parseJsonObject(c.data) ?? {},
              };
            })
            .filter((v): v is { kind: string; text: string; data: Record<string, unknown> } => !!v)
        : [],
    },
    instructions: {
      steps: parseStringArray(task.instructions?.steps, 30, 300),
      acceptance_criteria: parseStringArray(task.instructions?.acceptance_criteria, 20, 300),
      guardrails: parseStringArray(task.instructions?.guardrails, 20, 300),
    },
    automation: {
      can_auto_apply: parseBoolUnknown(task.automation?.can_auto_apply, false),
      auto_apply_default: parseBoolUnknown(task.automation?.auto_apply_default, false),
      actions: Array.isArray(task.automation?.actions)
        ? task.automation.actions
            .map((raw) => {
              const a = parseJsonObject(raw);
              if (!a) return null;
              return {
                action_type: cleanString(a.action_type, 80),
                payload: parseJsonObject(a.payload) ?? {},
              };
            })
            .filter((v): v is { action_type: string; payload: Record<string, unknown> } => !!v && !!v.action_type)
        : [],
    },
    outputs: {
      artifacts: Array.isArray(task.outputs?.artifacts)
        ? task.outputs.artifacts
            .map((raw) => {
              const a = parseJsonObject(raw);
              if (!a) return null;
              return {
                kind: cleanString(a.kind, 80),
                data: parseJsonObject(a.data) ?? {},
              };
            })
            .filter((v): v is { kind: string; data: Record<string, unknown> } => !!v && !!v.kind)
        : [],
    },
    dependencies: {
      depends_on_task_ids: parseStringArray(task.dependencies?.depends_on_task_ids, 50, 120),
      supersedes_task_ids: parseStringArray(task.dependencies?.supersedes_task_ids, 50, 120),
    },
  };
}

function priorityRank(priority: Step3TaskPriority): number {
  if (priority === "P0") return 0;
  if (priority === "P1") return 1;
  if (priority === "P2") return 2;
  return 3;
}

function buildStep3TaskBoardPayload(input: {
  site: Step1SiteRecord;
  runId: string | null;
  runDate: string | null;
  source: "latest" | "run";
  tasks: Step3TaskV1[];
}): Record<string, unknown> {
  const statusOrder: Step3TaskStatus[] = ["NEW", "READY", "BLOCKED", "IN_PROGRESS", "DONE"];
  const allStatuses: Step3TaskStatus[] = ["NEW", "READY", "BLOCKED", "IN_PROGRESS", "DONE", "SKIPPED", "FAILED"];
  const priorities: Step3TaskPriority[] = ["P0", "P1", "P2", "P3"];
  const modes: Step3TaskMode[] = ["AUTO", "DIY", "TEAM"];
  const categories: Step3TaskCategory[] = [
    "ON_PAGE",
    "TECHNICAL_SEO",
    "LOCAL_SEO",
    "CONTENT",
    "AUTHORITY",
    "SOCIAL",
    "MEASUREMENT",
  ];

  const byStatus: Record<string, number> = Object.fromEntries(allStatuses.map((s) => [s, 0]));
  const byPriority: Record<string, number> = Object.fromEntries(priorities.map((s) => [s, 0]));
  const byMode: Record<string, number> = Object.fromEntries(modes.map((s) => [s, 0]));
  const byCategory: Record<string, number> = Object.fromEntries(categories.map((s) => [s, 0]));
  const blockerCount = new Map<string, number>();
  const blockerExample = new Map<string, string>();
  const clusters = new Set<string>();
  const accessSet = new Set<string>();

  const cards = input.tasks.map((task) => {
    byStatus[task.status] = (byStatus[task.status] ?? 0) + 1;
    byPriority[task.priority] = (byPriority[task.priority] ?? 0) + 1;
    byMode[task.mode] = (byMode[task.mode] ?? 0) + 1;
    byCategory[task.category] = (byCategory[task.category] ?? 0) + 1;
    if (task.scope.cluster) clusters.add(task.scope.cluster);
    for (const access of task.requires.access) {
      accessSet.add(access);
    }
    const blockerCodes: string[] = [];
    for (const blocker of task.blockers) {
      const code = cleanString(blocker.code, 60);
      if (!code) continue;
      blockerCodes.push(code);
      blockerCount.set(code, (blockerCount.get(code) ?? 0) + 1);
      if (!blockerExample.has(code)) {
        blockerExample.set(code, cleanString(blocker.message, 200) || code);
      }
    }
    return {
      task_id: task.task_id,
      title: task.title,
      category: task.category,
      type: task.type,
      priority: task.priority,
      mode: task.mode,
      effort: task.effort,
      confidence: task.confidence,
      impact: task.estimated_impact,
      scope: {
        cluster: task.scope.cluster,
        keyword: task.scope.keyword,
        target_slug: task.scope.target_slug,
        geo: task.scope.geo,
      },
      requires_access: task.requires.access,
      status: task.status,
      blocker_codes: blockerCodes,
      updated_at: task.updated_at,
    };
  });

  cards.sort((a, b) => {
    const p = priorityRank(a.priority as Step3TaskPriority) - priorityRank(b.priority as Step3TaskPriority);
    if (p !== 0) return p;
    return String(a.updated_at).localeCompare(String(b.updated_at)) * -1;
  });

  const columns = statusOrder.map((status) => ({
    status,
    title:
      status === "IN_PROGRESS"
        ? "In progress"
        : status === "READY"
          ? "Ready"
          : status === "BLOCKED"
            ? "Blocked"
            : status === "DONE"
              ? "Done"
              : "New",
    tasks: cards.filter((task) => task.status === status),
  }));

  const topBlockers = [...blockerCount.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([code, count]) => ({
      code,
      count,
      example: blockerExample.get(code) ?? code,
    }));

  const quickWins = cards
    .filter((task) => task.status === "READY" && task.mode === "AUTO" && (task.priority === "P0" || task.priority === "P1"))
    .slice(0, 8)
    .map((task) => ({
      task_id: task.task_id,
      title: task.title,
      priority: task.priority,
      mode: task.mode,
    }));

  const siteInput = safeJsonParseObject(input.site.input_json) ?? {};
  const plan = getSitePlanFromInput(siteInput);
  const metroSlug = cleanString(plan.metro, 120).toLowerCase().replace(/[^a-z0-9]+/g, "-");
  const geoMode = plan.metro_proxy ? "both" : "us";

  return {
    schema_version: "task_board.v1",
    site: {
      site_id: input.site.site_id,
      site_url: input.site.site_url,
      plan_tier: plan.metro_proxy ? "metro" : "standard",
      geo_mode: geoMode,
      metro: plan.metro_proxy ? { name: plan.metro, slug: metroSlug } : null,
    },
    context: {
      site_run_id: input.runId,
      run_date: input.runDate,
      source: input.source,
    },
    summary: {
      counts: {
        total: cards.length,
        by_status: byStatus,
        by_priority: byPriority,
        by_mode: byMode,
        by_category: byCategory,
      },
      top_blockers: topBlockers,
      quick_wins: quickWins,
    },
    filters: {
      status: allStatuses,
      priority: priorities,
      mode: modes,
      category: categories,
      cluster: [...clusters].sort((a, b) => a.localeCompare(b)),
      requires_access: [...accessSet].sort((a, b) => a.localeCompare(b)),
    },
    columns,
    task_details: {
      by_id: Object.fromEntries(input.tasks.map((task) => [task.task_id, task])),
    },
    actions: {
      bulk: [
        {
          id: "AUTO_APPLY_READY",
          label: "Auto-apply all Ready AUTO tasks",
          enabled: quickWins.length > 0,
          requires_confirmation: true,
        },
        {
          id: "MARK_DONE_SELECTED",
          label: "Mark selected as Done",
          enabled: cards.length > 0,
          requires_confirmation: false,
        },
      ],
      connections: [
        {
          access: "GBP",
          label: "Connect Google Business Profile",
          url: "wp-admin/admin.php?page=ai-seo-connect&provider=gbp",
        },
        {
          access: "SOCIAL",
          label: "Connect Social Accounts",
          url: "wp-admin/admin.php?page=ai-seo-connect&provider=social",
        },
        {
          access: "GA4",
          label: "Connect GA4",
          url: "wp-admin/admin.php?page=ai-seo-connect&provider=ga4",
        },
        {
          access: "GSC",
          label: "Connect Search Console",
          url: "wp-admin/admin.php?page=ai-seo-connect&provider=gsc",
        },
      ],
    },
  };
}

async function loadStep3TaskById(
  env: Env,
  input: { siteId: string; taskId: string }
): Promise<Step3TaskV1 | null> {
  const row = await env.DB.prepare(
    `SELECT t.task_id, t.details_json, t.status, t.created_at, t.run_id
     FROM step3_tasks t
     WHERE t.site_id = ? AND t.task_id = ?
     LIMIT 1`
  )
    .bind(input.siteId, input.taskId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return parseTaskV1FromStored(
    {
      task_id: cleanString(row.task_id, 120),
      details_json: cleanString(row.details_json, 128000),
      status: cleanString(row.status, 20),
      created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    },
    input.siteId,
    cleanString(row.run_id, 120)
  );
}

async function loadStep3TaskRowById(
  env: Env,
  input: { siteId: string; taskId: string }
): Promise<{ task_id: string; run_id: string; details_json: string; status: string; created_at: number } | null> {
  const row = await env.DB.prepare(
    `SELECT task_id, run_id, details_json, status, created_at
     FROM step3_tasks
     WHERE site_id = ? AND task_id = ?
     LIMIT 1`
  )
    .bind(input.siteId, input.taskId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    task_id: cleanString(row.task_id, 120),
    run_id: cleanString(row.run_id, 120),
    details_json: cleanString(row.details_json, 128000),
    status: cleanString(row.status, 20),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

function normalizeTaskStatusInput(value: unknown): Step3TaskStatus | null {
  const s = cleanString(value, 30).toUpperCase();
  if (!s) return null;
  const allowed = new Set<Step3TaskStatus>([
    "NEW",
    "READY",
    "BLOCKED",
    "IN_PROGRESS",
    "DONE",
    "SKIPPED",
    "FAILED",
  ]);
  return allowed.has(s as Step3TaskStatus) ? (s as Step3TaskStatus) : null;
}

function canTransitionTaskStatus(from: Step3TaskStatus, to: Step3TaskStatus): boolean {
  if (from === to) return true;
  if (to === "BLOCKED") {
    return from === "NEW" || from === "READY" || from === "IN_PROGRESS";
  }
  if (from === "BLOCKED" && to === "READY") return true;
  if (from === "READY" && to === "IN_PROGRESS") return true;
  if ((from === "READY" || from === "IN_PROGRESS") && to === "DONE") return true;
  return false;
}

function parseBlockersFromBody(
  body: Record<string, unknown> | null
): Array<{ code: string; message: string }> {
  if (!body || !Array.isArray(body.blockers)) return [];
  const out: Array<{ code: string; message: string }> = [];
  for (const raw of body.blockers) {
    const row = parseJsonObject(raw);
    if (!row) continue;
    const code = cleanString(row.code, 60);
    const message = cleanString(row.message, 300);
    if (!code && !message) continue;
    out.push({
      code: code || "MISSING_INPUT",
      message: message || "Blocked by missing requirement.",
    });
    if (out.length >= 20) break;
  }
  return out;
}

async function persistStep3Task(
  env: Env,
  input: {
    siteId: string;
    task: Step3TaskV1;
  }
): Promise<void> {
  await env.DB.prepare(
    `UPDATE step3_tasks
     SET status = ?,
         details_json = ?
     WHERE site_id = ? AND task_id = ?`
  )
    .bind(
      statusToDbStatus(input.task.status),
      safeJsonStringify(input.task, 128000),
      input.siteId,
      input.task.task_id
    )
    .run();
}

async function updateStep3TaskStatus(
  env: Env,
  input: {
    siteId: string;
    taskId: string;
    targetStatus: Step3TaskStatus;
    blockers: Array<{ code: string; message: string }>;
  }
): Promise<{ ok: true; task: Step3TaskV1 } | { ok: false; status: number; error: string; current?: Step3TaskStatus }> {
  const taskRow = await loadStep3TaskRowById(env, { siteId: input.siteId, taskId: input.taskId });
  if (!taskRow) {
    return { ok: false, status: 404, error: "task_not_found" };
  }
  const task = parseTaskV1FromStored(taskRow, input.siteId, taskRow.run_id);
  const from = task.status;
  const to = input.targetStatus;
  if (!canTransitionTaskStatus(from, to)) {
    return {
      ok: false,
      status: 409,
      error: "invalid_status_transition",
      current: from,
    };
  }
  task.status = to;
  task.updated_at = isoFromMs(nowMs());
  if (to === "BLOCKED") {
    task.blockers =
      input.blockers.length > 0
        ? input.blockers
        : [{ code: "POLICY_GUARDRAIL", message: "Task blocked pending manual review." }];
  } else if (to === "READY") {
    task.blockers = [];
  } else if (to === "DONE" || to === "IN_PROGRESS") {
    if (task.blockers.length > 0) {
      task.blockers = [];
    }
  }
  await persistStep3Task(env, { siteId: input.siteId, task });
  return { ok: true, task };
}

async function runStep3BulkAction(
  env: Env,
  input: {
    siteId: string;
    runId: string;
    action: "AUTO_APPLY_READY" | "MARK_DONE_SELECTED";
    taskIds: string[];
  }
): Promise<{
  action: string;
  site_run_id: string;
  attempted: number;
  updated: number;
  skipped: number;
  failed: number;
  updated_task_ids: string[];
  errors: Array<{ task_id: string; error: string }>;
}> {
  const rows = await loadStep3TasksFiltered(env, {
    siteId: input.siteId,
    runId: input.runId,
    executionModes: [],
    taskGroups: [],
    statuses: [],
  });
  const byId = new Map(rows.map((row) => [row.task_id, row]));
  const selectedIds =
    input.taskIds.length > 0
      ? input.taskIds.filter((id) => byId.has(id))
      : input.action === "AUTO_APPLY_READY"
        ? [...byId.keys()]
        : [];

  const errors: Array<{ task_id: string; error: string }> = [];
  const updatedTaskIds: string[] = [];
  let attempted = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;

  for (const taskId of selectedIds) {
    attempted += 1;
    const raw = byId.get(taskId);
    if (!raw) {
      failed += 1;
      errors.push({ task_id: taskId, error: "task_not_found" });
      continue;
    }
    const task = parseTaskV1FromStored(raw, input.siteId, input.runId);
    if (input.action === "AUTO_APPLY_READY") {
      if (!(task.mode === "AUTO" && task.status === "READY")) {
        skipped += 1;
        continue;
      }
      const result = await updateStep3TaskStatus(env, {
        siteId: input.siteId,
        taskId,
        targetStatus: "DONE",
        blockers: [],
      });
      if (!result.ok) {
        failed += 1;
        errors.push({ task_id: taskId, error: result.error });
        continue;
      }
      updated += 1;
      updatedTaskIds.push(taskId);
      continue;
    }

    if (input.action === "MARK_DONE_SELECTED") {
      if (task.status === "DONE") {
        skipped += 1;
        continue;
      }
      const result = await updateStep3TaskStatus(env, {
        siteId: input.siteId,
        taskId,
        targetStatus: "DONE",
        blockers: [],
      });
      if (!result.ok) {
        failed += 1;
        errors.push({ task_id: taskId, error: result.error });
        continue;
      }
      updated += 1;
      updatedTaskIds.push(taskId);
      continue;
    }
  }

  return {
    action: input.action,
    site_run_id: input.runId,
    attempted,
    updated,
    skipped,
    failed,
    updated_task_ids: updatedTaskIds,
    errors,
  };
}

async function runStep3LocalExecutionPlan(
  env: Env,
  site: Step1SiteRecord,
  opts: { requestedStep2Date?: string | null; parentJobId: string }
): Promise<Record<string, unknown>> {
  const now = nowMs();
  const date = toDateYYYYMMDD(now);
  const research = await loadLatestStep1KeywordResearchResults(env, site.site_id);
  if (!research) {
    throw new Error("step1_keyword_research_required");
  }

  const step2Date = cleanString(opts.requestedStep2Date, 20) || (await loadLatestStep2DateForSite(env, site.site_id));
  if (!step2Date) {
    throw new Error("step2_report_required");
  }
  const step2Report = await loadLatestStep2Report(env, site.site_id, step2Date);
  const primaryRows = parseStep1PrimaryFromResults(research);
  const competitorCandidates = await loadStep3CompetitorCandidatesFromStep2(env, site.site_id, step2Date, 30);
  const coreCompetitors = competitorCandidates.filter((row) => !row.is_directory).slice(0, 12);
  const competitorSet = coreCompetitors.length > 0 ? coreCompetitors : competitorCandidates.slice(0, 12);
  const keywordSignals = buildStep3KeywordSignals(step2Report);
  const diffSignals = await loadStep3DiffSignals(env, { siteId: site.site_id, date: step2Date });
  const previousCadence = await loadStep3PreviousSocialCadence(env, site.site_id);

  const runId = uuid("s3run");
  await env.DB.prepare(
    `INSERT INTO step3_runs (
      run_id, site_id, date_yyyymmdd, source_step2_date, status, summary_json, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(runId, site.site_id, date, step2Date, "running", "{}", now, now)
    .run();

  const ownSignals = new Map<string, Step3OwnPageSignal>();
  const mozSignals = new Map<string, Step3MozKeywordSignal>();
  const top3Modules = new Map<string, { faq_rate: number; pricing_rate: number }>();
  for (const row of primaryRows) {
    const keyword = cleanString(row.keyword, 300);
    if (!keyword) continue;
    const slug = cleanString(row.recommended_slug, 160) || null;
    const targetUrl = slug
      ? `${cleanString(site.site_url, 2000).replace(/\/+$/, "")}/${slug.replace(/^\/+/, "")}`
      : cleanString(row.target_url, 2000) || null;
    if (targetUrl) {
      const own = await loadStep3OwnPageSignal(env, { targetUrl, targetSlug: slug, date: step2Date });
      if (own) ownSignals.set(toKeywordNorm(keyword), own);
    }
    const mozSignal = await loadStep3MozKeywordSignal(env, {
      siteId: site.site_id,
      date: step2Date,
      keyword,
      targetUrl,
    });
    if (mozSignal) {
      mozSignals.set(toKeywordNorm(keyword), mozSignal);
    }
    const rates = await loadStep3Top3ModuleRates(env, { siteId: site.site_id, date: step2Date, keyword });
    top3Modules.set(toKeywordNorm(keyword), rates);
  }
  const competitorCadenceNow =
    competitorSet.length > 0
      ? competitorSet.reduce((sum, row) => sum + Math.max(1, Math.min(7, Math.round(row.appearance_count / 2))), 0) /
        competitorSet.length
      : 0;
  const socialCadenceIncreased = previousCadence > 0 ? competitorCadenceNow > previousCadence * 1.15 : competitorCadenceNow >= 3;

  const tasks = buildStep3Tasks({
    site,
    siteRunId: runId,
    primaryRows,
    competitors: competitorSet,
    keywordSignals,
    ownSignals,
    mozSignals,
    top3Modules,
    diffSignals,
    socialCadenceIncreased,
    step2Date,
  });
  const riskFlags = buildStep3RiskFlags({
    site,
    primaryRows,
    competitors: competitorSet,
    tasks,
  });

  for (const row of tasks) {
    await env.DB.prepare(
      `INSERT INTO step3_tasks (
        task_id, run_id, site_id, task_group, task_type, execution_mode, priority,
        title, why_text, details_json, target_slug, target_url, status, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        cleanString(row.task_id, 120),
        runId,
        site.site_id,
        row.category,
        row.type,
        modeToExecutionMode(row.mode),
        priorityToDbWeight(row.priority),
        row.title,
        row.summary,
        safeJsonStringify(row, 64_000),
        row.scope.target_slug,
        row.scope.target_url,
        statusToDbStatus(row.status),
        nowMs()
      )
      .run();
  }

  const socialThemes = primaryRows
    .map((row) => cleanString(row.keyword, 120))
    .filter(Boolean)
    .slice(0, 6);
  for (const competitor of competitorSet) {
    await env.DB.prepare(
      `INSERT INTO step3_competitors (
        competitor_id, run_id, site_id, domain, source, appearance_count, avg_rank,
        is_directory, sample_urls_json, social_profiles_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        uuid("s3comp"),
        runId,
        site.site_id,
        competitor.domain,
        "step2_serp_top5_frequency",
        competitor.appearance_count,
        competitor.avg_rank,
        competitor.is_directory ? 1 : 0,
        safeJsonStringify(competitor.sample_urls, 16_000),
        safeJsonStringify(inferCompetitorSocialProfiles(competitor.domain), 8_000),
        nowMs()
      )
      .run();

    const platformRows = [
      {
        platform: "facebook",
        cadence: Number(Math.max(1, Math.min(7, Math.round(competitor.appearance_count / 2))).toFixed(1)),
        engagement: Number(Math.max(5, 60 - competitor.avg_rank * 6).toFixed(1)),
        content_types: ["before_after", "reviews", "offers"],
      },
      {
        platform: "instagram",
        cadence: Number(Math.max(1, Math.min(7, Math.round(competitor.appearance_count / 2))).toFixed(1)),
        engagement: Number(Math.max(5, 55 - competitor.avg_rank * 5).toFixed(1)),
        content_types: ["short_video", "team", "jobsite"],
      },
      {
        platform: "youtube",
        cadence: Number(Math.max(0.5, Math.min(4, competitor.appearance_count / 4)).toFixed(1)),
        engagement: Number(Math.max(3, 40 - competitor.avg_rank * 4).toFixed(1)),
        content_types: ["how_to", "faq", "case_story"],
      },
    ];
    for (const platformRow of platformRows) {
      await env.DB.prepare(
        `INSERT INTO step3_social_signals (
          signal_id, run_id, site_id, domain, platform, cadence_per_week, engagement_proxy,
          content_types_json, recurring_themes_json, source, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
        .bind(
          uuid("s3sig"),
          runId,
          site.site_id,
          competitor.domain,
          platformRow.platform,
          platformRow.cadence,
          platformRow.engagement,
          safeJsonStringify(platformRow.content_types, 4000),
          safeJsonStringify(socialThemes, 4000),
          "public_metadata_inference",
          nowMs()
        )
        .run();
    }
  }

  for (const flag of riskFlags) {
    await env.DB.prepare(
      `INSERT INTO step3_risk_flags (
        flag_id, run_id, site_id, risk_type, severity, message, blocked_action, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        uuid("s3risk"),
        runId,
        site.site_id,
        cleanString(flag.risk_type, 120),
        flag.severity,
        cleanString(flag.message, 500),
        cleanString(flag.blocked_action, 120) || null,
        nowMs()
      )
      .run();
  }

  const splitCounts = {
    auto_safe: tasks.filter((row) => row.mode === "AUTO").length,
    assisted: tasks.filter((row) => row.mode === "DIY").length,
    team_only: tasks.filter((row) => row.mode === "TEAM").length,
  };
  const step2KeywordReports = Array.isArray(step2Report.keyword_reports) ? step2Report.keyword_reports.length : 0;

  const reportPayload = {
    schema_version: "1.0",
    site_id: site.site_id,
    run_id: runId,
    date,
    source_step2_date: step2Date,
    assumptions: {
      region_scope: "US-only",
      social_signal_source: "public metadata + inference",
      note: "Competitor strategy is used for cadence/theme signals only; content output remains original.",
    },
    competitor_set: competitorSet.map((row) => ({
      domain: row.domain,
      appearance_count: row.appearance_count,
      avg_rank: Number(row.avg_rank.toFixed(2)),
      is_directory: row.is_directory,
      sample_urls: row.sample_urls.slice(0, 5),
    })),
    execution_split: splitCounts,
    tasks,
    risk_flags: riskFlags,
    metadata: {
      parent_job_id: opts.parentJobId,
      primary_keywords_considered: primaryRows.length,
      keyword_reports_available: step2KeywordReports,
    },
  };

  await env.DB.prepare(
    `INSERT INTO step3_reports (
      report_id, run_id, site_id, date_yyyymmdd, report_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)`
  )
    .bind(uuid("s3rep"), runId, site.site_id, date, safeJsonStringify(reportPayload, 512_000), nowMs())
    .run();

  const summary = {
    site_id: site.site_id,
    run_id: runId,
    date,
    source_step2_date: step2Date,
    tasks_total: tasks.length,
    split_counts: splitCounts,
    competitor_count: competitorSet.length,
    risk_flags: riskFlags.length,
  };
  await env.DB.prepare(
    `UPDATE step3_runs
     SET status = ?, summary_json = ?, updated_at = ?
     WHERE run_id = ?`
  )
    .bind("success", safeJsonStringify(summary, 64_000), nowMs(), runId)
    .run();

  return {
    ...summary,
    report: reportPayload,
  };
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
  proxy_lease_id: string | null;
  total_candidates: number;
  attempted: number;
  succeeded: number;
  failed: number;
  skipped: number;
  graph_ready_rows: number;
  rows: Array<{
    watch_id: string;
    keyword: string;
    status: "ok" | "error" | "skipped";
    reason: string | null;
    serp_id: string | null;
    job_id: string | null;
    proxy_lease_id: string | null;
    proxy_geo: string | null;
    graph_rows_last_30d: number;
  }>;
};

async function runSerpWatchlist(
  env: Env,
  input: { userId: string; watchId: string; limit: number; force: boolean; proxyLeaseId: string | null }
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
    proxy_lease_id: input.proxyLeaseId,
    total_candidates: candidates.length,
    attempted: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    graph_ready_rows: 0,
    rows: [],
  };

  for (const candidate of candidates) {
    let activeLease: ProxyLeaseRow | null = null;
    if (input.proxyLeaseId) {
      activeLease = await getActiveProxyLease(env, input.proxyLeaseId, candidate.user_id);
      if (!activeLease) {
        summary.failed += 1;
        summary.rows.push({
          watch_id: candidate.watch_id,
          keyword: candidate.phrase,
          status: "error",
          reason: "proxy_lease_not_found_or_expired",
          serp_id: null,
          job_id: null,
          proxy_lease_id: input.proxyLeaseId,
          proxy_geo: null,
          graph_rows_last_30d: 0,
        });
        continue;
      }
    }

    if (!input.force && candidate.last_run_at != null && toDateYYYYMMDD(candidate.last_run_at) === dayUtc) {
      summary.skipped += 1;
      summary.rows.push({
        watch_id: candidate.watch_id,
        keyword: candidate.phrase,
        status: "skipped",
        reason: "already_ran_today",
        serp_id: candidate.last_serp_id,
        job_id: null,
        proxy_lease_id: activeLease?.lease_id ?? null,
        proxy_geo: activeLease ? formatProxyGeo(activeLease) : null,
        graph_rows_last_30d: 0,
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
        proxy_lease_id: activeLease?.lease_id ?? null,
      },
    });
    const collected = await collectAndPersistSerpTop20(env, {
      userId: candidate.user_id,
      phrase: candidate.phrase,
      region,
      device: candidate.device,
      maxResults: candidate.max_results,
      proxyUrl: activeLease?.proxy_url ?? null,
      provider: resolveSerpPrimary(env, null),
      geoProvider: "proxy_lease_pool",
      geoLabel: cleanString(region.city ?? region.region, 120) || "us",
      auditJobId: jobId,
    });
    const finishedAt = nowMs();

    if (collected.ok === true) {
      const graphRows = await loadDailyLatestSerpRows(
        env,
        candidate.phrase,
        startedAt - 30 * 24 * 60 * 60 * 1000,
        candidate.device
      );
      if (graphRows.length > 0) {
        summary.graph_ready_rows += 1;
      }
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
          extractor_mode: collected.extractorMode,
          fallback_reason: collected.fallbackReason,
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
        proxy_lease_id: activeLease?.lease_id ?? null,
        proxy_geo: activeLease ? formatProxyGeo(activeLease) : null,
        graph_rows_last_30d: graphRows.length,
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
      proxy_lease_id: activeLease?.lease_id ?? null,
      proxy_geo: activeLease ? formatProxyGeo(activeLease) : null,
      graph_rows_last_30d: 0,
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

type MarketplaceTaskRow = {
  task_id: string;
  status: string;
  platform: string;
  issuer_wallet: string;
  payout_amount: string;
  payout_token: string;
  payout_chain: string | null;
  deadline_at: number;
  task_spec_json: string;
  task_spec_hash: string;
  created_at: number;
  updated_at: number;
};

type MarketplaceClaimRow = {
  claim_id: string;
  task_id: string;
  worker_wallet: string;
  status: string;
  claimed_at: number;
  expires_at: number;
  released_at: number | null;
  release_reason: string | null;
};

async function loadMarketplaceTask(env: Env, taskId: string): Promise<MarketplaceTaskRow | null> {
  const row = await env.DB.prepare(
    `SELECT
      task_id, status, platform, issuer_wallet,
      payout_amount, payout_token, payout_chain,
      deadline_at, task_spec_json, task_spec_hash, created_at, updated_at
     FROM task_market_tasks
     WHERE task_id = ?
     LIMIT 1`
  )
    .bind(taskId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    task_id: cleanString(row.task_id, 120),
    status: cleanString(row.status, 40),
    platform: cleanString(row.platform, 80),
    issuer_wallet: cleanString(row.issuer_wallet, 200),
    payout_amount: cleanString(row.payout_amount, 80),
    payout_token: cleanString(row.payout_token, 80),
    payout_chain: cleanString(row.payout_chain, 80) || null,
    deadline_at: clampInt(row.deadline_at, 0, Number.MAX_SAFE_INTEGER, 0),
    task_spec_json: cleanString(row.task_spec_json, 64000),
    task_spec_hash: cleanString(row.task_spec_hash, 200),
    created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
    updated_at: clampInt(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

async function loadActiveMarketplaceClaim(env: Env, taskId: string): Promise<MarketplaceClaimRow | null> {
  const row = await env.DB.prepare(
    `SELECT
      claim_id, task_id, worker_wallet, status, claimed_at, expires_at, released_at, release_reason
     FROM task_market_claims
     WHERE task_id = ? AND status = 'active'
     LIMIT 1`
  )
    .bind(taskId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return {
    claim_id: cleanString(row.claim_id, 120),
    task_id: cleanString(row.task_id, 120),
    worker_wallet: cleanString(row.worker_wallet, 200),
    status: cleanString(row.status, 40),
    claimed_at: clampInt(row.claimed_at, 0, Number.MAX_SAFE_INTEGER, 0),
    expires_at: clampInt(row.expires_at, 0, Number.MAX_SAFE_INTEGER, 0),
    released_at:
      row.released_at == null ? null : clampInt(row.released_at, 0, Number.MAX_SAFE_INTEGER, 0),
    release_reason: cleanString(row.release_reason, 200) || null,
  };
}

async function listOpenMarketplaceTasks(env: Env, limit: number): Promise<Array<Record<string, unknown>>> {
  const rows = await env.DB.prepare(
    `SELECT
      t.task_id,
      t.platform,
      t.payout_amount,
      t.payout_token,
      t.payout_chain,
      t.deadline_at,
      t.task_spec_hash,
      t.status,
      t.created_at
     FROM task_market_tasks t
     WHERE t.status = 'open'
       AND t.deadline_at > ?
       AND NOT EXISTS (
         SELECT 1
         FROM task_market_claims c
         WHERE c.task_id = t.task_id AND c.status = 'active'
       )
     ORDER BY t.created_at DESC
     LIMIT ?`
  )
    .bind(nowMs(), limit)
    .all<Record<string, unknown>>();
  return rows.results ?? [];
}

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

type MemoryUpsertInput = {
  type: string;
  scope_key: string | null;
  collected_day: string;
  geo_key: string;
  title: string | null;
  text_summary: string;
  tags: Record<string, unknown>;
  raw_payload: unknown;
};

function normalizeMemoryType(input: unknown): string {
  const base = cleanString(input, 120).toLowerCase();
  return base.replace(/[^a-z0-9_:-]+/g, "_").replace(/_+/g, "_").replace(/^_+|_+$/g, "") || "unknown";
}

function normalizeMemoryScopeKey(input: unknown): string {
  const base = cleanString(input, 160).toLowerCase();
  return base.replace(/[^a-z0-9_:-]+/g, "_").replace(/_+/g, "_").replace(/^_+|_+$/g, "") || "na";
}

function normalizeMemoryGeoKey(input: unknown): string {
  const raw = cleanString(input, 120).toLowerCase();
  if (!raw || raw === "us") return "us";
  if (raw === "both") return "both";
  if (raw.startsWith("metro:")) return raw;
  const slug = raw.replace(/[^a-z0-9-]+/g, "-").replace(/-+/g, "-").replace(/^-+|-+$/g, "");
  return slug ? `metro:${slug}` : "us";
}

function normalizeCollectedDay(input: unknown): string | null {
  const raw = cleanString(input, 20);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  return null;
}

function parseMemoryUpsertBody(body: Record<string, unknown>): MemoryUpsertInput | { error: string } {
  const type = normalizeMemoryType(body.type);
  const scopeKey = cleanString(body.scope_key, 160) ? normalizeMemoryScopeKey(body.scope_key) : null;
  const collectedDay = normalizeCollectedDay(body.collected_day) ?? toDateYYYYMMDD(nowMs());
  const geoKey = normalizeMemoryGeoKey(body.geo_key);
  const title = cleanString(body.title, 200) || null;
  const textSummary = cleanString(body.text_summary, 16000);
  if (!textSummary) {
    return { error: "text_summary_required" };
  }
  const tags = parseJsonObject(body.tags) ?? {};
  return {
    type,
    scope_key: scopeKey,
    collected_day: collectedDay,
    geo_key: geoKey,
    title,
    text_summary: textSummary,
    tags,
    raw_payload: body.raw_payload ?? {},
  };
}

async function loadMemorySite(env: Env, siteId: string): Promise<{ site_id: string } | null> {
  const row = await env.DB.prepare(`SELECT site_id FROM wp_ai_seo_sites WHERE site_id = ? LIMIT 1`)
    .bind(siteId)
    .first<Record<string, unknown>>();
  if (!row) return null;
  return { site_id: cleanString(row.site_id, 120) };
}

async function savePlanSelection(
  env: Env,
  input: {
    siteId: string | null;
    planKey: PlanDefinition["key"];
    contactEmail: string;
    companyName: string | null;
    notes: string | null;
    source: string;
  }
): Promise<{ subscriptionId: string }> {
  const subscriptionId = uuid("plan");
  const site = input.siteId ? await loadMemorySite(env, input.siteId) : null;
  await env.DB.prepare(
    `INSERT INTO billing_site_subscriptions (
      subscription_id, site_id, plan_key, company_name, contact_email, source, status, notes_json, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, 'lead', ?, strftime('%s','now'), strftime('%s','now'))`
  )
    .bind(
      subscriptionId,
      site?.site_id ?? null,
      input.planKey,
      input.companyName,
      input.contactEmail,
      input.source,
      canonicalJson({ notes: input.notes ?? "" })
    )
    .run();
  return { subscriptionId };
}

async function makeMemoryVectorId(input: {
  namespace: string;
  siteId: string;
  type: string;
  geoKey: string;
  collectedDay: string;
  scopeKey: string;
}): Promise<string> {
  const raw = `${input.namespace}|${input.siteId}|${input.type}|${input.geoKey}|${input.collectedDay}|${input.scopeKey}`;
  const hash = await sha256Hex(raw);
  return `m_${hash.slice(0, 24)}`;
}

async function embedTextOpenAI(
  env: Env,
  text: string
): Promise<{ embedding: number[]; tokenCount: number | null; model: string; dims: number }> {
  const apiKey = cleanString(env.OPENAI_API_KEY, 300);
  if (!apiKey) {
    throw new Error("openai_api_key_not_configured");
  }
  const response = await fetch("https://api.openai.com/v1/embeddings", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: MEMORY_EMBEDDING_MODEL,
      input: text,
    }),
  });
  if (!response.ok) {
    const err = cleanString(await response.text(), 1000);
    throw new Error(`openai_embedding_failed:${response.status}:${err}`);
  }
  const payload = (await response.json()) as Record<string, unknown>;
  const data = Array.isArray(payload.data) ? payload.data : [];
  const first = parseJsonObject(data[0]);
  const embeddingRaw = Array.isArray(first?.embedding) ? first?.embedding : [];
  const embedding = embeddingRaw
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  if (embedding.length === 0) {
    throw new Error("openai_embedding_empty");
  }
  const usage = parseJsonObject(payload.usage);
  return {
    embedding,
    tokenCount: usage ? clampInt(usage.total_tokens, 0, Number.MAX_SAFE_INTEGER, 0) : null,
    model: MEMORY_EMBEDDING_MODEL,
    dims: embedding.length,
  };
}

function projectEmbeddingToVectorDims(embedding: number[]): number[] {
  if (embedding.length === MEMORY_EMBEDDING_DIMS) {
    return embedding;
  }
  if (embedding.length === MEMORY_EMBEDDING_DIMS * 2) {
    const out: number[] = [];
    for (let i = 0; i < MEMORY_EMBEDDING_DIMS; i += 1) {
      out.push((embedding[i] + embedding[i + MEMORY_EMBEDDING_DIMS]) / 2);
    }
    return out;
  }
  throw new Error(`embedding_dimension_unsupported:${embedding.length}`);
}

async function upsertSemanticMemory(
  env: Env,
  siteId: string,
  input: MemoryUpsertInput
): Promise<{ memoryId: string; vectorId: string; r2Key: string }> {
  if (!env.MEMORY_R2) {
    throw new Error("memory_r2_binding_missing");
  }
  if (!env.USER_MEM) {
    throw new Error("vectorize_binding_missing");
  }

  const scopeKey = input.scope_key ?? "na";
  const memoryId = uuid("mem");
  const payloadCanonical = canonicalJson(input.raw_payload);
  const payloadSha = await sha256Hex(payloadCanonical);
  const r2Key = `memory/v1/site/${siteId}/day/${input.collected_day}/geo/${input.geo_key}/type/${input.type}/scope/${scopeKey}/${memoryId}.json`;

  await env.MEMORY_R2.put(r2Key, payloadCanonical, {
    httpMetadata: { contentType: "application/json" },
    customMetadata: {
      site_id: siteId,
      type: input.type,
      geo_key: input.geo_key,
      collected_day: input.collected_day,
      scope_key: scopeKey,
      memory_id: memoryId,
    },
  });

  const embedded = await embedTextOpenAI(env, input.text_summary);
  const vectorValues = projectEmbeddingToVectorDims(embedded.embedding);
  const vectorId = await makeMemoryVectorId({
    namespace: "mem_v1",
    siteId,
    type: input.type,
    geoKey: input.geo_key,
    collectedDay: input.collected_day,
    scopeKey,
  });

  await env.USER_MEM.upsert([
    {
      id: vectorId,
      values: vectorValues,
      metadata: {
        site_id: siteId,
        type: input.type,
        geo_key: input.geo_key,
        collected_day: input.collected_day,
        scope_key: scopeKey,
      },
    },
  ]);

  await env.DB.prepare(
    `INSERT INTO memory_items (
      memory_id, site_id, type, scope_key, collected_day, geo_key,
      title, text_summary, tags_json, source_r2_key, source_sha256,
      vector_namespace, vector_id, embedding_model, embedding_dims, token_count, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'mem_v1', ?, ?, ?, ?, strftime('%s','now'), strftime('%s','now'))
    ON CONFLICT(site_id, type, COALESCE(scope_key,''), collected_day, geo_key) DO UPDATE SET
      title = excluded.title,
      text_summary = excluded.text_summary,
      tags_json = excluded.tags_json,
      source_r2_key = excluded.source_r2_key,
      source_sha256 = excluded.source_sha256,
      vector_id = excluded.vector_id,
      embedding_model = excluded.embedding_model,
      embedding_dims = excluded.embedding_dims,
      token_count = excluded.token_count,
      updated_at = strftime('%s','now')`
  )
    .bind(
      memoryId,
      siteId,
      input.type,
      input.scope_key,
      input.collected_day,
      input.geo_key,
      input.title,
      input.text_summary,
      canonicalJson(input.tags),
      r2Key,
      payloadSha,
      vectorId,
      embedded.model,
      vectorValues.length,
      embedded.tokenCount
    )
    .run();

  await env.DB.prepare(
    `INSERT INTO memory_events (site_id, memory_id, event_type, message, meta_json, created_at)
     VALUES (?, ?, 'upsert', 'memory upserted', ?, strftime('%s','now'))`
  )
    .bind(siteId, memoryId, canonicalJson({ vector_id: vectorId, type: input.type, geo_key: input.geo_key }))
    .run();

  return { memoryId, vectorId, r2Key };
}

async function searchSemanticMemory(
  env: Env,
  siteId: string,
  query: string,
  options: {
    k: number;
    type: string | null;
    geoKey: string | null;
    days: number;
  }
): Promise<Array<Record<string, unknown>>> {
  if (!env.USER_MEM) {
    throw new Error("vectorize_binding_missing");
  }
  const embedded = await embedTextOpenAI(env, query);
  const queryVector = projectEmbeddingToVectorDims(embedded.embedding);
  const vectorFilter: Record<string, unknown> = { site_id: siteId };
  if (options.type) vectorFilter.type = options.type;
  if (options.geoKey) vectorFilter.geo_key = options.geoKey;

  const vectorResponse = (await env.USER_MEM.query(queryVector, {
    topK: options.k,
    filter: vectorFilter,
  })) as { matches?: Array<{ id: string; score: number }> };
  const matches = Array.isArray(vectorResponse.matches) ? vectorResponse.matches : [];
  if (matches.length === 0) {
    return [];
  }

  const earliestMs = nowMs() - options.days * 24 * 60 * 60 * 1000;
  const earliestDay = toDateYYYYMMDD(earliestMs);
  const vectorIds = matches.map((m) => cleanString(m.id, 400)).filter(Boolean);
  const placeholders = vectorIds.map(() => "?").join(",");
  const stmt = env.DB.prepare(
    `SELECT
       memory_id, site_id, type, scope_key, collected_day, geo_key, title,
       tags_json, source_r2_key, source_sha256, vector_namespace, vector_id, embedding_model, embedding_dims, token_count, updated_at
     FROM memory_items
     WHERE site_id = ?
       AND collected_day >= ?
       AND vector_id IN (${placeholders})`
  );
  const rows = await stmt.bind(siteId, earliestDay, ...vectorIds).all<Record<string, unknown>>();
  const byVectorId = new Map<string, Record<string, unknown>>();
  for (const row of rows.results ?? []) {
    byVectorId.set(cleanString(row.vector_id, 400), row);
  }
  return matches
    .map((match) => ({
      vector_id: cleanString(match.id, 400),
      score: Number(match.score ?? 0) || 0,
      item: byVectorId.get(cleanString(match.id, 400)) ?? null,
    }))
    .filter((entry) => entry.item !== null);
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const pluginV1Prefix = "/plugin/wp/v1";

    if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/portal" || url.pathname === "/pricing")) {
      const html = renderPortalHtml(url.hostname);
      return new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    }

    if (req.method === "GET" && url.pathname === "/v1/plans") {
      return Response.json({
        ok: true,
        plans: PORTAL_PLAN_DEFINITIONS,
      });
    }

    if (req.method === "POST" && url.pathname === "/v1/plans/select") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const planKey = normalizePlanKey(body.plan_key);
      if (!planKey) return Response.json({ ok: false, error: "invalid_plan_key" }, { status: 400 });
      const contactEmail = cleanString(body.contact_email, 200).toLowerCase();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contactEmail)) {
        return Response.json({ ok: false, error: "valid_contact_email_required" }, { status: 400 });
      }
      const siteIdRaw = cleanString(body.site_id, 120);
      const companyName = cleanString(body.company_name, 180) || null;
      const notes = cleanString(body.notes, 2000) || null;
      try {
        const saved = await savePlanSelection(env, {
          siteId: siteIdRaw || null,
          planKey,
          contactEmail,
          companyName,
          notes,
          source: "portal",
        });
        return Response.json({
          ok: true,
          subscription_id: saved.subscriptionId,
          site_id: siteIdRaw || null,
          plan_key: planKey,
          contact_email: contactEmail,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        return Response.json({ ok: false, error: message }, { status: 500 });
      }
    }

    if (req.method === "POST" && url.pathname === "/v1/tasks") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const idem = await resolveIdempotency(env, req, "POST:/v1/tasks", body, { required: true });
      if (!idem.ok) return idem.response;
      if (idem.replayResponse) return idem.replayResponse;

      const spec = normalizeTaskSpec(body);
      const validationError = validateTaskSpec(spec);
      if (validationError) {
        return Response.json({ ok: false, error: validationError }, { status: 400 });
      }
      const taskId = cleanString(body.task_id, 120) || uuid("tmktask");
      const specJson = canonicalJson(spec);
      const taskSpecHash = await sha256Hex(specJson);
      const agentSignature = await signAgentPayload(env, `${taskId}.${taskSpecHash}`);
      if (!agentSignature) {
        return Response.json({ ok: false, error: "task_signing_secret_not_configured" }, { status: 503 });
      }
      try {
        await env.DB.prepare(
          `INSERT INTO task_market_tasks (
            task_id, status, platform, issuer_wallet, payout_amount, payout_token, payout_chain,
            deadline_at, task_spec_json, task_spec_hash, created_at, updated_at
          ) VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
          .bind(
            taskId,
            spec.platform,
            spec.issuer_wallet,
            spec.payout.amount,
            spec.payout.token,
            spec.payout.chain,
            spec.deadline_at,
            specJson,
            taskSpecHash,
            nowMs(),
            nowMs()
          )
          .run();
      } catch {
        return Response.json({ ok: false, error: "task_id_already_exists" }, { status: 409 });
      }
      await writeTaskAuditLog(env, {
        taskId,
        eventType: "task_created",
        actorWallet: spec.issuer_wallet,
        payload: { task_spec_hash: taskSpecHash, platform: spec.platform, payout: spec.payout },
      });
      const responseBody = {
        ok: true,
        task_id: taskId,
        status: "open",
        taskSpecHash,
        agentSignature,
        task_spec: spec,
      };
      if (idem.key) {
        await saveIdempotencyRecord(env, {
          key: idem.key,
          endpoint: "POST:/v1/tasks",
          requestHash: idem.requestHash,
          responseStatus: 200,
          responseBody,
        });
      }
      return Response.json(responseBody);
    }

    if (req.method === "GET" && url.pathname === "/v1/tasks/open") {
      const limit = clampInt(url.searchParams.get("limit"), 1, 200, 50);
      const rows = await listOpenMarketplaceTasks(env, limit);
      return Response.json({
        ok: true,
        count: rows.length,
        tasks: rows.map((row) => ({
          task_id: cleanString(row.task_id, 120),
          status: cleanString(row.status, 40),
          platform: cleanString(row.platform, 80),
          payout_amount: cleanString(row.payout_amount, 80),
          payout_token: cleanString(row.payout_token, 80),
          payout_chain: cleanString(row.payout_chain, 80) || null,
          deadline_at: clampInt(row.deadline_at, 0, Number.MAX_SAFE_INTEGER, 0),
          task_spec_hash: cleanString(row.task_spec_hash, 200),
          created_at: clampInt(row.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
        })),
      });
    }

    const taskIdMatch = url.pathname.match(/^\/v1\/tasks\/([^/]+)$/);
    if (req.method === "GET" && taskIdMatch) {
      const taskId = cleanString(decodeURIComponent(taskIdMatch[1]), 120);
      const task = await loadMarketplaceTask(env, taskId);
      if (!task) {
        return Response.json({ ok: false, error: "task_not_found" }, { status: 404 });
      }
      const activeClaim = await loadActiveMarketplaceClaim(env, taskId);
      const latestVerification = await env.DB.prepare(
        `SELECT verification_id, passed, verification_result_hash, created_at
         FROM task_market_verifications
         WHERE task_id = ?
         ORDER BY created_at DESC
         LIMIT 1`
      )
        .bind(taskId)
        .first<Record<string, unknown>>();
      return Response.json({
        ok: true,
        task_id: task.task_id,
        status: task.status,
        platform: task.platform,
        issuer_wallet: task.issuer_wallet,
        payout: {
          amount: task.payout_amount,
          token: task.payout_token,
          chain: task.payout_chain,
        },
        deadline_at: task.deadline_at,
        task_spec_hash: task.task_spec_hash,
        task_spec: safeJsonParseObject(task.task_spec_json) ?? {},
        active_claim: activeClaim,
        latest_verification: latestVerification
          ? {
              verification_id: cleanString(latestVerification.verification_id, 120),
              passed: clampInt(latestVerification.passed, 0, 1, 0) === 1,
              verification_result_hash: cleanString(latestVerification.verification_result_hash, 200),
              created_at: clampInt(latestVerification.created_at, 0, Number.MAX_SAFE_INTEGER, 0),
            }
          : null,
      });
    }

    const taskActionMatch = url.pathname.match(/^\/v1\/tasks\/([^/]+)\/(claim|release|evidence|verify|payout-authorize)$/);
    if (taskActionMatch && req.method === "POST") {
      const taskId = cleanString(decodeURIComponent(taskActionMatch[1]), 120);
      const action = cleanString(taskActionMatch[2], 40);
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });

      const endpoint = `POST:/v1/tasks/:id/${action}`;
      const idem = await resolveIdempotency(env, req, endpoint, body, { required: true });
      if (!idem.ok) return idem.response;
      if (idem.replayResponse) return idem.replayResponse;

      const task = await loadMarketplaceTask(env, taskId);
      if (!task) return Response.json({ ok: false, error: "task_not_found" }, { status: 404 });

      if (action === "claim") {
        const wallet = cleanString(body.worker_wallet ?? body.wallet, 200).toLowerCase();
        if (!wallet) return Response.json({ ok: false, error: "worker_wallet_required" }, { status: 400 });
        if (task.deadline_at <= nowMs()) {
          return Response.json({ ok: false, error: "task_deadline_passed" }, { status: 409 });
        }
        const active = await loadActiveMarketplaceClaim(env, taskId);
        if (active) {
          if (active.worker_wallet === wallet) {
            const responseBody = { ok: true, task_id: taskId, claim: active, already_claimed: true };
            if (idem.key) {
              await saveIdempotencyRecord(env, {
                key: idem.key,
                endpoint,
                requestHash: idem.requestHash,
                responseStatus: 200,
                responseBody,
              });
            }
            return Response.json(responseBody);
          }
          return Response.json({ ok: false, error: "task_already_claimed" }, { status: 409 });
        }
        const claimId = uuid("claim");
        const claimedAt = nowMs();
        const ttlMinutes = clampInt(body.ttl_minutes, 1, TASK_MAX_CLAIM_MINUTES, 60);
        const expiresAt = claimedAt + ttlMinutes * 60_000;
        try {
          await env.DB.prepare(
            `INSERT INTO task_market_claims (
              claim_id, task_id, worker_wallet, status, claimed_at, expires_at
            ) VALUES (?, ?, ?, 'active', ?, ?)`
          )
            .bind(claimId, taskId, wallet, claimedAt, expiresAt)
            .run();
        } catch {
          return Response.json({ ok: false, error: "task_already_claimed" }, { status: 409 });
        }
        await env.DB.prepare(
          `UPDATE task_market_tasks
           SET status = 'claimed', updated_at = ?
           WHERE task_id = ?`
        )
          .bind(nowMs(), taskId)
          .run();
        await writeTaskAuditLog(env, {
          taskId,
          eventType: "task_claimed",
          actorWallet: wallet,
          payload: { claim_id: claimId, expires_at: expiresAt },
        });
        const responseBody = {
          ok: true,
          task_id: taskId,
          claim: {
            claim_id: claimId,
            task_id: taskId,
            worker_wallet: wallet,
            status: "active",
            claimed_at: claimedAt,
            expires_at: expiresAt,
            released_at: null,
            release_reason: null,
          },
        };
        if (idem.key) {
          await saveIdempotencyRecord(env, {
            key: idem.key,
            endpoint,
            requestHash: idem.requestHash,
            responseStatus: 200,
            responseBody,
          });
        }
        return Response.json(responseBody);
      }

      if (action === "release") {
        const active = await loadActiveMarketplaceClaim(env, taskId);
        if (!active) return Response.json({ ok: false, error: "active_claim_not_found" }, { status: 404 });
        const wallet = cleanString(body.worker_wallet ?? body.wallet, 200).toLowerCase();
        const force = parseBoolUnknown(body.force, false);
        const now = nowMs();
        const timedOut = now > active.expires_at;
        if (!force && !timedOut && wallet !== active.worker_wallet) {
          return Response.json({ ok: false, error: "claim_owner_mismatch" }, { status: 403 });
        }
        const reason = cleanString(body.reason, 120) || (timedOut ? "timeout" : "manual_release");
        const releaseStatus = reason === "timeout" || timedOut ? "expired" : "released";
        await env.DB.prepare(
          `UPDATE task_market_claims
           SET status = ?, released_at = ?, release_reason = ?
           WHERE claim_id = ?`
        )
          .bind(releaseStatus, now, reason, active.claim_id)
          .run();
        const nextTaskStatus = task.deadline_at > now ? "open" : "expired";
        await env.DB.prepare(
          `UPDATE task_market_tasks
           SET status = ?, updated_at = ?
           WHERE task_id = ?`
        )
          .bind(nextTaskStatus, now, taskId)
          .run();
        await writeTaskAuditLog(env, {
          taskId,
          eventType: "task_released",
          actorWallet: wallet || active.worker_wallet,
          payload: { claim_id: active.claim_id, reason, status: releaseStatus },
        });
        const responseBody = {
          ok: true,
          task_id: taskId,
          released: true,
          claim_id: active.claim_id,
          status: releaseStatus,
          release_reason: reason,
        };
        if (idem.key) {
          await saveIdempotencyRecord(env, {
            key: idem.key,
            endpoint,
            requestHash: idem.requestHash,
            responseStatus: 200,
            responseBody,
          });
        }
        return Response.json(responseBody);
      }

      if (action === "evidence") {
        const wallet = cleanString(body.worker_wallet ?? body.wallet, 200).toLowerCase();
        if (!wallet) return Response.json({ ok: false, error: "worker_wallet_required" }, { status: 400 });
        const active = await loadActiveMarketplaceClaim(env, taskId);
        if (!active) return Response.json({ ok: false, error: "active_claim_not_found" }, { status: 404 });
        if (active.worker_wallet !== wallet) {
          return Response.json({ ok: false, error: "claim_owner_mismatch" }, { status: 403 });
        }
        const rawEvidence = parseJsonObject(body.evidence) ?? body;
        const evidence = normalizeEvidenceBundle(rawEvidence);
        if (
          evidence.urls.length < 1 &&
          Object.keys(evidence.fields).length < 1 &&
          evidence.screenshots.length < 1 &&
          !evidence.trace_ref
        ) {
          return Response.json({ ok: false, error: "evidence_payload_empty" }, { status: 400 });
        }
        const evidenceCanonical = canonicalJson(evidence);
        const evidenceHash = await sha256Hex(evidenceCanonical);
        const evidenceId = uuid("evidence");
        await env.DB.prepare(
          `INSERT INTO task_market_evidence (
            evidence_id, task_id, claim_id, worker_wallet, evidence_json, evidence_hash, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?)`
        )
          .bind(evidenceId, taskId, active.claim_id, wallet, evidenceCanonical, evidenceHash, nowMs())
          .run();
        await writeTaskAuditLog(env, {
          taskId,
          eventType: "evidence_submitted",
          actorWallet: wallet,
          payload: { evidence_id: evidenceId, evidence_hash: evidenceHash },
        });
        const responseBody = {
          ok: true,
          task_id: taskId,
          evidence_id: evidenceId,
          evidence_hash: evidenceHash,
        };
        if (idem.key) {
          await saveIdempotencyRecord(env, {
            key: idem.key,
            endpoint,
            requestHash: idem.requestHash,
            responseStatus: 200,
            responseBody,
          });
        }
        return Response.json(responseBody);
      }

      if (action === "verify") {
        const evidenceId = cleanString(body.evidence_id, 120);
        const evidenceRow = evidenceId
          ? await env.DB.prepare(
              `SELECT evidence_id, evidence_json, worker_wallet
               FROM task_market_evidence
               WHERE task_id = ? AND evidence_id = ?
               LIMIT 1`
            )
              .bind(taskId, evidenceId)
              .first<Record<string, unknown>>()
          : await env.DB.prepare(
              `SELECT evidence_id, evidence_json, worker_wallet
               FROM task_market_evidence
               WHERE task_id = ?
               ORDER BY created_at DESC
               LIMIT 1`
            )
              .bind(taskId)
              .first<Record<string, unknown>>();
        if (!evidenceRow) {
          return Response.json({ ok: false, error: "evidence_not_found" }, { status: 404 });
        }
        const spec = safeJsonParseObject(task.task_spec_json) ?? {};
        const normalizedSpec = normalizeTaskSpec(spec);
        const evidence = normalizeEvidenceBundle(safeJsonParseObject(cleanString(evidenceRow.evidence_json, 64000)) ?? {});
        const result = await evaluateEvidenceAgainstSpec(
          taskId,
          normalizedSpec,
          cleanString(evidenceRow.evidence_id, 120),
          evidence
        );
        const verificationResultHash = await sha256Hex(canonicalJson(result));
        const serverSignature = await signAgentPayload(env, `${taskId}.${verificationResultHash}`);
        if (!serverSignature) {
          return Response.json({ ok: false, error: "task_signing_secret_not_configured" }, { status: 503 });
        }
        const verificationId = uuid("verify");
        await env.DB.prepare(
          `INSERT INTO task_market_verifications (
            verification_id, task_id, evidence_id, passed, result_json, verification_result_hash, server_signature, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
          .bind(
            verificationId,
            taskId,
            cleanString(evidenceRow.evidence_id, 120),
            result.passed ? 1 : 0,
            canonicalJson(result),
            verificationResultHash,
            serverSignature,
            nowMs()
          )
          .run();
        await writeTaskAuditLog(env, {
          taskId,
          eventType: "verification_completed",
          actorWallet: cleanString(evidenceRow.worker_wallet, 200) || null,
          payload: { verification_id: verificationId, passed: result.passed, verification_result_hash: verificationResultHash },
        });
        const responseBody = {
          ok: true,
          task_id: taskId,
          verification_id: verificationId,
          result,
          verificationResultHash,
          serverSignature,
        };
        if (idem.key) {
          await saveIdempotencyRecord(env, {
            key: idem.key,
            endpoint,
            requestHash: idem.requestHash,
            responseStatus: 200,
            responseBody,
          });
        }
        return Response.json(responseBody);
      }

      if (action === "payout-authorize") {
        const wallet = cleanString(body.worker_wallet ?? body.wallet, 200).toLowerCase();
        if (!wallet) return Response.json({ ok: false, error: "worker_wallet_required" }, { status: 400 });
        const active = await loadActiveMarketplaceClaim(env, taskId);
        if (!active) return Response.json({ ok: false, error: "active_claim_not_found" }, { status: 404 });
        if (active.worker_wallet !== wallet) {
          return Response.json({ ok: false, error: "claim_owner_mismatch" }, { status: 403 });
        }
        const latestVerification = await env.DB.prepare(
          `SELECT verification_id, passed, verification_result_hash
           FROM task_market_verifications
           WHERE task_id = ?
           ORDER BY created_at DESC
           LIMIT 1`
        )
          .bind(taskId)
          .first<Record<string, unknown>>();
        if (!latestVerification || clampInt(latestVerification.passed, 0, 1, 0) !== 1) {
          return Response.json({ ok: false, error: "verification_not_passed" }, { status: 409 });
        }
        const serverSecret = getTaskSigningSecret(env);
        if (!serverSecret) {
          return Response.json({ ok: false, error: "task_signing_secret_not_configured" }, { status: 503 });
        }
        const authorizationId = uuid("payoutauth");
        const payload = {
          authorization_id: authorizationId,
          task_id: taskId,
          claim_id: active.claim_id,
          worker_wallet: wallet,
          payout: {
            amount: task.payout_amount,
            token: task.payout_token,
            chain: task.payout_chain,
          },
          verification_result_hash: cleanString(latestVerification.verification_result_hash, 200),
          authorized_at: nowMs(),
          expires_at: nowMs() + 15 * 60 * 1000,
          nonce: uuid("nonce"),
        };
        const payloadHash = await sha256Hex(canonicalJson(payload));
        const serverSignature = await hmacSha256Hex(serverSecret, payloadHash);
        await env.DB.prepare(
          `INSERT INTO task_market_payout_authorizations (
            authorization_id, task_id, claim_id, worker_wallet,
            payout_payload_json, payout_payload_hash, server_signature, status, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, 'authorized', ?)`
        )
          .bind(
            authorizationId,
            taskId,
            active.claim_id,
            wallet,
            canonicalJson(payload),
            payloadHash,
            serverSignature,
            nowMs()
          )
          .run();
        await writeTaskAuditLog(env, {
          taskId,
          eventType: "payout_authorized",
          actorWallet: wallet,
          payload: { authorization_id: authorizationId, payout_payload_hash: payloadHash },
        });
        const responseBody = {
          ok: true,
          task_id: taskId,
          authorization_id: authorizationId,
          payout_authorization: payload,
          payoutAuthorizationHash: payloadHash,
          serverSignature,
        };
        if (idem.key) {
          await saveIdempotencyRecord(env, {
            key: idem.key,
            endpoint,
            requestHash: idem.requestHash,
            responseStatus: 200,
            responseBody,
          });
        }
        return Response.json(responseBody);
      }

      return Response.json({ ok: false, error: "unsupported_task_action" }, { status: 400 });
    }

    const memoryUpsertSiteId = parseSiteIdFromPath(url.pathname, "/memory/upsert");
    if (req.method === "POST" && memoryUpsertSiteId) {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const site = await loadMemorySite(env, memoryUpsertSiteId);
      if (!site) return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });

      const parsed = parseMemoryUpsertBody(body);
      if ("error" in parsed) {
        return Response.json({ ok: false, error: parsed.error }, { status: 400 });
      }
      try {
        const saved = await upsertSemanticMemory(env, site.site_id, parsed);
        return Response.json({
          ok: true,
          site_id: site.site_id,
          memory_id: saved.memoryId,
          vector_id: saved.vectorId,
          r2_key: saved.r2Key,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await env.DB
          .prepare(
            `INSERT INTO memory_events (site_id, event_type, message, meta_json, created_at)
             VALUES (?, 'error', ?, ?, strftime('%s','now'))`
          )
          .bind(site.site_id, "memory upsert failed", canonicalJson({ error: message }))
          .run();
        const status = /not_configured|binding_missing/.test(message) ? 503 : 500;
        return Response.json({ ok: false, error: message }, { status });
      }
    }

    const memorySearchSiteId = parseSiteIdFromPath(url.pathname, "/memory/search");
    if (req.method === "GET" && memorySearchSiteId) {
      const site = await loadMemorySite(env, memorySearchSiteId);
      if (!site) return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      const q = cleanString(url.searchParams.get("q"), 3000);
      if (!q) return Response.json({ ok: false, error: "q_required" }, { status: 400 });
      const k = clampInt(url.searchParams.get("k"), 1, 20, 8);
      const days = clampInt(url.searchParams.get("days"), 1, 365, 30);
      const type = cleanString(url.searchParams.get("type"), 120);
      const geo = cleanString(url.searchParams.get("geo_key"), 120);
      try {
        const matches = await searchSemanticMemory(env, site.site_id, q, {
          k,
          type: type ? normalizeMemoryType(type) : null,
          geoKey: geo ? normalizeMemoryGeoKey(geo) : null,
          days,
        });
        return Response.json({
          ok: true,
          site_id: site.site_id,
          query: q,
          count: matches.length,
          matches,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        return Response.json({ ok: false, error: message }, { status: /not_configured|binding_missing/.test(message) ? 503 : 500 });
      }
    }

    if (req.method === "POST" && (url.pathname === `${pluginV1Prefix}/sites/upsert` || url.pathname === `${pluginV1Prefix}/sites/analyze`)) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const normalizedBody =
        url.pathname === `${pluginV1Prefix}/sites/upsert`
          ? normalizeSiteUpsertPayload(verified.body)
          : verified.body;
      const siteUrl = cleanString(normalizedBody.site_url, 2000);
      if (!siteUrl) {
        return Response.json({ ok: false, error: "site_url required." }, { status: 400 });
      }
      const saved = await upsertStep1Site(env, normalizedBody);
      const profile = safeJsonParseObject(saved.site_profile_json) ?? {};
      const savedInput = safeJsonParseObject(saved.input_json) ?? {};
      const plan = getSitePlanFromInput(savedInput);
      return Response.json({
        ok: true,
        site_brief_id: saved.site_id,
        site_id: saved.site_id,
        site_url: saved.site_url,
        site_name: saved.site_name,
        wp_site_id: cleanString(savedInput.wp_site_id, 120) || null,
        site_profile: profile,
        site_brief: parseJsonObject(profile.site_brief) ?? null,
        billing: {
          model: "per_site",
          entitlement: {
            tracked_keywords_cap: SITE_KEYWORD_CAP,
            tracked_keywords_in_use: SITE_KEYWORD_CAP,
          },
          limits: {
            serp_results_per_keyword_cap: SITE_SERP_RESULTS_CAP,
            url_fetches_per_day_cap: SITE_MAX_URL_FETCHES_PER_DAY,
            backlink_enrich_per_day_cap: SITE_MAX_BACKLINK_ENRICH_PER_DAY,
            competitor_graph_domains_per_day_cap: SITE_MAX_GRAPH_DOMAINS_PER_DAY,
          },
        },
        plugin_ui: {
          tracking_status: `Tracking ${SITE_KEYWORD_CAP} keywords`,
          update_cadence: "Daily competitive report updates every 24 hours",
          local_metro_tracking: plan.metro_proxy
            ? `Local metro tracking enabled: ${plan.metro ?? "configured"}`
            : "Local metro tracking disabled",
        },
        analyzed_at: saved.last_analysis_at,
      });
    }

    const pluginMemoryUpsertSiteId = parseSiteIdFromPrefixedPath(
      url.pathname,
      `${pluginV1Prefix}/sites/`,
      "/memory/upsert"
    );
    if (req.method === "POST" && pluginMemoryUpsertSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadMemorySite(env, pluginMemoryUpsertSiteId);
      if (!site) return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      const parsed = parseMemoryUpsertBody(verified.body);
      if ("error" in parsed) {
        return Response.json({ ok: false, error: parsed.error }, { status: 400 });
      }
      try {
        const saved = await upsertSemanticMemory(env, site.site_id, parsed);
        return Response.json({
          ok: true,
          site_id: site.site_id,
          memory_id: saved.memoryId,
          vector_id: saved.vectorId,
          r2_key: saved.r2Key,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await env.DB
          .prepare(
            `INSERT INTO memory_events (site_id, event_type, message, meta_json, created_at)
             VALUES (?, 'error', ?, ?, strftime('%s','now'))`
          )
          .bind(site.site_id, "memory upsert failed", canonicalJson({ error: message }))
          .run();
        const status = /not_configured|binding_missing/.test(message) ? 503 : 500;
        return Response.json({ ok: false, error: message }, { status });
      }
    }

    const pluginMemorySearchSiteId = parseSiteIdFromPrefixedPath(
      url.pathname,
      `${pluginV1Prefix}/sites/`,
      "/memory/search"
    );
    if (req.method === "POST" && pluginMemorySearchSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadMemorySite(env, pluginMemorySearchSiteId);
      if (!site) return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });

      const q = cleanString(verified.body.q, 3000);
      if (!q) return Response.json({ ok: false, error: "q_required" }, { status: 400 });
      const k = clampInt(verified.body.k, 1, 20, 8);
      const days = clampInt(verified.body.days, 1, 365, 30);
      const type = cleanString(verified.body.type, 120);
      const geo = cleanString(verified.body.geo_key, 120);
      try {
        const matches = await searchSemanticMemory(env, site.site_id, q, {
          k,
          type: type ? normalizeMemoryType(type) : null,
          geoKey: geo ? normalizeMemoryGeoKey(geo) : null,
          days,
        });
        return Response.json({
          ok: true,
          site_id: site.site_id,
          query: q,
          count: matches.length,
          matches,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        return Response.json({ ok: false, error: message }, { status: /not_configured|binding_missing/.test(message) ? 503 : 500 });
      }
    }

    const pluginSiteResearchSiteId = parseSiteIdFromPrefixedPath(
      url.pathname,
      `${pluginV1Prefix}/sites/`,
      "/keyword-research"
    );
    if (req.method === "POST" && pluginSiteResearchSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const siteId = pluginSiteResearchSiteId;
      const site = await loadStep1Site(env, siteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const jobId = await createJobRecord(env, {
        siteId,
        type: "keyword_research_step1",
        request: {
          site_id: siteId,
          semrush_keywords_count: Array.isArray(verified.body.semrush_keywords)
            ? verified.body.semrush_keywords.length
            : 0,
        },
      });
      try {
        const executed = await runStep1KeywordResearch(env, site, verified.body);
        await createArtifactRecord(env, {
          jobId,
          kind: "keyword_research.results",
          payload: executed.result,
        });
        await finalizeJobSuccess(env, jobId, 1);
        return Response.json({
          ok: true,
          job_id: jobId,
          site_id: siteId,
          research_run_id: executed.researchRunId,
          status: "succeeded",
          results_ready: true,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await finalizeJobFailure(env, jobId, "keyword_research_failed", { error: message });
        await createArtifactRecord(env, {
          jobId,
          kind: "keyword_research.error",
          payload: { error: message },
        });
        return Response.json({ ok: false, job_id: jobId, error: message }, { status: 500 });
      }
    }

    const pluginSiteStep2RunSiteId = parseSiteIdFromPrefixedPath(url.pathname, `${pluginV1Prefix}/sites/`, "/step2/run");
    if (req.method === "POST" && pluginSiteStep2RunSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadStep1Site(env, pluginSiteStep2RunSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const body = verified.body ?? {};
      const maxKeywords = clampInt(body.max_keywords, 1, SITE_KEYWORD_CAP, SITE_KEYWORD_CAP);
      const maxResults = clampInt(body.max_results, 1, SITE_SERP_RESULTS_CAP, SITE_SERP_RESULTS_CAP);
      const geo = cleanString(body.geo, 120) || "US";
      const runTypeRaw = cleanString(body.run_type, 20).toLowerCase();
      const runType: "baseline" | "delta" | "auto" =
        runTypeRaw === "baseline" || runTypeRaw === "delta" ? runTypeRaw : "auto";
      const parentJobId = await createJobRecord(env, {
        siteId: site.site_id,
        type: "site_daily_run",
        request: {
          site_id: site.site_id,
          max_keywords: maxKeywords,
          max_results: maxResults,
          geo,
          run_type: runType,
        },
      });
      try {
        const summary = await runStep2DailyHarvest(env, site, {
          maxKeywords,
          maxResults,
          geo,
          parentJobId,
          runType,
        });
        let mozSummary: Record<string, unknown> | null = null;
        try {
          mozSummary = await runMozProfileForSite(env, {
            site,
            day: toDateYYYYMMDD(nowMs()),
            isWeekly: new Date().getUTCDay() === 1,
            trigger: "step2",
          });
          await createArtifactRecord(env, {
            jobId: parentJobId,
            kind: "moz.profile.summary",
            payload: mozSummary,
          });
        } catch (mozError) {
          await createArtifactRecord(env, {
            jobId: parentJobId,
            kind: "moz.profile.error",
            payload: {
              error: String((mozError as Error)?.message ?? mozError),
            },
          });
        }
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step2.harvest.summary",
          payload: summary,
        });
        await finalizeJobSuccess(env, parentJobId, Math.max(1, summary.keyword_count));
        return Response.json({
          ok: true,
          job_id: parentJobId,
          run_type: "site_daily_run",
          summary,
          moz_summary: mozSummary,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await finalizeJobFailure(env, parentJobId, "step2_harvest_failed", { error: message });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step2.harvest.error",
          payload: { error: message },
        });
        return Response.json({ ok: false, job_id: parentJobId, error: message }, { status: 500 });
      }
    }

    const pluginSiteStep3PlanSiteId = parseSiteIdFromPrefixedPath(url.pathname, `${pluginV1Prefix}/sites/`, "/step3/plan");
    if (req.method === "POST" && pluginSiteStep3PlanSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadStep1Site(env, pluginSiteStep3PlanSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const requestedStep2Date = cleanString(verified.body.step2_date, 20) || null;
      const parentJobId = await createJobRecord(env, {
        siteId: site.site_id,
        type: "step3_local_execution_plan",
        request: {
          site_id: site.site_id,
          step2_date: requestedStep2Date,
        },
      });
      try {
        const summary = await runStep3LocalExecutionPlan(env, site, {
          requestedStep2Date,
          parentJobId,
        });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step3.plan.summary",
          payload: summary,
        });
        await finalizeJobSuccess(env, parentJobId, Math.max(1, clampInt(summary.tasks_total, 1, 500, 1)));
        return Response.json({
          ok: true,
          job_id: parentJobId,
          site_id: site.site_id,
          summary,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await finalizeJobFailure(env, parentJobId, "step3_plan_failed", { error: message });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step3.plan.error",
          payload: { error: message },
        });
        return Response.json({ ok: false, job_id: parentJobId, error: message }, { status: 500 });
      }
    }

    const pluginTaskBoardSiteId = parseSiteIdFromPrefixedPath(url.pathname, `${pluginV1Prefix}/sites/`, "/tasks/board");
    if (req.method === "POST" && pluginTaskBoardSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadStep1Site(env, pluginTaskBoardSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const requestedRunId = cleanString(verified.body.site_run_id, 120) || null;
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id, runId: requestedRunId });
      if (!runMeta) {
        return Response.json({
          ok: true,
          ...buildStep3TaskBoardPayload({
            site,
            runId: requestedRunId,
            runDate: null,
            source: requestedRunId ? "run" : "latest",
            tasks: [],
          }),
        });
      }
      const rows = await loadStep3TasksForRun(env, runMeta.run_id);
      const tasks = rows.map((row) => parseTaskV1FromStored(row, site.site_id, runMeta.run_id));
      return Response.json({
        ok: true,
        ...buildStep3TaskBoardPayload({
          site,
          runId: runMeta.run_id,
          runDate: runMeta.date_yyyymmdd,
          source: requestedRunId ? "run" : "latest",
          tasks,
        }),
      });
    }

    const pluginStep3TasksSiteId = parseSiteIdFromPrefixedPath(url.pathname, `${pluginV1Prefix}/sites/`, "/step3/tasks");
    if (req.method === "POST" && pluginStep3TasksSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadStep1Site(env, pluginStep3TasksSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const runIdFilter = cleanString(verified.body.site_run_id, 120) || null;
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id, runId: runIdFilter });
      if (!runMeta) {
        return Response.json({
          ok: true,
          site_id: site.site_id,
          site_run_id: runIdFilter,
          run_date: null,
          filters_applied: {
            execution_mode: [],
            task_group: [],
            status: [],
          },
          count: 0,
          tasks: [],
          task_details: { by_id: {} },
        });
      }
      const executionModeFilters = parseStringListInput(verified.body.execution_mode, 30, 30)
        .map(mapQueryExecutionMode)
        .filter((v): v is string => !!v);
      const taskGroupFilters = parseStringListInput(verified.body.task_group, 40, 40)
        .map((v) => cleanString(v, 40).toUpperCase())
        .filter(Boolean);
      const statusFilters = parseStringListInput(verified.body.status, 20, 40)
        .map(mapQueryStatusToDb)
        .filter((v): v is string => !!v);
      const rows = await loadStep3TasksFiltered(env, {
        siteId: site.site_id,
        runId: runMeta.run_id,
        executionModes: executionModeFilters,
        taskGroups: taskGroupFilters,
        statuses: statusFilters,
      });
      const tasks = rows.map((row) => parseTaskV1FromStored(row, site.site_id, runMeta.run_id));
      const cards = tasks.map(mapTaskToCard);
      return Response.json({
        ok: true,
        site_id: site.site_id,
        site_run_id: runMeta.run_id,
        run_date: runMeta.date_yyyymmdd,
        filters_applied: {
          execution_mode: executionModeFilters,
          task_group: taskGroupFilters,
          status: statusFilters,
        },
        count: cards.length,
        tasks: cards,
        task_details: {
          by_id: Object.fromEntries(tasks.map((task) => [task.task_id, task])),
        },
      });
    }

    const pluginTaskDetailMatch = url.pathname.match(/^\/plugin\/wp\/v1\/sites\/([^/]+)\/tasks\/([^/]+)$/);
    if (pluginTaskDetailMatch && req.method === "POST") {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const siteId = cleanString(decodeURIComponent(pluginTaskDetailMatch[1]), 120);
      const taskId = cleanString(decodeURIComponent(pluginTaskDetailMatch[2]), 120);
      if (!siteId || !taskId) {
        return Response.json({ ok: false, error: "invalid_site_or_task_id" }, { status: 400 });
      }
      const site = await loadStep1Site(env, siteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const targetStatus = normalizeTaskStatusInput(verified.body.status);
      if (!targetStatus) {
        return Response.json(
          {
            ok: false,
            error: "status_required",
            allowed_statuses: ["READY", "IN_PROGRESS", "DONE", "BLOCKED"],
          },
          { status: 400 }
        );
      }
      const blockers = parseBlockersFromBody(verified.body);
      const updated = await updateStep3TaskStatus(env, {
        siteId: site.site_id,
        taskId,
        targetStatus,
        blockers,
      });
      if (updated.ok === false) {
        return Response.json(
          { ok: false, error: updated.error, current_status: updated.current ?? null },
          { status: updated.status }
        );
      }
      return Response.json({ ok: true, task: updated.task });
    }

    const pluginSiteTasksBulkSiteId = parseSiteIdFromPrefixedPath(url.pathname, `${pluginV1Prefix}/sites/`, "/tasks/bulk");
    if (req.method === "POST" && pluginSiteTasksBulkSiteId) {
      const verified = await verifySignedPluginRequest(req, env);
      if (verified.ok === false) {
        return verified.response;
      }
      const site = await loadStep1Site(env, pluginSiteTasksBulkSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const actionRaw = cleanString(verified.body.action, 60).toUpperCase();
      if (actionRaw !== "AUTO_APPLY_READY" && actionRaw !== "MARK_DONE_SELECTED") {
        return Response.json(
          {
            ok: false,
            error: "invalid_action",
            allowed_actions: ["AUTO_APPLY_READY", "MARK_DONE_SELECTED"],
          },
          { status: 400 }
        );
      }
      const runIdRequested = cleanString(verified.body.site_run_id, 120) || null;
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id, runId: runIdRequested });
      if (!runMeta) {
        return Response.json({ ok: false, error: "run_not_found" }, { status: 404 });
      }
      const taskIds = parseStringListInput(verified.body.task_ids, 300, 120);
      if (actionRaw === "MARK_DONE_SELECTED" && taskIds.length < 1) {
        return Response.json(
          { ok: false, error: "task_ids_required_for_mark_done_selected" },
          { status: 400 }
        );
      }
      const result = await runStep3BulkAction(env, {
        siteId: site.site_id,
        runId: runMeta.run_id,
        action: actionRaw,
        taskIds,
      });
      return Response.json({ ok: true, ...result });
    }

    if (req.method === "POST" && (url.pathname === "/v1/sites/upsert" || url.pathname === "/v1/sites/analyze")) {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }
      const normalizedBody = url.pathname === "/v1/sites/upsert" ? normalizeSiteUpsertPayload(body) : body;
      const siteUrl = cleanString(normalizedBody.site_url, 2000);
      if (!siteUrl) {
        return Response.json({ ok: false, error: "site_url required." }, { status: 400 });
      }
      const saved = await upsertStep1Site(env, normalizedBody);
      const profile = safeJsonParseObject(saved.site_profile_json) ?? {};
      const savedInput = safeJsonParseObject(saved.input_json) ?? {};
      const plan = getSitePlanFromInput(savedInput);
      return Response.json({
        ok: true,
        site_brief_id: saved.site_id,
        site_id: saved.site_id,
        site_url: saved.site_url,
        site_name: saved.site_name,
        wp_site_id: cleanString(savedInput.wp_site_id, 120) || null,
        site_profile: profile,
        site_brief: parseJsonObject(profile.site_brief) ?? null,
        billing: {
          model: "per_site",
          entitlement: {
            tracked_keywords_cap: SITE_KEYWORD_CAP,
            tracked_keywords_in_use: SITE_KEYWORD_CAP,
          },
          limits: {
            serp_results_per_keyword_cap: SITE_SERP_RESULTS_CAP,
            url_fetches_per_day_cap: SITE_MAX_URL_FETCHES_PER_DAY,
            backlink_enrich_per_day_cap: SITE_MAX_BACKLINK_ENRICH_PER_DAY,
            competitor_graph_domains_per_day_cap: SITE_MAX_GRAPH_DOMAINS_PER_DAY,
          },
        },
        plugin_ui: {
          tracking_status: `Tracking ${SITE_KEYWORD_CAP} keywords`,
          update_cadence: "Daily competitive report updates every 24 hours",
          local_metro_tracking: plan.metro_proxy
            ? `Local metro tracking enabled: ${plan.metro ?? "configured"}`
            : "Local metro tracking disabled",
        },
        analyzed_at: saved.last_analysis_at,
      });
    }

    const siteResearchPathSiteId = parseSiteIdFromPath(url.pathname, "/keyword-research");
    if (req.method === "POST" && siteResearchPathSiteId) {
      const siteId = siteResearchPathSiteId;
      const site = await loadStep1Site(env, siteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }

      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }

      const jobId = await createJobRecord(env, {
        siteId,
        type: "keyword_research_step1",
        request: {
          site_id: siteId,
          semrush_keywords_count: Array.isArray(body?.semrush_keywords) ? body?.semrush_keywords.length : 0,
        },
      });

      try {
        const executed = await runStep1KeywordResearch(env, site, body);
        await createArtifactRecord(env, {
          jobId,
          kind: "keyword_research.results",
          payload: executed.result,
        });
        await finalizeJobSuccess(env, jobId, 1);
        return Response.json({
          ok: true,
          job_id: jobId,
          site_id: siteId,
          research_run_id: executed.researchRunId,
          status: "succeeded",
          results_ready: true,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await finalizeJobFailure(env, jobId, "keyword_research_failed", { error: message });
        await createArtifactRecord(env, {
          jobId,
          kind: "keyword_research.error",
          payload: { error: message },
        });
        return Response.json({ ok: false, job_id: jobId, error: message }, { status: 500 });
      }
    }

    const siteResultPathSiteId = parseSiteIdFromPath(url.pathname, "/keyword-research/results");
    if (req.method === "GET" && siteResultPathSiteId) {
      const siteId = siteResultPathSiteId;
      const result = await loadLatestStep1KeywordResearchResults(env, siteId);
      if (!result) {
        return Response.json({ ok: false, error: "keyword_research_results_not_found" }, { status: 404 });
      }
      return Response.json({
        ok: true,
        site_id: siteId,
        ...result,
      });
    }

    const siteStep2RunSiteId = parseSiteIdFromPath(url.pathname, "/step2/run");
    const siteDailyRunSiteId = parseSiteIdFromPath(url.pathname, "/daily-run");
    const runSiteId = siteStep2RunSiteId || siteDailyRunSiteId;
    if (req.method === "POST" && runSiteId) {
      const site = await loadStep1Site(env, runSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      body = body ?? {};
      const maxKeywords = clampInt(body.max_keywords, 1, SITE_KEYWORD_CAP, SITE_KEYWORD_CAP);
      const maxResults = clampInt(body.max_results, 1, SITE_SERP_RESULTS_CAP, SITE_SERP_RESULTS_CAP);
      const geo = cleanString(body.geo, 120) || "US";
      const runTypeRaw = cleanString(body.run_type, 20).toLowerCase();
      const runType: "baseline" | "delta" | "auto" =
        runTypeRaw === "baseline" || runTypeRaw === "delta" ? runTypeRaw : "auto";

      const parentJobId = await createJobRecord(env, {
        siteId: site.site_id,
        type: "site_daily_run",
        request: {
          site_id: site.site_id,
          max_keywords: maxKeywords,
          max_results: maxResults,
          geo,
          run_type: runType,
        },
      });
      try {
        const summary = await runStep2DailyHarvest(env, site, {
          maxKeywords,
          maxResults,
          geo,
          parentJobId,
          runType,
        });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step2.harvest.summary",
          payload: summary,
        });
        await finalizeJobSuccess(env, parentJobId, Math.max(1, summary.keyword_count));
        return Response.json({
          ok: true,
          job_id: parentJobId,
          run_type: "site_daily_run",
          summary,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await finalizeJobFailure(env, parentJobId, "step2_harvest_failed", { error: message });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step2.harvest.error",
          payload: { error: message },
        });
        return Response.json({ ok: false, job_id: parentJobId, error: message }, { status: 500 });
      }
    }

    const siteStep2ReportSiteId = parseSiteIdFromPath(url.pathname, "/step2/report");
    if (req.method === "GET" && siteStep2ReportSiteId) {
      const date = cleanString(url.searchParams.get("date"), 10);
      const report = await loadLatestStep2Report(env, siteStep2ReportSiteId, date);
      return Response.json({ ok: true, report });
    }

    const siteStep3PlanSiteId = parseSiteIdFromPath(url.pathname, "/step3/plan");
    if (req.method === "POST" && siteStep3PlanSiteId) {
      const site = await loadStep1Site(env, siteStep3PlanSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      const requestedStep2Date = cleanString(body?.step2_date, 20) || null;
      const parentJobId = await createJobRecord(env, {
        siteId: site.site_id,
        type: "step3_local_execution_plan",
        request: {
          site_id: site.site_id,
          step2_date: requestedStep2Date,
        },
      });
      try {
        const summary = await runStep3LocalExecutionPlan(env, site, {
          requestedStep2Date,
          parentJobId,
        });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step3.plan.summary",
          payload: summary,
        });
        await finalizeJobSuccess(env, parentJobId, Math.max(1, clampInt(summary.tasks_total, 1, 500, 1)));
        return Response.json({
          ok: true,
          job_id: parentJobId,
          site_id: site.site_id,
          summary,
        });
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        await finalizeJobFailure(env, parentJobId, "step3_plan_failed", { error: message });
        await createArtifactRecord(env, {
          jobId: parentJobId,
          kind: "step3.plan.error",
          payload: { error: message },
        });
        return Response.json({ ok: false, job_id: parentJobId, error: message }, { status: 500 });
      }
    }

    const siteStep3ReportSiteId = parseSiteIdFromPath(url.pathname, "/step3/report");
    if (req.method === "GET" && siteStep3ReportSiteId) {
      const date = cleanString(url.searchParams.get("date"), 10);
      const report = await loadLatestStep3Report(env, siteStep3ReportSiteId, date);
      return Response.json({ ok: true, ...report });
    }

    const siteStep3TasksSiteId = parseSiteIdFromPath(url.pathname, "/step3/tasks");
    if (req.method === "GET" && siteStep3TasksSiteId) {
      const site = await loadStep1Site(env, siteStep3TasksSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const runIdFilter = cleanString(url.searchParams.get("site_run_id"), 120) || null;
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id, runId: runIdFilter });
      if (!runMeta) {
        return Response.json({
          ok: true,
          site_id: site.site_id,
          site_run_id: runIdFilter,
          run_date: null,
          filters_applied: {
            execution_mode: [],
            task_group: [],
            status: [],
          },
          count: 0,
          tasks: [],
          task_details: { by_id: {} },
        });
      }

      const executionModeFilters = parseCsvQueryValues(url, "execution_mode")
        .map(mapQueryExecutionMode)
        .filter((v): v is string => !!v);
      const taskGroupFilters = parseCsvQueryValues(url, "task_group")
        .map((v) => cleanString(v, 40).toUpperCase())
        .filter(Boolean);
      const statusFilters = parseCsvQueryValues(url, "status")
        .map(mapQueryStatusToDb)
        .filter((v): v is string => !!v);

      const rows = await loadStep3TasksFiltered(env, {
        siteId: site.site_id,
        runId: runMeta.run_id,
        executionModes: executionModeFilters,
        taskGroups: taskGroupFilters,
        statuses: statusFilters,
      });
      const tasks = rows.map((row) => parseTaskV1FromStored(row, site.site_id, runMeta.run_id));
      const cards = tasks.map(mapTaskToCard);
      return Response.json({
        ok: true,
        site_id: site.site_id,
        site_run_id: runMeta.run_id,
        run_date: runMeta.date_yyyymmdd,
        filters_applied: {
          execution_mode: executionModeFilters,
          task_group: taskGroupFilters,
          status: statusFilters,
        },
        count: cards.length,
        tasks: cards,
        task_details: {
          by_id: Object.fromEntries(tasks.map((task) => [task.task_id, task])),
        },
      });
    }

    const siteTasksBoardSiteId = parseSiteIdFromPath(url.pathname, "/tasks/board");
    if (req.method === "GET" && siteTasksBoardSiteId) {
      const site = await loadStep1Site(env, siteTasksBoardSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id });
      if (!runMeta) {
        return Response.json({
          ok: true,
          ...buildStep3TaskBoardPayload({
            site,
            runId: null,
            runDate: null,
            source: "latest",
            tasks: [],
          }),
        });
      }
      const rows = await loadStep3TasksForRun(env, runMeta.run_id);
      const tasks = rows.map((row) => parseTaskV1FromStored(row, site.site_id, runMeta.run_id));
      return Response.json({
        ok: true,
        ...buildStep3TaskBoardPayload({
          site,
          runId: runMeta.run_id,
          runDate: runMeta.date_yyyymmdd,
          source: "latest",
          tasks,
        }),
      });
    }

    const runTasksBoardMatch = url.pathname.match(/^\/v1\/sites\/([^/]+)\/runs\/([^/]+)\/tasks\/board$/);
    if (req.method === "GET" && runTasksBoardMatch) {
      const siteId = cleanString(decodeURIComponent(runTasksBoardMatch[1]), 120);
      const runId = cleanString(decodeURIComponent(runTasksBoardMatch[2]), 120);
      const site = await loadStep1Site(env, siteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id, runId });
      if (!runMeta) {
        return Response.json({ ok: false, error: "run_not_found" }, { status: 404 });
      }
      const rows = await loadStep3TasksForRun(env, runMeta.run_id);
      const tasks = rows.map((row) => parseTaskV1FromStored(row, site.site_id, runMeta.run_id));
      return Response.json({
        ok: true,
        ...buildStep3TaskBoardPayload({
          site,
          runId: runMeta.run_id,
          runDate: runMeta.date_yyyymmdd,
          source: "run",
          tasks,
        }),
      });
    }

    const taskDetailMatch = url.pathname.match(/^\/v1\/sites\/([^/]+)\/tasks\/([^/]+)$/);
    if (taskDetailMatch && req.method === "PATCH") {
      const siteId = cleanString(decodeURIComponent(taskDetailMatch[1]), 120);
      const taskId = cleanString(decodeURIComponent(taskDetailMatch[2]), 120);
      if (!siteId || !taskId) {
        return Response.json({ ok: false, error: "invalid_site_or_task_id" }, { status: 400 });
      }
      const site = await loadStep1Site(env, siteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      const targetStatus = normalizeTaskStatusInput(body?.status);
      if (!targetStatus) {
        return Response.json(
          {
            ok: false,
            error: "status_required",
            allowed_statuses: ["READY", "IN_PROGRESS", "DONE", "BLOCKED"],
          },
          { status: 400 }
        );
      }
      const blockers = parseBlockersFromBody(body);
      const updated = await updateStep3TaskStatus(env, {
        siteId: site.site_id,
        taskId,
        targetStatus,
        blockers,
      });
      if (!updated.ok) {
        return Response.json(
          { ok: false, error: updated.error, current_status: updated.current ?? null },
          { status: updated.status }
        );
      }
      return Response.json({ ok: true, task: updated.task });
    }

    if (req.method === "GET" && taskDetailMatch) {
      const siteId = cleanString(decodeURIComponent(taskDetailMatch[1]), 120);
      const taskId = cleanString(decodeURIComponent(taskDetailMatch[2]), 120);
      if (!siteId || !taskId) {
        return Response.json({ ok: false, error: "invalid_site_or_task_id" }, { status: 400 });
      }
      const site = await loadStep1Site(env, siteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const task = await loadStep3TaskById(env, { siteId: site.site_id, taskId });
      if (!task) {
        return Response.json({ ok: false, error: "task_not_found" }, { status: 404 });
      }
      return Response.json({ ok: true, task });
    }

    const siteTasksBulkSiteId = parseSiteIdFromPath(url.pathname, "/tasks/bulk");
    if (req.method === "POST" && siteTasksBulkSiteId) {
      const site = await loadStep1Site(env, siteTasksBulkSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
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
      const actionRaw = cleanString(body.action, 60).toUpperCase();
      if (actionRaw !== "AUTO_APPLY_READY" && actionRaw !== "MARK_DONE_SELECTED") {
        return Response.json(
          {
            ok: false,
            error: "invalid_action",
            allowed_actions: ["AUTO_APPLY_READY", "MARK_DONE_SELECTED"],
          },
          { status: 400 }
        );
      }
      const runIdRequested = cleanString(body.site_run_id, 120) || null;
      const runMeta = await loadStep3RunMeta(env, { siteId: site.site_id, runId: runIdRequested });
      if (!runMeta) {
        return Response.json({ ok: false, error: "run_not_found" }, { status: 404 });
      }
      const taskIds = parseStringArray(body.task_ids, 300, 120);
      if (actionRaw === "MARK_DONE_SELECTED" && taskIds.length < 1) {
        return Response.json(
          {
            ok: false,
            error: "task_ids_required_for_mark_done_selected",
          },
          { status: 400 }
        );
      }
      const result = await runStep3BulkAction(env, {
        siteId: site.site_id,
        runId: runMeta.run_id,
        action: actionRaw,
        taskIds,
      });
      return Response.json({ ok: true, ...result });
    }

    const v1JobMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)$/);
    if (req.method === "GET" && v1JobMatch) {
      const jobId = cleanString(decodeURIComponent(v1JobMatch[1]), 120);
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
      const siteIdHint = cleanString(body.site_id, 120) || null;
      const siteProfile =
        siteIdHint && (await loadStep1Site(env, siteIdHint))
          ? await loadSiteProviderProfile(env, siteIdHint)
          : null;
      const serpProvider = resolveSerpPrimary(
        env,
        siteProfile?.serp_provider ?? (cleanString(body.serp_provider, 40) ? normalizeSerpProvider(body.serp_provider) : null)
      );
      const geoProvider = siteProfile?.geo_provider ?? normalizeGeoProvider(body.geo_provider);
      const geoMapped = mapGeoForProvider(
        cleanString(body.geo_label, 120) || cleanString(region.city ?? region.region, 120) || "us",
        region,
        geoProvider
      );

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
          site_id: siteIdHint,
          serp_provider: serpProvider,
          geo_provider: geoProvider,
          proxy_lease_id: proxyLeaseId,
        },
      });

      const collected = await collectAndPersistSerpTop20(env, {
        userId,
        phrase,
        region: geoMapped.region,
        device,
        maxResults,
        proxyUrl: activeLease?.proxy_url ?? null,
        provider: serpProvider,
        geoProvider,
        geoLabel: geoMapped.decodoGeoLabel ?? (cleanString(body.geo_label, 120) || null),
        auditJobId: jobId,
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
        extractor_mode: collected.extractorMode,
        fallback_reason: collected.fallbackReason,
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

    const siteMozBudgetSiteId = parseSiteIdFromPath(url.pathname, "/moz/budget");
    if (req.method === "GET" && siteMozBudgetSiteId) {
      const site = await loadStep1Site(env, siteMozBudgetSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const estimate = estimateMozRowBudget({
        keywords: clampInt(url.searchParams.get("keywords"), 1, 500, SITE_KEYWORD_CAP),
        daily_new_entrants: clampInt(url.searchParams.get("daily_new_entrants"), 0, 5000, 0),
        daily_top_movers: clampInt(url.searchParams.get("daily_top_movers"), 0, 5000, 0),
        baseline_mode:
          cleanString(url.searchParams.get("baseline_mode"), 20).toLowerCase() === "fuller"
            ? "fuller"
            : "light",
        cost_per_1k_rows_usd: Number(url.searchParams.get("cost_per_1k_rows_usd") || MOZ_DEFAULT_COST_PER_1K_ROWS_USD),
      });
      const profile = await loadMozSiteProfile(env, site.site_id);
      const isWeekly = new Date().getUTCDay() === 1;
      const plan = await buildMozExecutionPlan(env, {
        siteId: site.site_id,
        day: toDateYYYYMMDD(nowMs()),
        profile,
        isWeekly,
      });
      const usageBreakdown = await loadMozMonthlyUsageBreakdown(env, site.site_id, toDateYYYYMMDD(nowMs()).slice(0, 7));
      return Response.json({ ok: true, site_id: site.site_id, profile, estimate, plan, month_usage_breakdown: usageBreakdown });
    }

    const siteProvidersSiteId = parseSiteIdFromPath(url.pathname, "/providers");
    if (req.method === "GET" && siteProvidersSiteId) {
      const site = await loadStep1Site(env, siteProvidersSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const providers = await loadSiteProviderProfile(env, site.site_id);
      return Response.json({ ok: true, site_id: site.site_id, providers });
    }
    if (req.method === "POST" && siteProvidersSiteId) {
      const site = await loadStep1Site(env, siteProvidersSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const providers = await upsertSiteProviderProfile(env, {
        siteId: site.site_id,
        serpProvider: normalizeSerpProvider(body.serp_provider),
        pageProvider: normalizePageProvider(body.page_provider),
        geoProvider: normalizeGeoProvider(body.geo_provider),
      });
      return Response.json({ ok: true, site_id: site.site_id, providers });
    }

    const siteMozProfileSiteId = parseSiteIdFromPath(url.pathname, "/moz/profile");
    if (req.method === "GET" && siteMozProfileSiteId) {
      const site = await loadStep1Site(env, siteMozProfileSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      const profile = await loadMozSiteProfile(env, site.site_id);
      return Response.json({ ok: true, site_id: site.site_id, profile });
    }

    if (req.method === "POST" && siteMozProfileSiteId) {
      const site = await loadStep1Site(env, siteMozProfileSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const mozProfileRaw = cleanString(body.moz_profile, 40).toLowerCase();
      const mozProfile: MozProfileName = mozProfileRaw === "scalable_delta" ? "scalable_delta" : "single_site_max";
      const profile = await upsertMozSiteProfile(env, {
        siteId: site.site_id,
        mozProfile,
        monthlyRowsBudget: clampInt(body.moz_monthly_rows_budget ?? body.monthly_rows_budget, 0, 100_000_000, 15_000),
        weeklyFocusUrlCount: clampInt(body.weekly_focus_url_count, 1, 500, 20),
        dailyKeywordDepth: clampInt(body.daily_keyword_depth, 1, 20, 20),
      });
      return Response.json({ ok: true, site_id: site.site_id, profile });
    }

    const siteMozRunSiteId = parseSiteIdFromPath(url.pathname, "/moz/run");
    if (req.method === "POST" && siteMozRunSiteId) {
      const site = await loadStep1Site(env, siteMozRunSiteId);
      if (!site) {
        return Response.json({ ok: false, error: "site_not_found" }, { status: 404 });
      }
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      body = body ?? {};
      const day = normalizeDayString(body.day);
      const isWeekly = parseBoolUnknown(body.is_weekly, new Date().getUTCDay() === 1);
      const summary = await runMozProfileForSite(env, {
        site,
        day,
        isWeekly,
        trigger: "manual",
      });
      return Response.json({ ok: true, summary });
    }

    if (req.method === "POST" && url.pathname === "/moz/url-metrics") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const rows = Array.isArray(body.rows) ? body.rows : [];
      if (rows.length < 1) {
        return Response.json({ ok: false, error: "rows_required" }, { status: 400 });
      }
      const userId = cleanString(body.user_id, 120) || null;
      const siteId = cleanString(body.site_id, 120) || null;
      const geoKey = parseMozGeoKey(body.geo_key);
      const collectedDay = normalizeDayString(body.collected_day);
      const siteRunId = cleanString(body.site_run_id, 120) || null;
      const jobId = await createJobRecord(env, {
        userId,
        siteId,
        type: "moz_url_metrics",
        request: { row_count: rows.length, geo_key: geoKey, collected_day: collectedDay },
      });
      let stored = 0;
      for (const raw of rows) {
        const row = parseJsonObject(raw);
        if (!row) continue;
        const urlId = await getOrCreateUrlId(env, cleanString(row.url, 2000));
        if (!urlId) continue;
        await env.DB.prepare(
          `INSERT INTO moz_url_metrics_snapshots (
            snapshot_id, url_id, collected_day, geo_key,
            page_authority, domain_authority, spam_score, linking_domains, external_links,
            metrics_json, rows_used, site_run_id, job_id, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(url_id, collected_day, geo_key) DO UPDATE SET
            page_authority = excluded.page_authority,
            domain_authority = excluded.domain_authority,
            spam_score = excluded.spam_score,
            linking_domains = excluded.linking_domains,
            external_links = excluded.external_links,
            metrics_json = excluded.metrics_json,
            rows_used = excluded.rows_used,
            site_run_id = excluded.site_run_id,
            job_id = excluded.job_id,
            created_at = excluded.created_at`
        )
          .bind(
            uuid("mozurl"),
            urlId,
            collectedDay,
            geoKey,
            Number(row.page_authority ?? row.pa ?? 0) || null,
            Number(row.domain_authority ?? row.da ?? 0) || null,
            Number(row.spam_score ?? row.spam ?? 0) || null,
            clampInt(row.linking_domains, 0, 100000000, 0) || null,
            clampInt(row.external_links, 0, 100000000, 0) || null,
            safeJsonStringify(row, 32000),
            1,
            siteRunId,
            jobId,
            Math.floor(nowMs() / 1000)
          )
          .run();
        stored += 1;
      }
      await createArtifactRecord(env, {
        jobId,
        kind: "moz.url_metrics.raw",
        payload: { request: body, stored_rows: stored },
      });
      let usageSiteId: string | null = null;
      let usageProfile: MozProfileName | null = null;
      if (siteId) {
        const usageSite = await loadStep1Site(env, siteId);
        if (usageSite) {
          usageSiteId = usageSite.site_id;
          usageProfile = (await loadMozSiteProfile(env, usageSite.site_id)).moz_profile;
        }
      }
      await recordMozJobUsage(env, {
        siteId: usageSiteId,
        day: collectedDay,
        jobId,
        jobType: "moz_url_metrics",
        rowsUsed: stored,
        degradedMode: parseBoolUnknown(body.degraded_mode, false),
        fallbackReason: cleanString(body.fallback_reason, 200) || null,
        profile: usageProfile,
      });
      await finalizeJobSuccess(env, jobId, Math.max(1, stored));
      return Response.json({ ok: true, job_id: jobId, stored_rows: stored, collected_day: collectedDay, geo_key: geoKey });
    }

    if (req.method === "POST" && url.pathname === "/moz/anchor-text") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const targetUrl = cleanString(body.target_url, 2000);
      if (!targetUrl) return Response.json({ ok: false, error: "target_url_required" }, { status: 400 });
      const urlId = await getOrCreateUrlId(env, targetUrl);
      if (!urlId) return Response.json({ ok: false, error: "invalid_target_url" }, { status: 400 });
      const anchors = parseStringArray(body.top_anchors, 200, 400);
      const geoKey = parseMozGeoKey(body.geo_key);
      const jobId = await createJobRecord(env, {
        userId: cleanString(body.user_id, 120) || null,
        siteId: cleanString(body.site_id, 120) || null,
        type: "moz_anchor_text",
        request: { target_url: targetUrl, anchor_count: anchors.length, geo_key: geoKey },
      });
      const collectedDay = normalizeDayString(body.collected_day);
      const siteRunId = cleanString(body.site_run_id, 120) || null;
      await env.DB.prepare(
        `INSERT INTO moz_anchor_text_snapshots (
          snapshot_id, target_url_id, collected_day, geo_key, top_anchors_json, total_anchor_rows,
          totals_json, rows_used, site_run_id, job_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(target_url_id, collected_day, geo_key) DO UPDATE SET
          top_anchors_json = excluded.top_anchors_json,
          total_anchor_rows = excluded.total_anchor_rows,
          totals_json = excluded.totals_json,
          rows_used = excluded.rows_used,
          site_run_id = excluded.site_run_id,
          job_id = excluded.job_id,
          created_at = excluded.created_at`
      )
        .bind(
          uuid("mozanch"),
          urlId,
          collectedDay,
          geoKey,
          safeJsonStringify(anchors, 32000),
          anchors.length,
          safeJsonStringify(parseJsonObject(body.totals) ?? {}, 8000),
          anchors.length,
          siteRunId,
          jobId,
          Math.floor(nowMs() / 1000)
        )
        .run();
      await createArtifactRecord(env, {
        jobId,
        kind: "moz.anchor_text.raw",
        payload: body,
      });
      let usageSiteId: string | null = null;
      let usageProfile: MozProfileName | null = null;
      const bodySiteId = cleanString(body.site_id, 120);
      if (bodySiteId) {
        const usageSite = await loadStep1Site(env, bodySiteId);
        if (usageSite) {
          usageSiteId = usageSite.site_id;
          usageProfile = (await loadMozSiteProfile(env, usageSite.site_id)).moz_profile;
        }
      }
      await recordMozJobUsage(env, {
        siteId: usageSiteId,
        day: collectedDay,
        jobId,
        jobType: "moz_anchor_text",
        rowsUsed: anchors.length,
        degradedMode: parseBoolUnknown(body.degraded_mode, false),
        fallbackReason: cleanString(body.fallback_reason, 200) || null,
        profile: usageProfile,
      });
      await finalizeJobSuccess(env, jobId, Math.max(1, anchors.length));
      return Response.json({
        ok: true,
        job_id: jobId,
        target_url_id: urlId,
        collected_day: collectedDay,
        geo_key: geoKey,
        anchor_count: anchors.length,
      });
    }

    if (req.method === "POST" && url.pathname === "/moz/linking-root-domains") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const targetUrl = cleanString(body.target_url, 2000);
      if (!targetUrl) return Response.json({ ok: false, error: "target_url_required" }, { status: 400 });
      const urlId = await getOrCreateUrlId(env, targetUrl);
      if (!urlId) return Response.json({ ok: false, error: "invalid_target_url" }, { status: 400 });
      const domainsRaw = Array.isArray(body.top_domains) ? body.top_domains : [];
      const geoKey = parseMozGeoKey(body.geo_key);
      const topDomains = domainsRaw
        .map((raw) => parseJsonObject(raw))
        .filter((v): v is Record<string, unknown> => !!v)
        .slice(0, 500);
      const jobId = await createJobRecord(env, {
        userId: cleanString(body.user_id, 120) || null,
        siteId: cleanString(body.site_id, 120) || null,
        type: "moz_linking_root_domains",
        request: { target_url: targetUrl, domain_rows: topDomains.length, geo_key: geoKey },
      });
      const collectedDay = normalizeDayString(body.collected_day);
      const siteRunId = cleanString(body.site_run_id, 120) || null;
      await env.DB.prepare(
        `INSERT INTO moz_linking_root_domains_snapshots (
          snapshot_id, target_url_id, collected_day, geo_key, top_domains_json, total_domain_rows,
          totals_json, rows_used, site_run_id, job_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(target_url_id, collected_day, geo_key) DO UPDATE SET
          top_domains_json = excluded.top_domains_json,
          total_domain_rows = excluded.total_domain_rows,
          totals_json = excluded.totals_json,
          rows_used = excluded.rows_used,
          site_run_id = excluded.site_run_id,
          job_id = excluded.job_id,
          created_at = excluded.created_at`
      )
        .bind(
          uuid("mozroot"),
          urlId,
          collectedDay,
          geoKey,
          safeJsonStringify(topDomains, 32000),
          topDomains.length,
          safeJsonStringify(parseJsonObject(body.totals) ?? {}, 8000),
          topDomains.length,
          siteRunId,
          jobId,
          Math.floor(nowMs() / 1000)
        )
        .run();
      await createArtifactRecord(env, {
        jobId,
        kind: "moz.linking_root_domains.raw",
        payload: body,
      });
      let usageSiteId: string | null = null;
      let usageProfile: MozProfileName | null = null;
      const bodySiteId = cleanString(body.site_id, 120);
      if (bodySiteId) {
        const usageSite = await loadStep1Site(env, bodySiteId);
        if (usageSite) {
          usageSiteId = usageSite.site_id;
          usageProfile = (await loadMozSiteProfile(env, usageSite.site_id)).moz_profile;
        }
      }
      await recordMozJobUsage(env, {
        siteId: usageSiteId,
        day: collectedDay,
        jobId,
        jobType: "moz_linking_root_domains",
        rowsUsed: topDomains.length,
        degradedMode: parseBoolUnknown(body.degraded_mode, false),
        fallbackReason: cleanString(body.fallback_reason, 200) || null,
        profile: usageProfile,
      });
      await finalizeJobSuccess(env, jobId, Math.max(1, topDomains.length));
      return Response.json({
        ok: true,
        job_id: jobId,
        target_url_id: urlId,
        collected_day: collectedDay,
        geo_key: geoKey,
        domain_rows: topDomains.length,
      });
    }

    if (req.method === "POST" && url.pathname === "/moz/link-intersect") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const collectedDay = normalizeDayString(body.collected_day);
      const cluster = cleanString(body.cluster, 160);
      const geoKey = parseMozGeoKey(body.geo_key);
      const siteId = cleanString(body.site_id, 120) || null;
      const payload = parseJsonObject(body.intersect) ?? {};
      const siteRunId = cleanString(body.site_run_id, 120) || null;
      const rowsUsed = clampInt(body.rows_used, 0, 1_000_000_000, 1);
      const jobId = await createJobRecord(env, {
        userId: cleanString(body.user_id, 120) || null,
        siteId,
        type: "moz_link_intersect",
        request: { cluster, collected_day: collectedDay, geo_key: geoKey },
      });
      await env.DB.prepare(
        `INSERT INTO moz_link_intersect_snapshots (
          snapshot_id, site_id, cluster, collected_day, geo_key, intersect_json, totals_json, rows_used, site_run_id, job_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, cluster, collected_day, geo_key) DO UPDATE SET
          intersect_json = excluded.intersect_json,
          totals_json = excluded.totals_json,
          rows_used = excluded.rows_used,
          site_run_id = excluded.site_run_id,
          job_id = excluded.job_id,
          created_at = excluded.created_at`
      )
        .bind(
          uuid("mozint"),
          siteId,
          cluster,
          collectedDay,
          geoKey,
          safeJsonStringify(payload, 32000),
          safeJsonStringify(parseJsonObject(body.totals) ?? {}, 8000),
          rowsUsed,
          siteRunId,
          jobId,
          Math.floor(nowMs() / 1000)
        )
        .run();
      await createArtifactRecord(env, { jobId, kind: "moz.link_intersect.raw", payload: body });
      let usageSiteId: string | null = null;
      let usageProfile: MozProfileName | null = null;
      if (siteId) {
        const usageSite = await loadStep1Site(env, siteId);
        if (usageSite) {
          usageSiteId = usageSite.site_id;
          usageProfile = (await loadMozSiteProfile(env, usageSite.site_id)).moz_profile;
        }
      }
      await recordMozJobUsage(env, {
        siteId: usageSiteId,
        day: collectedDay,
        jobId,
        jobType: "moz_link_intersect",
        rowsUsed,
        degradedMode: parseBoolUnknown(body.degraded_mode, false),
        fallbackReason: cleanString(body.fallback_reason, 200) || null,
        profile: usageProfile,
      });
      await finalizeJobSuccess(env, jobId, 1);
      return Response.json({ ok: true, job_id: jobId, collected_day: collectedDay, cluster, geo_key: geoKey, site_id: siteId });
    }

    if (req.method === "POST" && url.pathname === "/moz/usage-data") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const collectedDay = normalizeDayString(body.collected_day);
      const usage = parseJsonObject(body.usage) ?? body;
      const rowsUsed = clampInt(usage.rows_used, 0, 10_000_000_000, 0);
      const rowsLimit = clampInt(usage.rows_limit, 0, 10_000_000_000, 0);
      const jobId = await createJobRecord(env, {
        userId: cleanString(body.user_id, 120) || null,
        siteId: cleanString(body.site_id, 120) || null,
        type: "moz_usage_data",
        request: { collected_day: collectedDay, rows_used: rowsUsed, rows_limit: rowsLimit },
      });
      await env.DB.prepare(
        `INSERT INTO moz_usage_snapshots (
          snapshot_id, collected_day, usage_json, rows_used, rows_limit, job_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(collected_day) DO UPDATE SET
          usage_json = excluded.usage_json,
          rows_used = excluded.rows_used,
          rows_limit = excluded.rows_limit,
          job_id = excluded.job_id,
          created_at = excluded.created_at`
      )
        .bind(
          uuid("mozuse"),
          collectedDay,
          safeJsonStringify(usage, 32000),
          rowsUsed || null,
          rowsLimit || null,
          jobId,
          Math.floor(nowMs() / 1000)
        )
        .run();
      await createArtifactRecord(env, { jobId, kind: "moz.usage_data.raw", payload: body });
      let usageSiteId: string | null = null;
      let usageProfile: MozProfileName | null = null;
      const bodySiteId = cleanString(body.site_id, 120);
      if (bodySiteId) {
        const usageSite = await loadStep1Site(env, bodySiteId);
        if (usageSite) {
          usageSiteId = usageSite.site_id;
          usageProfile = (await loadMozSiteProfile(env, usageSite.site_id)).moz_profile;
        }
      }
      await recordMozJobUsage(env, {
        siteId: usageSiteId,
        day: collectedDay,
        jobId,
        jobType: "moz_usage_data",
        rowsUsed: 1,
        degradedMode: parseBoolUnknown(body.degraded_mode, false),
        fallbackReason: cleanString(body.fallback_reason, 200) || null,
        profile: usageProfile,
      });
      await finalizeJobSuccess(env, jobId, 1);
      return Response.json({ ok: true, job_id: jobId, collected_day: collectedDay, rows_used: rowsUsed, rows_limit: rowsLimit });
    }

    if (req.method === "POST" && url.pathname === "/moz/index-metadata") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      const collectedDay = normalizeDayString(body.collected_day);
      const metadata = parseJsonObject(body.metadata) ?? body;
      const indexUpdatedAt = cleanString(metadata.index_updated_at ?? metadata.last_updated, 80) || null;
      const jobId = await createJobRecord(env, {
        userId: cleanString(body.user_id, 120) || null,
        siteId: cleanString(body.site_id, 120) || null,
        type: "moz_index_metadata",
        request: { collected_day: collectedDay, index_updated_at: indexUpdatedAt },
      });
      await env.DB.prepare(
        `INSERT INTO moz_index_metadata_snapshots (
          snapshot_id, collected_day, metadata_json, index_updated_at, job_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(collected_day) DO UPDATE SET
          metadata_json = excluded.metadata_json,
          index_updated_at = excluded.index_updated_at,
          job_id = excluded.job_id,
          created_at = excluded.created_at`
      )
        .bind(
          uuid("mozidx"),
          collectedDay,
          safeJsonStringify(metadata, 32000),
          indexUpdatedAt,
          jobId,
          Math.floor(nowMs() / 1000)
        )
        .run();
      await createArtifactRecord(env, { jobId, kind: "moz.index_metadata.raw", payload: body });
      let usageSiteId: string | null = null;
      let usageProfile: MozProfileName | null = null;
      const bodySiteId = cleanString(body.site_id, 120);
      if (bodySiteId) {
        const usageSite = await loadStep1Site(env, bodySiteId);
        if (usageSite) {
          usageSiteId = usageSite.site_id;
          usageProfile = (await loadMozSiteProfile(env, usageSite.site_id)).moz_profile;
        }
      }
      await recordMozJobUsage(env, {
        siteId: usageSiteId,
        day: collectedDay,
        jobId,
        jobType: "moz_index_metadata",
        rowsUsed: 1,
        degradedMode: parseBoolUnknown(body.degraded_mode, false),
        fallbackReason: cleanString(body.fallback_reason, 200) || null,
        profile: usageProfile,
      });
      await finalizeJobSuccess(env, jobId, 1);
      return Response.json({ ok: true, job_id: jobId, collected_day: collectedDay, index_updated_at: indexUpdatedAt });
    }

    if (req.method === "POST" && url.pathname === "/page/fetch") {
      let body: Record<string, unknown> | null = null;
      try {
        body = parseJsonObject(await req.json());
      } catch {
        body = null;
      }
      if (!body) {
        return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
      }
      const urlToFetch = cleanString(body.url, 2000);
      const siteId = cleanString(body.site_id, 120) || null;
      if (!urlToFetch) {
        return Response.json({ ok: false, error: "url_required" }, { status: 400 });
      }
      const site = siteId ? await loadStep1Site(env, siteId) : null;
      const providers = site ? await loadSiteProviderProfile(env, site.site_id) : null;
      const pageProvider = resolvePagePrimary(
        env,
        providers?.page_provider ?? (cleanString(body.page_provider, 40) ? normalizePageProvider(body.page_provider) : null)
      );
      const geoProvider = providers?.geo_provider ?? normalizeGeoProvider(body.geo_provider);
      const geoLabel = cleanString(body.geo_label, 120) || "us";
      const jobId = await createJobRecord(env, {
        siteId: site?.site_id ?? null,
        userId: cleanString(body.user_id, 120) || null,
        type: "page_fetch_extract",
        request: {
          url: urlToFetch,
          site_id: site?.site_id ?? null,
          page_provider: pageProvider,
          geo_provider: geoProvider,
          geo_label: geoLabel,
        },
      });
      const cache: { etag: string | null; last_modified: string | null; html: string | null } | null = null;
      const fetched = await fetchPageHtmlByProvider(env, {
        pageProvider,
        geoProvider,
        geoLabel,
        url: urlToFetch,
        timeoutMs: clampInt(body.timeout_ms, 1000, 20000, 8000),
        cache,
      });
      if (!fetched.html) {
        await finalizeJobFailure(env, jobId, "page_fetch_failed", {
          url: urlToFetch,
          status: fetched.status,
          extractor_mode: fetched.extractor_mode,
          fallback_reason: fetched.fallback_reason,
        });
        return Response.json(
          {
            ok: false,
            error: "page_fetch_failed",
            job_id: jobId,
            status: fetched.status,
            extractor_mode: fetched.extractor_mode,
            fallback_reason: fetched.fallback_reason,
          },
          { status: 502 }
        );
      }
      const persisted = await persistCanonicalPageSnapshot(env, {
        url: urlToFetch,
        html: fetched.html,
        parserVersion: fetched.extractor_mode === "decodo_web_api" ? "decodo-web-v1" : "direct-fetch-v1",
        rawSha256: fetched.raw_payload_sha256,
      });
      await createArtifactRecord(env, {
        jobId,
        kind: "page.fetch.audit",
        payload: {
          url: urlToFetch,
          site_id: site?.site_id ?? null,
          extractor_mode: fetched.extractor_mode,
          fallback_reason: fetched.fallback_reason,
          raw_payload_sha256: fetched.raw_payload_sha256,
          provider_task_id: fetched.provider_task_id,
          url_id: persisted.urlId,
          page_snapshot_id: persisted.pageSnapshotId,
          content_hash: persisted.contentHash,
        },
      });
      await finalizeJobSuccess(env, jobId, 1);
      return Response.json({
        ok: true,
        job_id: jobId,
        url: urlToFetch,
        site_id: site?.site_id ?? null,
        extractor_mode: fetched.extractor_mode,
        fallback_reason: fetched.fallback_reason,
        provider_task_id: fetched.provider_task_id,
        url_id: persisted.urlId,
        page_snapshot_id: persisted.pageSnapshotId,
        content_hash: persisted.contentHash,
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
      const proxyLeaseId = cleanString(body.proxy_lease_id, 120) || null;
      const summary = await runSerpWatchlist(env, {
        userId,
        watchId,
        limit,
        force,
        proxyLeaseId,
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
      const scheduledTs = controller.scheduledTime ?? nowMs();
      const d = new Date(scheduledTs);
      const isWeeklyRefresh = d.getUTCDay() === 1; // Monday UTC
      const isMonthlyRefresh = d.getUTCDate() === 1;
      const serpSummary = await runSerpWatchlist(env, {
        userId: "",
        watchId: "",
        limit: 500,
        force: false,
        proxyLeaseId: null,
      });
      const step2Sites = await listStep2EligibleSites(env, 20);
      const step2Summaries: Array<Record<string, unknown>> = [];
      for (const site of step2Sites) {
        try {
          const summary = await runStep2DailyHarvest(env, site, {
            maxKeywords: SITE_KEYWORD_CAP,
            maxResults: SITE_SERP_RESULTS_CAP,
            geo: site.local_mode ? "US-metro" : "US",
            parentJobId: null,
            runType: "auto",
          });
          let mozSummary: Record<string, unknown> | null = null;
          try {
            mozSummary = await runMozProfileForSite(env, {
              site,
              day: toDateYYYYMMDD(scheduledTs),
              isWeekly: isWeeklyRefresh,
              trigger: "cron",
            });
          } catch (mozError) {
            mozSummary = {
              ok: false,
              error: String((mozError as Error)?.message ?? mozError),
            };
          }
          step2Summaries.push({
            site_id: site.site_id,
            ok: true,
            summary,
            moz_summary: mozSummary,
          });
        } catch (error) {
          step2Summaries.push({
            site_id: site.site_id,
            ok: false,
            error: String((error as Error)?.message ?? error),
          });
        }
      }
      const monthlyStep1Refresh: Array<Record<string, unknown>> = [];
      if (isMonthlyRefresh) {
        for (const site of step2Sites.slice(0, 20)) {
          try {
            const refresh = await runStep1KeywordResearch(env, site, null);
            monthlyStep1Refresh.push({
              site_id: site.site_id,
              ok: true,
              research_run_id: refresh.researchRunId,
            });
          } catch (error) {
            monthlyStep1Refresh.push({
              site_id: site.site_id,
              ok: false,
              error: String((error as Error)?.message ?? error),
            });
          }
        }
      }
      console.log(
        JSON.stringify({
          event: "daily_serp_and_step2_run",
          scheduled_time: controller.scheduledTime ?? null,
          cron: controller.cron ?? null,
          weekly_refresh: isWeeklyRefresh,
          monthly_refresh: isMonthlyRefresh,
          serp_watchlist_summary: serpSummary,
          step2_site_count: step2Sites.length,
          step2_summaries: step2Summaries,
          monthly_step1_refresh: monthlyStep1Refresh,
        })
      );
    })();
    ctx.waitUntil(runPromise);
    await runPromise;
  },
};
