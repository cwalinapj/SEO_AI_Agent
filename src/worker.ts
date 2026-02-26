export interface Env {
  DB: D1Database;
  PAGESPEED_API_KEY: string;
  WP_PLUGIN_SHARED_SECRET?: string;
}

const COOLDOWN_MS = 12 * 60 * 60 * 1000; // 12 hours
const PLUGIN_TIMESTAMP_WINDOW_MS = 5 * 60 * 1000;

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

    if (req.method === "POST" && url.pathname === "/plugin/wp/schema/save") {
      const verified = await verifySignedPluginRequest(req, env);
      if (!verified.ok) {
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
      if (!verified.ok) {
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
      if (!verified.ok) {
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
      if (!verified.ok) {
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

      const cooldown = await canRunSpeed(env, site_id);
      if (!cooldown.ok) {
        return Response.json({ ok: false, reason: "cooldown", wait_ms: cooldown.waitMs }, { status: 429 });
      }

      const strategy = (body.strategy ? String(body.strategy) : String(site.default_strategy || "mobile")).toLowerCase();
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

        return Response.json({ ok: true, snapshot_id, strategy: s, delta, cur, prev });
      } catch (e: any) {
        return Response.json({ ok: false, error: String(e?.message ?? e) }, { status: 502 });
      }
    }

    return new Response("Not Found", { status: 404 });
  }
};
