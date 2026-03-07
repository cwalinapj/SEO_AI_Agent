export interface Env {
  CONTACT_WEBHOOK_SECRET?: string;
  CONTACT_FORM_TO_EMAIL?: string;
  CONTACT_FORM_FROM_EMAIL?: string;
  RESEND_API_KEY?: string;
  EMAIL_PROVIDER?: string;
}

function cleanString(input: unknown, maxLen = 4000): string {
  if (typeof input !== "string") return "";
  const value = input.trim();
  if (!value) return "";
  return value.slice(0, maxLen);
}

function parseJsonObject(input: unknown): Record<string, unknown> | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) return null;
  return input as Record<string, unknown>;
}

function safeEqualHex(a: string, b: string): boolean {
  const aNorm = cleanString(a, 512).toLowerCase();
  const bNorm = cleanString(b, 512).toLowerCase();
  if (aNorm.length !== bNorm.length || aNorm.length === 0) return false;
  let diff = 0;
  for (let i = 0; i < aNorm.length; i += 1) diff |= aNorm.charCodeAt(i) ^ bNorm.charCodeAt(i);
  return diff === 0;
}

async function hmacSha256Hex(secret: string, payload: string): Promise<string> {
  const enc = new TextEncoder();
  const keyData = enc.encode(secret);
  const messageData = enc.encode(payload);
  const key = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, messageData));
  return Array.from(sig).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function sendViaMailchannels(env: Env, lead: Record<string, unknown>, host: string): Promise<void> {
  const toEmail = cleanString(env.CONTACT_FORM_TO_EMAIL, 320) || "paul@aiwpdev.com";
  const fromEmail = cleanString(env.CONTACT_FORM_FROM_EMAIL, 320) || "noreply@aiwpdev.com";
  const subject = `New Contact Lead - ${host}`;
  const bodyText = [
    `Name: ${cleanString(lead.name, 200) || "(not provided)"}`,
    `Email: ${cleanString(lead.email, 320) || "(not provided)"}`,
    `Company: ${cleanString(lead.company, 200) || "(not provided)"}`,
    "",
    "Message:",
    cleanString(lead.message, 5000) || "(empty)",
  ].join("\n");

  const response = await fetch("https://api.mailchannels.net/tx/v1/send", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      personalizations: [{ to: [{ email: toEmail }] }],
      from: { email: fromEmail, name: "AIWPDev Contact Relay" },
      subject,
      content: [{ type: "text/plain", value: bodyText }],
      reply_to: { email: cleanString(lead.email, 320) || fromEmail, name: cleanString(lead.name, 200) || "Website Lead" },
    }),
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`mailchannels_failed:${response.status}:${errText.slice(0, 250)}`);
  }
}

async function sendViaResend(env: Env, lead: Record<string, unknown>, host: string): Promise<void> {
  const apiKey = cleanString(env.RESEND_API_KEY, 500);
  if (!apiKey) throw new Error("resend_api_key_missing");
  const toEmail = cleanString(env.CONTACT_FORM_TO_EMAIL, 320) || "paul@aiwpdev.com";
  const fromEmail = cleanString(env.CONTACT_FORM_FROM_EMAIL, 320) || "noreply@aiwpdev.com";
  const text = [
    `Name: ${cleanString(lead.name, 200) || "(not provided)"}`,
    `Email: ${cleanString(lead.email, 320) || "(not provided)"}`,
    `Company: ${cleanString(lead.company, 200) || "(not provided)"}`,
    "",
    "Message:",
    cleanString(lead.message, 5000) || "(empty)",
  ].join("\n");

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      from: fromEmail,
      to: [toEmail],
      subject: `New Contact Lead - ${host}`,
      text,
      reply_to: cleanString(lead.email, 320) || undefined,
    }),
  });
  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`resend_failed:${response.status}:${errText.slice(0, 250)}`);
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/health")) {
      return Response.json({ ok: true, service: "contact-relay" });
    }
    if (req.method !== "POST" || url.pathname !== "/relay/contact") {
      return Response.json({ ok: false, error: "not_found" }, { status: 404 });
    }

    const rawBody = await req.text();
    const timestamp = cleanString(req.headers.get("x-relay-timestamp"), 64);
    const signature = cleanString(req.headers.get("x-relay-signature"), 256).toLowerCase();
    const secret = cleanString(env.CONTACT_WEBHOOK_SECRET, 500);

    if (!timestamp || !signature || !secret) {
      return Response.json({ ok: false, error: "missing_signature_or_secret" }, { status: 401 });
    }

    const tsNum = Number(timestamp);
    if (!Number.isFinite(tsNum)) {
      return Response.json({ ok: false, error: "invalid_timestamp" }, { status: 401 });
    }
    const ageSec = Math.abs(Math.floor(Date.now() / 1000) - Math.floor(tsNum));
    if (ageSec > 300) {
      return Response.json({ ok: false, error: "timestamp_out_of_window" }, { status: 401 });
    }

    const expected = await hmacSha256Hex(secret, `${timestamp}.${rawBody}`);
    if (!safeEqualHex(signature, expected)) {
      return Response.json({ ok: false, error: "invalid_signature" }, { status: 401 });
    }

    let parsed: Record<string, unknown> | null = null;
    try {
      parsed = parseJsonObject(JSON.parse(rawBody));
    } catch {
      parsed = null;
    }
    if (!parsed) {
      return Response.json({ ok: false, error: "invalid_json_body" }, { status: 400 });
    }

    const lead = parseJsonObject(parsed.lead) ?? {};
    const host = cleanString(parsed.host, 255) || "unknown-host";
    if (!cleanString(lead.name, 200) || !cleanString(lead.email, 320) || !cleanString(lead.message, 5000)) {
      return Response.json({ ok: false, error: "name_email_message_required" }, { status: 400 });
    }

    const provider = cleanString(env.EMAIL_PROVIDER, 40).toLowerCase() || "mailchannels";
    try {
      if (provider === "resend") {
        await sendViaResend(env, lead, host);
      } else {
        await sendViaMailchannels(env, lead, host);
      }
    } catch (error) {
      return Response.json(
        { ok: false, error: "email_send_failed", detail: cleanString((error as Error)?.message ?? error, 500) },
        { status: 502 }
      );
    }

    return Response.json({ ok: true, relayed: true, provider });
  },
};
