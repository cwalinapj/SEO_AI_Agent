# Traffic Oracle / Eligibility Engine MVP

## 1) Goal / Product Promise

Enter your domain and immediately see whether you qualify for free hosting and renewal subsidies. The system is designed for Web2 operators: no Google Ads or Search Console access tokens are required from users. We rely on independent public signals, cross-validated by live SERP checks, then return a clear eligibility decision with reasons and next steps.

## 2) System Overview (Traffic Oracle)

The Traffic Oracle is a multi-signal decision layer that produces:

- `tier`: Bronze, Silver, Gold, or Verify-only
- `subsidy_estimate`: rough USD subsidy range for hosting/renewal support
- `confidence`: low, medium, high
- `reasons`: machine-friendly and human-readable decision reasons
- `next_steps`: concrete actions needed for upgrade or verification

Design principle:

- SEMrush and Ahrefs are **signals** for broad coverage.
- Live SERP checks are **truth verification** for current rank reality.
- Decisions are explainable, reproducible, and cache-aware.

## 3) Signals (with pros/cons)

### A) Provider estimate signals (scraped via Apify)

Sources:

- SEMrush domain overview: organic traffic estimate, keyword count, paid traffic hints, top pages
- Ahrefs domain overview: DR, backlinks, referring domains, organic keyword footprint

Pros:

- Fast to collect for many domains
- Good coverage for cold starts
- Useful historical trend snapshots

Cons:

- Estimates can diverge by provider
- Data freshness varies by provider and market
- Can be noisy on smaller/local domains

Confidence stance:

- **Medium confidence**, **high coverage**

### B) Live SERP truth (Apify)

Checks:

- keyword -> position (top 100)
- SERP feature presence (local pack, snippets, shopping, maps, etc.)
- competitor density for the same keyword set

Keyword set sources:

1. Provider top keywords from SEMrush/Ahrefs (seed)
2. Crawl-derived terms/entities from homepage, title, H1, key pages, schema text
3. Optional industry keyword packs

Operational cap:

- hard cap keyword checks per run (example: 200)

Pros:

- Measures actual discoverability now
- Harder to fake than static provider metrics
- Useful for rank-shift anomaly detection

Cons:

- More expensive than estimate-only checks
- Requires queueing and batching
- Regional SERP differences require locale strategy

### C) Keyword value (CPC/volume) without Ads tokens

Primary strategy:

- Use provider APIs for CPC + volume (DataForSEO, SerpApi, equivalent feeds)

Fallback strategy:

- Apify scrape-based CPC/volume as best-effort only
- Fallback values are non-authoritative and lower confidence

Policy:

- No dependency on user-owned Google tokens
- CPC/volume enriches scoring but does not block MVP decisions

## 4) Scoring Model (practical)

Compute a normalized `score_0_100` with fixed weighted components:

- Rank Signal (40%)
- Traffic Estimate (20%)
- Backlink/Authority (15%)
- Site Footprint (15%)
- Risk/Fraud penalty (10% negative)

### Rank Signal (40%)

Inputs:

- count in top 3
- count in top 10
- count in top 20
- optional weighting by `CPC * volume` where available

Expected behavior:

- high-value top 10 rankings drive strongest positive contribution

### Traffic Estimate (20%)

Inputs:

- provider traffic estimates
- median aggregation
- clipping to reduce outlier impact

Expected behavior:

- avoids over-trusting a single vendor estimate

### Backlink/Authority (15%)

Inputs:

- DR/domain authority equivalent
- referring domain quality
- trend direction (up, flat, down)

Expected behavior:

- trend-aware authority signal, not raw backlink count only

### Site Footprint (15%)

Inputs:

- crawl size
- freshness indicators
- page quality/health indicators (basic)

Expected behavior:

- rewards actively maintained real sites

### Risk/Fraud penalty (10% negative)

Inputs:

- parked domain patterns
- malware/spam signals
- abnormal rank spikes and volatility patterns

Expected behavior:

- subtract score; can force manual review paths

### Tier mapping

- `>= 80`: Gold
- `60-79`: Silver
- `40-59`: Bronze
- `< 40`: Verify-only

## 5) Pipeline (Apify-first architecture)

### Step 1: Domain intake

Input:

- domain name from onboarding UI/API

Work:

- normalize domain
- fetch homepage and sitemap hints
- extract candidate keywords and entities

Output:

- `domain_profile` seed with extracted terms

### Step 2: Provider scrapes

Input:

- normalized domain

Work:

- run Apify actors for SEMrush and Ahrefs overviews
- cache responses for 7-30 days

Output:

- `provider_signals` snapshot

### Step 3: Build keyword set

Input:

- provider keyword seeds
- crawl-derived terms/entities
- optional industry packs

Work:

- union, dedupe, normalize, cap (example: 200)

Output:

- final `keyword_set`

### Step 4: Live SERP checks

Input:

- `keyword_set`

Work:

- query SERP actor
- record positions, SERP features, competitors

Output:

- `rank_matrix`

### Step 5: Keyword value join

Input:

- keywords in `rank_matrix`

Work:

- enrich with CPC/volume from provider
- fallback enrichment only if primary unavailable

Output:

- `keyword_value_table`

### Step 6: Score + decision

Input:

- provider, rank, value, footprint, risk data

Work:

- compute score and tier
- derive subsidy estimate and confidence
- generate explainable reasons/next steps

Output (high-level schemas):

- `traffic_signals`:
  - domain
  - score_0_100
  - tier
  - confidence
  - provider_snapshot_at
  - rank_snapshot_at
  - components[]
- `eligibility_decisions`:
  - domain
  - eligible_for_free_hosting
  - eligible_for_renewal_subsidy
  - subsidy_estimate
  - reason_codes[]
  - next_steps[]
  - evaluated_at

## 6) Guardrails (must-have)

- Rate limiting and async queues:
  - intake returns `processing` for heavy checks
  - status polling endpoint for completion
- Stale-while-revalidate:
  - always return last known score quickly
  - refresh in background
- Provider quorum and graceful degradation:
  - tolerate one provider failure
  - downgrade confidence when only partial data is available
- Abuse prevention and budget caps:
  - per-domain and per-account run quotas
  - suspicious repeat checks throttled
- Explainability by default:
  - persist `reason_codes`
  - render reasons in dashboard and API response

## 7) MVP Phasing

### MVP v1 (1-2 days)

- domain crawl + keyword/entity extraction
- live SERP checks for about 30 keywords
- rank-only scoring
- no CPC/value requirement
- output tier + confidence + reason codes + next steps

### MVP v2

- add SEMrush + Ahrefs ingestion at scale
- add CPC/volume provider enrichment
- add fraud/risk features
- improve subsidy estimate quality
- add stronger cache invalidation and scheduling

## 8) Integration notes for TollDNS / Domain Continuity

- Traffic Oracle feeds a compatibility payload similar to `DOMAIN_EXPIRY_WORKER_URL` expectations:
  - `expires_at`
  - `traffic_signal`
  - `treasury_renewal_allowed`
- Eligibility tier influences continuity behavior:
  - grace-mode banner policy
  - treasury renewal/subsidy decision path
- Decision contract is consumable by gateway continuity endpoints without requiring direct provider credentials from users.

## Glossary

- SERP: Search Engine Results Page for a query.
- CPC: Cost per click estimate for paid keyword auctions.
- Quorum: Agreement across multiple providers/signals before trusting output.
- Coverage signal: Broad but less precise metric source.
- Truth signal: Direct current-state verification source.
- Rank matrix: Table of keyword positions and SERP features by run.
- Reason code: Stable machine-readable explanation for a decision.
- Stale-while-revalidate: Return cached result immediately, refresh in background.
- Eligibility tier: Policy bucket (Bronze/Silver/Gold/Verify-only) that drives incentives.
- Subsidy estimate: Approximate support amount for hosting/renewal programs.
