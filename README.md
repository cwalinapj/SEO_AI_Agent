## SEO_AI_Agent

### Database migrations include support for

#### SERP sampling & persistence
- **`POST /serp/sample`** persistence:
  - Tables: `serp_runs`, `serp_results`
  - Fallback tracking: `mode`, `fallback_reason`

#### Onboarding link ranking
- **`POST /onboarding/next-links`** ranking inputs via view: `onboarding_next_links`
  - Rank weight from SERP position
  - `kw_score = normalize(volume) × normalize(cpc) × normalize(rank_weight)` when metrics are available
  - Rank-only fallback when CPC/volume are missing
  - Domain diversity: one result per domain

#### Style preference capture
- **`POST /event`** stores style preferences in `user_events` (`like` / `dislike`) for downstream semantic-memory updates

#### Default next-question template
- The default next-question template offers:
  - Font
  - Palette
  - Layout
  - “Show me different styles”

---

### Production PSI monitoring schema

- **`POST /deploy/notify`** updates `sites.last_deploy_hash`
- **`POST /speed/check`** writes `speed_snapshots` with triggers:
  - `deploy` / `manual` / `failsafe`
- 12-hour per-site + strategy cooldown
- Delta view: `speed_snapshot_deltas`
  - LCP/CLS/TBT/field regressions
  - Severity: `warn` / `critical`

#### Required secret
- `PAGESPEED_API_KEY`

```bash
npx wrangler secret put PAGESPEED_API_KEY
