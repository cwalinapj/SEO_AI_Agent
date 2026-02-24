# SEO_AI_Agent

Database migrations include support for:
- `POST /serp/sample` persistence (`serp_runs`, `serp_results`) with fallback mode tracking (`mode`, `fallback_reason`).
- `POST /onboarding/next-links` ranking inputs via `onboarding_next_links` view:
  - rank weight from SERP position
  - `kw_score = normalize(volume) × normalize(cpc) × normalize(rank_weight)` when metrics are available
  - rank-only fallback when CPC/volume are missing
  - domain diversity (one result per domain)
- `POST /event` style preference capture in `user_events` (`like` / `dislike`) for downstream semantic-memory updates.
- A default next-question template that offers `font`, `palette`, `layout`, and `show me different styles`.
