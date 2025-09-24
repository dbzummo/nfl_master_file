# NFL Master File — Project Map

## Phases
- **Phase 0:** Data ingest & normalization (MSF schedule, odds, rosters, injuries)
- **Phase 1:** Odds fetch integration
- **Phase 2:** Injury pipeline + backoff/retry
- **Phase 3:** Model calibration (Elo, Platt scaling)
- **Phase 4:** Board rendering (board_week.html)
- **Phase 5+:** Evaluation, reporting, automation

## Current State
- ✅ Odds/market join works, but fetch_odds.py sometimes returns None.
- ⚠ Injuries: intermittent 429, backoff partially working.
- ⚠ Board renderer: schema alignment issues (`p_home` vs `p_home_model`).
- 🛠 Active fix: patch fetch_odds None-guard and re-raise.

## Next Actions
- **Daniel (local):** Run Phase 0, confirm `out/week_with_market.csv` has 15 rows (Week 2).
- **ChatGPT:** Patch fetch_odds guard, draft injuries backoff logic.

## Known Issues
- Injury API overload → retry needed.
- Large .bak files clutter repo (archived).
- `.env` must never be mirrored.

## Canonical Bundle (always mirrored to Drive)
- repo_tree.txt
- scripts/
- src/
- config/
- manifest.json
- out/ (current week only)
- reports/

---
