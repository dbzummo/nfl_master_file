# NFL Forecasting Project — Savepoint v5 (Strict, Fail‑Fast Build)

**Principle:** Accuracy > convenience. The run must *fail loudly* on stale or conflicting data. No silent fallbacks. No “best guesses.”
**You say:** “Run week 1.” **The system does:** validate → fetch → simulate → export, or abort with precise reasons.

---

## 1) What this model does (today)

- **Inputs (live + static):**
  - **Odds** via TheOddsAPI (consensus line/spread/total) → `fetch_odds.py`.
  - **Injuries** via API (SportsDataIO) → `fetch_injuries.py` (strict: no empty set unless config allows).
  - **Rosters** (Primary: SportsDataIO; Secondary: nflverse placeholder for now) → `fetch_rosters.py`.
  - **Team Ratings** (preseason Kalman state) → `kalman_state_preseason.csv` (`rating`, `uncertainty`).
  - **Depth Charts + Position Values** → `team_depth_charts_with_values.csv`.
  - **Home Field Advantage** (HFA) → `stadium_hfa_advanced.csv` merged by `team_code` (strict schema).

- **Audit layer** (gatekeeper) → `run_audit.py`:
  - Calls two roster sources.
  - **BLOCK** when both empty; **HOLD** for material field mismatches (e.g., named starter conflicts).
  - The main run **aborts** on any BLOCK; prints a banner for any HOLD (configurable to abort).

- **Monte Carlo** → `run_monte_carlo.py`:
  - Mean margin = `(rating_home − rating_away) + HFA + injury_net + depth‑chart deltas`.
  - **σ (variance)**: context‑aware — if `variance_model.joblib` present, use it; else deterministic mapping from total/spread.
  - Simulates outcomes → win prob (H), cover prob (H), total Over prob.

- **Runner** → `run_predictions.py`:
  - Loads `.env` (keys), asserts schemas, executes audit, fetches odds/injuries, runs MC, writes artifacts.

- **Outputs:**
  - `out_week1_predictions.csv` — table of all games (win/cover/O/U).
  - `out_week1_gamecards.csv` — detailed drivers used per matchup.
  - `roster_audit_log.json` — audit evidence (per team).
  - `manifest.json` — run metadata (git/hash, timestamps, config hash, file fingerprints).

---

## 2) Files in this savepoint (and why they exist)

> Files marked **(required)** must be present for a run to proceed; missing ones will trigger fail‑fast with an actionable error.

- **Code**
  - `run_predictions.py` **(required)** — Orchestrator with strict gates (BLOCK/HOLD).
  - `run_monte_carlo.py` **(required)** — Simulation engine (documented interfaces).
  - `run_audit.py` **(required)** — Two‑source roster audit (BLOCK/HOLD classification).
  - `update_kalman_state.py` — Weekly Kalman update (post‑results ingestion).
  - `fetch_odds.py` **(required)** — TheOddsAPI client; returns normalized DataFrame.
  - `fetch_injuries.py` **(required)** — SportsDataIO client; returns structured injuries.
  - `fetch_rosters.py` **(required)** — SportsDataIO primary; nflverse placeholder secondary.
  - `utils_io.py` — File utilities, hashing, schema checks.
  - `build_variance_model.py` — Fit/retrain `variance_model.joblib` from history.
  - `build_hfa_model.py` — Support tooling for HFA recalibration.

- **Data / Models**
  - `kalman_state_preseason.csv` **(required)** — columns: `team_code,rating,uncertainty,hfa` (strict).
  - `team_depth_charts_with_values.csv` **(required)** — columns: `team_code,position,player,value,depth` (≥ core positions).
  - `stadium_hfa_advanced.csv` **(required)** — columns: `team_code,hfa` (numeric).
  - `stadium_details.csv` — stadium metadata (altitude, roof, surface, coords).
  - `variance_model.joblib` — optional; if absent, deterministic σ mapping used.
  - `player_priors_2025.csv` — individual priors (EPA/snap‑weighted) for depth deltas.
  - `player_team_map.csv` — name canonicalization and current team mapping.
  - `play_by_play_20{22,23,24}.csv` — historical inputs for priors (optional at runtime).
  - `schedule_2025.csv` — when extending beyond W1 (not required for live odds window).
  - `star_players_2025.csv` — drivers for “star badge” weights (optional; superseded by numeric values).

- **Config**
  - `master_model_file_v5.0.json` **(required)** — single source of truth for run behavior (see §5).
  - `.env` **(required at runtime)** — `THE_ODDS_API_KEY`, `SPORTSDATAIO_API_KEY`.

- **Artifacts (auto‑generated on each run)**
  - `out_week1_predictions.csv`, `out_week1_gamecards.csv`, `roster_audit_log.json`, `manifest.json`.

---

## 3) Non‑negotiable Guardrails (how we enforce 100% accuracy)

1. **Two‑Source Roster Audit** (BLOCK/HOLD)
   - **BLOCK** if *both* providers return zero players for any team in the slate. Run aborts.
   - **HOLD** if named‑starter conflicts or >X% mismatch at key positions (configurable). Default: abort on HOLD for QB/OC/CB1/OT.
2. **Odds / Injuries non‑empty checks**
   - Empty odds/injuries from the APIs → abort with a concrete message (key missing or plan limit exceeded).
3. **Schema checks**
   - `kalman_state_preseason.csv` must have **exact**: `team_code,rating,uncertainty,hfa`.
   - `stadium_hfa_advanced.csv` must have **team_code,hfa (float)**; any other columns ignored.
   - `team_depth_charts_with_values.csv` must cover core positions. Missing positions → HOLD (and optional abort).
4. **No stale fallbacks**
   - If a live provider fails, we do **not** silently use old local files. We stop and tell you why.
5. **Provenance**
   - Every run writes `manifest.json` with input hashes + config hash for reproducibility.

---

## 4) Environment & Setup (macOS, zsh)

1. Place your `.env` in project root:
   ```ini
   THE_ODDS_API_KEY=...
   SPORTSDATAIO_API_KEY=...
   ```
2. Load it per‑session **or** auto‑load:
   - Per‑session (simple):
     ```bash
     cd ~/Desktop/NFL_MASTER_FILE
     export $(cat .env | xargs)
     ```
   - Auto‑load (add to `~/.zshrc`):
     ```bash
     set -a
     [ -f ~/Desktop/NFL_MASTER_FILE/.env ] && source ~/Desktop/NFL_MASTER_FILE/.env
     set +a
     ```
3. Python deps:
   ```bash
   python3 -m pip install -r requirements.txt
   ```

---

## 5) Configuration Reference — `master_model_file_v5.0.json`

Key fields (examples—check your file for actual values):
```jsonc
{
  "strict_mode": true,
  "abort_on_hold_positions": ["QB","LT","CB1","OC"],
  "injury_weights": { "OUT": -1.0, "DOUBTFUL": -0.6, "QUESTIONABLE": -0.25 },
  "depth_chart_weights": { "QB": 3.0, "LT": 1.2, "WR1": 0.8, "CB1": 1.1, "ED1": 1.0, "OC": 0.7 },
  "hfa_scale": 1.0,
  "sigma": {
    "use_model": true,
    "model_path": "variance_model.joblib",
    "fallback": { "base": 10.2, "per_total_50": 0.15, "per_abs_spread_7": 0.10, "cap": 13.0 }
  },
  "reporting": { "top_n": 5, "save_gamecards": true }
}
```
- **strict_mode** = `true` → no silent degradation.
- **abort_on_hold_positions** — if HOLD at these positions, stop the run.
- **hfa_scale** — multiply `stadium_hfa_advanced.csv:hfa` by this factor (kept separate for calibration).
- **sigma** — use model if available else deterministic mapping.

---

## 6) How the data flows (Run of Week 1)

1. **Audit**: `run_audit.py`
   - Calls SportsDataIO + nflverse placeholder; builds `roster_audit_log.json` with evidence.
   - Any BLOCK/HOLD (per config) → **abort** with a banner.
2. **Load**: `kalman_state_preseason.csv`, `team_depth_charts_with_values.csv`, `stadium_hfa_advanced.csv`
   - Column assertions; HFA merged by `team_code` only.
3. **Fetch**: `fetch_odds.py`, `fetch_injuries.py`
   - TheOddsAPI window is auto‑computed for the upcoming week.
   - SportsDataIO injuries normalized to team/player/impact.
4. **Monte Carlo**: `run_monte_carlo.py`
   - `mu = (rating_H − rating_A) + HFA_H + injury_net + depth_delta`.
   - `sigma`: model or deterministic; guardrailed to [7, 15] (configurable).
   - Derive win/cover/O‑U probabilities.
5. **Report**: write CSVs + manifest.

---

## 7) HFA (Home Field Advantage) — **no more zeroing out**

- Source of truth: `stadium_hfa_advanced.csv` with **team_code,hfa** (float; positive favors home).
- If your HFA was built from **altitude, coords, roof, weather history, travel**, keep it here; the runner only scales by `hfa_scale`.
- Quick validation before runs:
  ```bash
  python3 - <<'PY'
import pandas as pd
h = pd.read_csv('stadium_hfa_advanced.csv')
print(h.describe(include='all'))
print("Non-zero HFA teams:", (h['hfa'] != 0).sum())
PY
  ```
- If you want tiered HFA (e.g., DEN/SEA > others), encode it directly in this file. **We do not recompute HFA during a run.**

---

## 8) Running a week (single command + what to expect)

```bash
cd ~/Desktop/NFL_MASTER_FILE
export $(cat .env | xargs)      # or auto-loaded via ~/.zshrc
python3 run_predictions.py | tee out_week1_console.txt
```

- **On success**, you get:
  - Console table of all games (win/cover/O‑U)
  - `out_week1_predictions.csv`
  - `out_week1_gamecards.csv`
  - `roster_audit_log.json`
  - `manifest.json` (hashes + input versions)

- **If it aborts**, the console will tell you **exactly** which gate failed (e.g., “BLOCK on JAX” or “stadium_hfa_advanced.csv missing numeric column ’hfa’”).

---

## 9) Troubleshooting (fast, targeted)

- **“Missing THE_ODDS_API_KEY”** → `.env` not loaded. `export $(cat .env | xargs)` then rerun.
- **“Roster audit BLOCK/HOLD”** → open `roster_audit_log.json` and resolve (update provider, correct team code, or override via config only if you accept the risk).
- **“KeyError: 'hfa'”** → your HFA file lacks `hfa` column. Ensure exactly `team_code,hfa`.
- **Weird σ warnings** → delete `variance_model.joblib` to use fallback mapping; retrain later with `build_variance_model.py`.

---

## 10) Roadmap (from unified plan + new work since)

**Phase A — Reliability (DONE/ONGOING)**
- ✅ Strict audit gates (BLOCK/HOLD) in runner.
- ✅ Odds/Injuries live fetch with required‑data checks.
- ✅ Schema assertions for ratings/HFA/depth charts.
- ✅ Deterministic σ fallback if model absent.

**Phase B — Data integrity & coverage (IN PROGRESS)**
- ☐ Expand `team_depth_charts_with_values.csv` completeness & numeric valuations.
- ☐ Finalize nflverse as true secondary roster source (current placeholder).
- ☐ Lock HFA file with your advanced values and commit the distribution that produced them.

**Phase C — Modeling upgrades**
- ☐ Weekly Kalman update ingestion (`update_kalman_state.py`) post results.
- ☐ Calibrate confidence tiers vs. realized cover rates.
- ☐ Enrich injury priors (pos-specific replacement levels).
- ☐ Coaching/scheme deltas & special teams modules.

---

## 11) Contract for accuracy (the “no nonsense” rules)

- If any **live dependency** fails (odds, injuries, rosters), the run **must** stop.
- No **silent defaults** (like zeroing HFA) unless explicitly configured and flagged in console + manifest.
- Any override must be **documented** in the manifest + console with the reason.

---

## 12) Minimal checklist before each run

- `.env` keys loaded? (`echo $THE_ODDS_API_KEY`, `echo $SPORTSDATAIO_API_KEY`)
- `stadium_hfa_advanced.csv` has non‑zero distribution you expect?
- `kalman_state_preseason.csv` has `rating,uncertainty,hfa` for 32 `team_code`s?
- `team_depth_charts_with_values.csv` covers core positions for all teams?
- Audit returns **no BLOCK/HOLD** (unless HOLD is allowed by config).

---

*This README is designed so any AI (or human) can pick up the project, understand its rules, and run it without guesswork while preserving the “accuracy over everything” ethos.*
