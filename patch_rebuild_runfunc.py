import re, sys, pathlib

p = pathlib.Path("run_predictions.py")
src = p.read_text(encoding="utf-8")

# 0) Ensure required imports exist (idempotent)
def ensure_import(s, needle, line):
    if needle not in s:
        # insert after the last import line
        m = re.search(r'(?m)^(from\s+\S+\s+import\s+.*|import\s+\S+)', s)
        if m:
            # find last import block
            last = None
            for mm in re.finditer(r'(?m)^(from\s+\S+\s+import\s+.*|import\s+\S+)', s):
                last = mm
            if last:
                pos = last.end()
                s = s[:pos] + "\n" + line + s[pos:]
        else:
            s = line + "\n" + s
    return s

src = ensure_import(src, "derive_injuries_from_rosters", "from injuries_fallbacks import derive_injuries_from_rosters")
src = ensure_import(src, "validate_odds", "from validators import validate_odds, validate_ratings, validate_depth, validate_injuries, apply_aliases")
src = ensure_import(src, "hashlib", "import hashlib")
src = ensure_import(src, "Path", "from pathlib import Path")

# 1) Find def run_weekly_predictions with any signature/annotation
m_def = re.search(r'(?m)^[ \t]*def\s+run_weekly_predictions\s*\([^)]*\)\s*(?:->\s*[^\:]+)?\s*:\s*', src)
if not m_def:
    print("❌ Could not find def run_weekly_predictions().")
    sys.exit(1)

def_start = m_def.start()

# 2) Find the end of the function (next top-level def or main-guard), else EOF
m_next_def = re.search(r'(?m)^[ \t]*def\s+\w+\s*\(', src[m_def.end():])
m_main_guard = re.search(r'(?m)^[ \t]*if\s+__name__\s*==\s*[\'"]__main__[\'"]\s*:', src[m_def.end():])
cands = [m for m in (m_next_def, m_main_guard) if m]
func_end = m_def.end() + min(m.start() for m in cands) if cands else len(src)

# 3) Determine function indent
line_start = src.rfind("\n", 0, m_def.end()) + 1
def_line = src[line_start: m_def.end()]
indent = re.match(r'[ \t]*', def_line).group(0)
inner = indent + "    "

# 4) Build a canonical body (keeps your pipeline + extras in manifest)
body = f"""
{inner}print("STEP 0: Preparing environment & config...")
{inner}cfg = _load_json(CONFIG_PATH)
{inner}require_env(os.environ, REQUIRED_ENV)
{inner}# provenance: config hash
{inner}try:
{inner}    config_hash = hashlib.sha256(Path(CONFIG_PATH).read_bytes()).hexdigest()
{inner}except Exception:
{inner}    config_hash = None

{inner}print("STEP 1: Fetching live odds (defines the week & teams in play)...")
{inner}odds_df = get_consensus_nfl_odds()
{inner}if isinstance(odds_df, list):
{inner}    odds_df = pd.DataFrame(odds_df)
{inner}need_odds = ["home_team", "away_team", "spread_home", "spread_away", "total", "kickoff_utc", "neutral_site"]
{inner}require_columns(odds_df, "weekly_odds", need_odds)
{inner}if odds_df.empty:
{inner}    die("No odds returned for the current week window. Check API key/plan or date window.")
{inner}# alias & validate odds after ratings are loaded (we need team map), so we defer strict validation

{inner}teams_in_play = _pick_teams_from_odds(odds_df)

{inner}print("\\nSTEP 2: Running live roster audit (BLOCK/HOLD)…")
{inner}audit_log = run_roster_audit(teams_to_check=teams_in_play)
{inner}enforce_roster_audit(audit_log)

{inner}print("\\nSTEP 3: Loading ratings + merging stadium HFA (no zeroing)…")
{inner}ratings_df = merge_hfa(str(RATINGS_PATH), str(HFA_PATH))
{inner}require_columns(ratings_df, "ratings+HFA", ["team_code", "rating", "uncertainty", "hfa"])
{inner}# normalize/validate ratings and odds coherency
{inner}ratings_df = apply_aliases(ratings_df, cols=["team_code"])
{inner}odds_df = apply_aliases(odds_df, cols=["home_team", "away_team"])
{inner}validate_ratings(ratings_df, strict=True)
{inner}validate_odds(odds_df, ratings_df, strict=True)

{inner}print("\\nSTEP 4: Loading depth charts…")
{inner}depth_df = _load_depth_charts(DEPTH_PATH)
{inner}depth_df = apply_aliases(depth_df, cols=["team_code"])
{inner}validate_depth(depth_df, strict=True)

{inner}print("\\nSTEP 5: Fetching latest injury data (strict)...")
{inner}injury_source = "live"
{inner}injuries = fetch_injured_players()
{inner}inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0
{inner}injuries_live_count = inj_ct
{inner}injuries_fallback_count = 0

{inner}if inj_ct == 0:
{inner}    # Conservative fallback from roster statuses (IR/PUP/NFI/Suspended)
{inner}    injuries = derive_injuries_from_rosters(teams_in_play)
{inner}    inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0
{inner}    injuries_fallback_count = inj_ct
{inner}    injury_source = "fallback"
{inner}    teams_ct = injuries["team_code"].nunique() if inj_ct else 0
{inner}    print(f"Roster-derived injuries: {{inj_ct}} rows across {{teams_ct}} teams.")

{inner}# Normalize injuries team codes and validate leniently (ok for empty)
{inner}if isinstance(injuries, pd.DataFrame) and not injuries.empty:
{inner}    injuries = apply_aliases(injuries, cols=["team_code"])
{inner}    validate_injuries(injuries, strict=False)

{inner}print(f"Found {{inj_ct}} records from injuries (live or fallback).")

{inner}sigma_policy_name = "constant"
{inner}print("\\nSTEP 6: Monte Carlo simulations...")
{inner}result = run_simulation(odds_df, ratings_df, depth_df, injuries)
{inner}if isinstance(result, tuple) and len(result) == 2:
{inner}    df_pred, df_cards = result
{inner}else:
{inner}    df_pred, df_cards = result, pd.DataFrame()

{inner}require_columns(df_pred, "simulation output (preds)", [
{inner}    "home_team","away_team","vegas_line","vegas_total","sigma",
{inner}    "win_prob_home","cover_prob_home","ou_prob_over","kickoff_utc","neutral_site"
{inner}])

{inner}print("\\n--- WEEKLY PREDICTIONS ---")
{inner}print(df_pred.to_string(index=False))

{inner}print("\\nSTEP 7: Writing artifacts...")
{inner}df_pred.to_csv(OUT_PREDS, index=False)
{inner}if not df_cards.empty:
{inner}    df_cards.to_csv(OUT_CARDS, index=False)

{inner}run_meta = {{
{inner}    "runner": platform.node(),
{inner}    "timestamp_utc": pd.Timestamp.utcnow().isoformat(),
{inner}    "python": platform.python_version(),
{inner}    "host": socket.gethostname(),
{inner}    "config_used": str(CONFIG_PATH.name)
{inner}}}
{inner}inputs = {{
{inner}    "ratings_csv": str(RATINGS_PATH.name),
{inner}    "stadium_hfa_csv": str(HFA_PATH.name),
{inner}    "depth_charts_csv": str(DEPTH_PATH.name),
{inner}    "odds_provider": "TheOddsAPI",
{inner}    "injury_provider": "SportsDataIO (or configured provider)"
{inner}}}
{inner}outputs = {{
{inner}    "predictions_csv": str(OUT_PREDS.name),
{inner}    "gamecards_csv": str(OUT_CARDS.name) if OUT_CARDS.exists() else None
{inner}}}
{inner}audits = {{
{inner}    "roster_audit": audit_log
{inner}}}
{inner}extras = {{
{inner}    "injury_source": injury_source,
{inner}    "injuries_live_count": injuries_live_count,
{inner}    "injuries_fallback_count": injuries_fallback_count,
{inner}    "sigma_policy": sigma_policy_name,
{inner}    "config_hash": config_hash
{inner}}}

{inner}write_manifest(OUT_MANIFEST, run_meta, inputs, outputs, audits, extras)

{inner}print(f"\\nSaved: {{OUT_PREDS.name}}" + (f", {{OUT_CARDS.name}}" if OUT_CARDS.exists() else ""))
{inner}print(f"Saved: {{OUT_MANIFEST.name}}")

{inner}return df_pred, df_cards
""".rstrip("\n") + "\n"

# 5) Rebuild the function by keeping the 'def ...:' line, replacing everything after it up to func_end
head = src[:m_def.end()]
tail = src[func_end:]
new_src = head + body + tail
p.write_text(new_src, encoding="utf-8")
print("✅ Rebuilt run_weekly_predictions() with clean indentation and manifest extras.")
