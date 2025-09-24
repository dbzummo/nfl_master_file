import re, sys, pathlib

FN = "run_predictions.py"
p = pathlib.Path(FN)
src = p.read_text(encoding="utf-8")

# Ensure import for fallback injuries (harmless if present)
if "from injuries_fallbacks import derive_injuries_from_rosters" not in src:
    src = src.replace(
        "from run_monte_carlo import run_simulation",
        "from run_monte_carlo import run_simulation\nfrom injuries_fallbacks import derive_injuries_from_rosters"
    )

# Find the anchor line that ends STEP 4: the depth charts assignment line
m_after_step4 = re.search(r'(?m)^([ \t]*)depth_df\s*=\s*_load_depth_charts\(\s*DEPTH_PATH\s*\)\s*$', src)
if not m_after_step4:
    print("ERROR: could not locate the STEP 4 depth_df assignment line.")
    sys.exit(2)

indent = m_after_step4.group(1)  # correct indent for this function body
start_idx = m_after_step4.end()

# Find the 'print(' line that introduces STEP 7 to cap the region we replace
m_step7_print = re.search(r'(?m)^[ \t]*print\([^\\n]*STEP\s*7[^\\n]*\)', src[start_idx:])
if not m_step7_print:
    print("ERROR: could not locate STEP 7 print line after STEP 4.")
    sys.exit(2)

end_idx = start_idx + m_step7_print.start()

# Build canonical STEP 5/6 block using the detected indent
block = [
    f'{indent}print("\\nSTEP 5: Fetching latest injury data (strict)...")',
    f'{indent}injuries = fetch_injured_players()',
    f'{indent}inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0',
    "",
    f'{indent}if inj_ct == 0:',
    f'{indent}    # Live injuries empty: derive conservative unavailability from roster statuses (IR/PUP/NFI/Suspended).',
    f'{indent}    teams_in_play = _pick_teams_from_odds(odds_df)',
    f'{indent}    injuries = derive_injuries_from_rosters(teams_in_play)',
    f'{indent}    inj_ct = len(injuries)',
    f'{indent}    teams_ct = injuries["team_code"].nunique() if inj_ct else 0',
    f'{indent}    print(f"Roster-derived injuries: {{inj_ct}} rows across {{teams_ct}} teams.")',
    "",
    f'{indent}print(f"Found {{inj_ct}} records from injuries (live or fallback).")',
    "",
    f'{indent}print("\\nSTEP 6: Monte Carlo simulations...")',
]

new_src = src[:start_idx] + "\n" + "\n".join(block) + "\n\n" + src[end_idx:]
p.write_text(new_src, encoding="utf-8")
print("✅ Rebuilt STEP 5/6 region with correct indentation; preserved STEP 7 and beyond.")
