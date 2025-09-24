import re, sys, pathlib

FN = "run_predictions.py"
p = pathlib.Path(FN)
src = p.read_text(encoding="utf-8")

# 1) Find the end of STEP 4 (depth_df assignment) and capture its indent
m_after_step4 = re.search(r'(?m)^([ \t]*)depth_df\s*=\s*_load_depth_charts\(\s*DEPTH_PATH\s*\)\s*$', src)
if not m_after_step4:
    print("ERROR: couldn't find STEP 4 depth_df assignment line.")
    sys.exit(2)
indent = m_after_step4.group(1)
start_idx = m_after_step4.end()

# 2) Find the first STEP 5 marker after STEP 4
m_step5 = re.search(r'(?m)^[ \t]*print\([^\n]*STEP\s*5[^\n]*\)\s*$', src[start_idx:])
if not m_step5:
    print("ERROR: couldn't find STEP 5 print line.")
    sys.exit(2)
step5_abs = start_idx + m_step5.start()

# 3) Find the simulation call to cap our replacement region
m_sim = re.search(r'(?m)^[ \t]*result\s*=\s*run_simulation\(', src[step5_abs:])
if not m_sim:
    print("ERROR: couldn't find 'result = run_simulation(' after STEP 5.")
    sys.exit(2)
sim_abs = step5_abs + m_sim.start()

# 4) Build canonical STEP 5/6 block with the exact indent
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
    ""
]
replacement = "\n".join(block)

# 5) Splice: keep everything before STEP 5, insert our block, then keep the original simulation onward
new_src = src[:step5_abs] + replacement + src[sim_abs:]
# 6) Clean orphaned 'STEP 6' without print and duplicate injury lines at file scope
new_src = re.sub(r'(?m)^[ \t]*STEP\s*6:.*\)\s*$', '', new_src)
new_src = re.sub(r'(?m)^[ \t]*print\(f"Found {inj_ct} records from injuries.*\)\s*$',
                 lambda m: m.group(0) if 'STEP 6' in new_src[new_src.find(m.group(0))-200:new_src.find(m.group(0))+200] else '',
                 new_src)

p.write_text(new_src, encoding="utf-8")
print("✅ Rebuilt STEP 5/6 region and removed stray lines.")
