#!/usr/bin/env python3
"""
repair_step5_6.py — one-shot repair for run_predictions.py STEP-5/6 region
Use this if indentation, stray lines, or broken prints sneak back in.
"""
import re, sys, pathlib, shutil, time

FN = "run_predictions.py"
p = pathlib.Path(FN)
src = p.read_text(encoding="utf-8")

# Backup with timestamp
backup = f"{FN}.bak.{int(time.time())}"
shutil.copy(FN, backup)
print(f"📦 Backup saved as {backup}")

# Ensure fallback import
if "from injuries_fallbacks import derive_injuries_from_rosters" not in src:
    src = src.replace(
        "from run_monte_carlo import run_simulation",
        "from run_monte_carlo import run_simulation\nfrom injuries_fallbacks import derive_injuries_from_rosters"
    )

# Find STEP-4 anchor
m_after_step4 = re.search(r'(?m)^([ \t]*)depth_df\s*=\s*_load_depth_charts\(\s*DEPTH_PATH\s*\)\s*$', src)
if not m_after_step4:
    sys.exit("❌ Could not locate STEP-4 depth_df assignment line.")
indent = m_after_step4.group(1)
start_idx = m_after_step4.end()

# Find STEP-5 print
m_step5 = re.search(r'(?m)^[ \t]*print\([^\n]*STEP\s*5[^\n]*\)\s*$', src[start_idx:])
if not m_step5:
    sys.exit("❌ Could not locate STEP-5 print line.")
step5_abs = start_idx + m_step5.start()

# Find simulation call to cap region
m_sim = re.search(r'(?m)^[ \t]*result\s*=\s*run_simulation\(', src[step5_abs:])
if not m_sim:
    sys.exit("❌ Could not locate run_simulation call.")
sim_abs = step5_abs + m_sim.start()

# Canonical block
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

# Rebuild file
new_src = src[:step5_abs] + replacement + src[sim_abs:]
# Clean any orphan STEP-6 text without print
new_src = re.sub(r'(?m)^[ \t]*STEP\s*6:.*\)\s*$', '', new_src)

p.write_text(new_src, encoding="utf-8")
print("✅ STEP-5/6 region repaired successfully.")
