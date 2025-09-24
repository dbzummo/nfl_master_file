import re, sys, pathlib

FN = "run_predictions.py"
p = pathlib.Path(FN)
src = p.read_text(encoding="utf-8")

# Ensure the import just once
if "from injuries_fallbacks import derive_injuries_from_rosters" not in src:
    src = src.replace(
        "from run_monte_carlo import run_simulation",
        "from run_monte_carlo import run_simulation\nfrom injuries_fallbacks import derive_injuries_from_rosters"
    )

# Regex to find the STEP 5 print line, capturing its indentation
step5_re = re.compile(r'(?m)^([ \t]*)print\([^\n]*STEP\s*5[^\n]*\)')
m5 = step5_re.search(src)
if not m5:
    print("ERROR: Could not find the STEP 5 print line.")
    sys.exit(2)

indent = m5.group(1)  # exact indent used in your file
start_idx = m5.start()

# Find the start of STEP 6 print line (so we replace up to but not including it)
step6_re = re.compile(r'(?m)^[ \t]*print\([^\n]*STEP\s*6[^\n]*\)')
m6 = step6_re.search(src, m5.end())
if m6:
    end_idx = m6.start()
else:
    # If STEP 6 is missing/mangled, replace only the original STEP 5 line;
    # we'll add our own STEP 6 print in the replacement.
    end_idx = m5.end()
    need_step6 = True

# Build replacement block using the detected indent
rep_lines = [
    f'{indent}print("\\nSTEP 5: Fetching latest injury data (strict)...")',
    f'{indent}injuries = fetch_injured_players()',
    f'{indent}inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0',
    '',
    f'{indent}if inj_ct == 0:',
    f'{indent}    # Live injuries empty: derive conservative unavailability from roster statuses (IR/PUP/NFI/Suspended).',
    f'{indent}    teams_in_play = _pick_teams_from_odds(odds_df)',
    f'{indent}    injuries = derive_injuries_from_rosters(teams_in_play)',
    f'{indent}    inj_ct = len(injuries)',
    f'{indent}    teams_ct = injuries["team_code"].nunique() if inj_ct else 0',
    f'{indent}    print(f"Roster-derived injuries: {{inj_ct}} rows across {{teams_ct}} teams.")',
    '',
    f'{indent}print(f"Found {{inj_ct}} records from injuries (live or fallback).")',
]
# Only add STEP 6 print if it's not present (or was mangled)
if not m6:
    rep_lines += ['', f'{indent}print("\\nSTEP 6: Monte Carlo simulations...")']

replacement = "\n".join(rep_lines) + "\n"

# Splice in the replacement
new_src = src[:start_idx] + replacement + src[end_idx:]
p.write_text(new_src, encoding="utf-8")
print("✅ STEP 5 replaced with indentation preserved; STEP 6 ensured if missing.")
