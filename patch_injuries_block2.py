import sys, pathlib

STEP5 = "STEP 5"
STEP6 = "STEP 6"

p = pathlib.Path("run_predictions.py")
src = p.read_text(encoding="utf-8")

# Ensure the import
if "from injuries_fallbacks import derive_injuries_from_rosters" not in src:
    src = src.replace(
        "from run_monte_carlo import run_simulation",
        "from run_monte_carlo import run_simulation\nfrom injuries_fallbacks import derive_injuries_from_rosters"
    )

# Find boundaries by the literal markers
i5 = src.find(STEP5)
i6 = src.find(STEP6)
if i5 == -1 or i6 == -1 or i6 <= i5:
    print("Could not find STEP 5 / STEP 6 markers – aborting.")
    sys.exit(1)

# Walk backwards from STEP 5 to the beginning of that print line
line_start = src.rfind("print(", 0, i5)
if line_start == -1:
    print("Could not find the print( line that introduces STEP 5 – aborting.")
    sys.exit(1)

prefix = src[:line_start]
suffix = src[i6:]  # keep the STEP 6 print and everything after

replacement = (
    'print("\\nSTEP 5: Fetching latest injury data (strict)...")\n'
    'injuries = fetch_injured_players()\n'
    'inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0\n'
    '\n'
    'if inj_ct == 0:\n'
    '    # Live injuries empty: derive conservative unavailability from roster statuses (IR/PUP/NFI/Suspended).\n'
    '    teams_in_play = _pick_teams_from_odds(odds_df)\n'
    '    injuries = derive_injuries_from_rosters(teams_in_play)\n'
    '    inj_ct = len(injuries)\n'
    '    teams_ct = injuries["team_code"].nunique() if inj_ct else 0\n'
    '    print(f"Roster-derived injuries: {inj_ct} rows across {teams_ct} teams.")\n'
    '\n'
    'print(f"Found {inj_ct} records from injuries (live or fallback).")\n'
    '\n'
)

p.write_text(prefix + replacement + suffix, encoding="utf-8")
print("✅ Replaced STEP 5 block using marker boundaries.")
