import pathlib, sys

FN = "run_predictions.py"
p = pathlib.Path(FN)
src = p.read_text(encoding="utf-8")

# 0) Make sure the import exists (place it after run_monte_carlo import)
if "from injuries_fallbacks import derive_injuries_from_rosters" not in src:
    anchor = "from run_monte_carlo import run_simulation"
    if anchor in src:
        src = src.replace(
            anchor,
            anchor + "\nfrom injuries_fallbacks import derive_injuries_from_rosters"
        )

# 1) Find STEP 5 and STEP 6 markers
i5 = src.find("STEP 5")
i6 = src.find("STEP 6")
if i5 == -1:
    print("ERROR: Could not find 'STEP 5' marker.")
    sys.exit(2)
if i6 == -1 or i6 <= i5:
    # If STEP 6 got mangled, we will reinsert it after our replacement.
    i6 = -1

# 2) Find the start-of-line 'print(' that introduces STEP 5
line_start = src.rfind("print(", 0, i5)
if line_start == -1:
    print("ERROR: Could not find the print( line that introduces STEP 5.")
    sys.exit(2)

# 3) Compute the prefix/suffix cut points
prefix = src[:line_start]
suffix = src[i6:] if i6 != -1 else src[line_start:]  # if STEP 6 missing, we will add our own tail

# 4) Build the clean STEP 5 block (ASCII only; 4 spaces indentation)
replacement = (
    '    print("\\nSTEP 5: Fetching latest injury data (strict)...")\n'
    '    injuries = fetch_injured_players()\n'
    '    inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0\n'
    '\n'
    '    if inj_ct == 0:\n'
    '        # Live injuries empty: derive conservative unavailability from roster statuses (IR/PUP/NFI/Suspended).\n'
    '        teams_in_play = _pick_teams_from_odds(odds_df)\n'
    '        injuries = derive_injuries_from_rosters(teams_in_play)\n'
    '        inj_ct = len(injuries)\n'
    '        teams_ct = injuries["team_code"].nunique() if inj_ct else 0\n'
    '        print(f"Roster-derived injuries: {inj_ct} rows across {teams_ct} teams.")\n'
    '\n'
    '    print(f"Found {inj_ct} records from injuries (live or fallback).")\n'
    '\n'
    '    print("\\nSTEP 6: Monte Carlo simulations...")\n'
)

# 5) If STEP 6 existed, keep everything from its print onward; otherwise our replacement includes STEP 6 print
if i6 != -1:
    # Keep the original STEP 6 print(...) and after.
    # Find the exact 'print(' that contains STEP 6 to avoid double-printing
    s6_print_idx = src.rfind("print(", 0, i6)
    if s6_print_idx != -1:
        suffix = src[s6_print_idx:]

# 6) Write
p.write_text(prefix + replacement + suffix, encoding="utf-8")
print("✅ Patched STEP 5 block and ensured STEP 6 print + import.")
