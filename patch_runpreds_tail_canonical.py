import re, pathlib, sys

p = pathlib.Path("run_predictions.py")
src = p.read_text(encoding="utf-8")

# 1) Find the function start (def run_weekly_predictions)
m_def = re.search(r'(?m)^def\s+run_weekly_predictions\s*\(\s*\)\s*:', src)
if not m_def:
    sys.exit("❌ Could not find `def run_weekly_predictions()`.")

# 2) From there, find the STEP 7 print line we want to replace forward from
m_step7 = re.search(r'(?m)^[ \t]*print\(\s*["\']\\nSTEP 7: Writing artifacts', src[m_def.start():])
if not m_step7:
    sys.exit("❌ Could not find `print(\"\\nSTEP 7: Writing artifacts…\")` inside the function.")

step7_abs = m_def.start() + m_step7.start()

# 3) Find the end of the function: next unindented 'def ' or the module tail
m_next_def = re.search(r'(?m)^\s*def\s+\w+\s*\(', src[step7_abs:])
m_main_guard = re.search(r'(?m)^\s*if\s+__name__\s*==\s*["\']__main__["\']\s*:', src[step7_abs:])
candidates = [m for m in [m_next_def, m_main_guard] if m]
if candidates:
    end_abs = step7_abs + min(m.start() for m in candidates)
else:
    end_abs = len(src)

# 4) Determine indent from the STEP 7 line
line_start = src.rfind("\n", 0, step7_abs) + 1
line = src[line_start: step7_abs]
indent = re.match(r'[ \t]*', line).group(0)
inner = indent + "    "

# 5) Build canonical tail block: STEP 7 saves, manifest (run_meta/inputs/outputs/audits), extras, write_manifest, prints, return
tail = (
    f'{indent}print("\\nSTEP 7: Writing artifacts...")\n'
    f'{indent}df_pred.to_csv(OUT_PREDS, index=False)\n'
    f'{indent}if not df_cards.empty:\n'
    f'{inner}df_cards.to_csv(OUT_CARDS, index=False)\n'
    f'\n'
    f'{indent}# Manifest\n'
    f'{indent}run_meta = {{\n'
    f'{inner}"runner": platform.node(),\n'
    f'{inner}"timestamp_utc": pd.Timestamp.utcnow().isoformat(),\n'
    f'{inner}"python": platform.python_version(),\n'
    f'{inner}"host": socket.gethostname(),\n'
    f'{inner}"config_used": str(CONFIG_PATH.name)\n'
    f'{indent}}}\n'
    f'{indent}inputs = {{\n'
    f'{inner}"ratings_csv": str(RATINGS_PATH.name),\n'
    f'{inner}"stadium_hfa_csv": str(HFA_PATH.name),\n'
    f'{inner}"depth_charts_csv": str(DEPTH_PATH.name),\n'
    f'{inner}"odds_provider": "TheOddsAPI",\n'
    f'{inner}"injury_provider": "SportsDataIO (or configured provider)"\n'
    f'{indent}}}\n'
    f'{indent}outputs = {{\n'
    f'{inner}"predictions_csv": str(OUT_PREDS.name),\n'
    f'{inner}"gamecards_csv": str(OUT_CARDS.name) if OUT_CARDS.exists() else None\n'
    f'{indent}}}\n'
    f'{indent}audits = {{\n'
    f'{inner}"roster_audit": audit_log\n'
    f'{indent}}}\n'
    f'\n'
    f'{indent}# Extras for provenance\n'
    f'{indent}extras = {{\n'
    f'{inner}"injury_source": injury_source if "injury_source" in locals() else "live",\n'
    f'{inner}"injuries_live_count": injuries_live_count if "injuries_live_count" in locals() else 0,\n'
    f'{inner}"injuries_fallback_count": injuries_fallback_count if "injuries_fallback_count" in locals() else 0,\n'
    f'{inner}"sigma_policy": sigma_policy_name if "sigma_policy_name" in locals() else "constant",\n'
    f'{inner}"config_hash": config_hash if "config_hash" in locals() else None\n'
    f'{indent}}}\n'
    f'\n'
    f'{indent}write_manifest(OUT_MANIFEST, run_meta, inputs, outputs, audits, extras)\n'
    f'\n'
    f'{indent}print(f"\\nSaved: {{OUT_PREDS.name}}" + (f", {{OUT_CARDS.name}}" if OUT_CARDS.exists() else ""))\n'
    f'{indent}print(f"Saved: {{OUT_MANIFEST.name}}")\n'
    f'\n'
    f'{indent}return df_pred, df_cards\n'
)

# 6) Splice new tail into file
new_src = src[:step7_abs] + tail + src[end_abs:]
p.write_text(new_src, encoding="utf-8")
print("✅ Replaced STEP 7 tail with a canonical, consistently-indented block (including manifest extras).")
