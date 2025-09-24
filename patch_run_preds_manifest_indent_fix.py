import re, pathlib, sys

p = pathlib.Path("run_predictions.py")
src = p.read_text(encoding="utf-8")

def ensure_sigma_before_step6(s: str) -> str:
    """
    Ensure a correctly-indented line
        <indent>sigma_policy_name = "constant"
    appears immediately before:
        <indent>print("\\nSTEP 6: Monte Carlo simulations...")
    """
    step6_re = re.compile(r'(?m)^([ \t]*)print\("\\\\nSTEP 6: Monte Carlo simulations')
    m = step6_re.search(s)
    if not m:
        return s
    indent = m.group(1)
    # Look at the previous non-empty line before the STEP 6 print
    start = m.start()
    prefix = s[:start]
    # Find last newline before STEP6
    last_nl = prefix.rfind("\n", 0, len(prefix)-1)
    # Find second-to-last newline to isolate previous line
    prev_nl = prefix.rfind("\n", 0, last_nl)
    prev_line = prefix[prev_nl+1:last_nl] if last_nl != -1 else prefix
    # If previous line already defines sigma_policy_name, do nothing
    if re.search(r'\bsigma_policy_name\s*=\s*["\']', prev_line):
        return s
    # Insert our assignment with the same indent
    insertion = f'{indent}sigma_policy_name = "constant"\n'
    return s[:start] + insertion + s[start:]

def ensure_extras_block_before_manifest(s: str) -> str:
    """
    Ensure an 'extras = {...}' dict appears right before the write_manifest(...) call,
    with proper indentation matching the call.
    """
    call_re = re.compile(
        r'(?m)^([ \t]*)write_manifest\(\s*OUT_MANIFEST\s*,\s*run_meta\s*,\s*inputs\s*,\s*outputs\s*,\s*audits(?:\s*,\s*extras)?\s*\)'
    )
    m = call_re.search(s)
    if not m:
        return s
    indent = m.group(1)
    call_start = m.start()

    # Build extras block with matching indent
    inner = indent + "    "
    extras_block = (
        f"{indent}extras = {{\n"
        f"{inner}'injury_source': injury_source,\n"
        f"{inner}'injuries_live_count': injuries_live_count,\n"
        f"{inner}'injuries_fallback_count': injuries_fallback_count,\n"
        f"{inner}'sigma_policy': sigma_policy_name,\n"
        f"{inner}'config_hash': config_hash\n"
        f"{indent}}}\n"
    )

    # If an extras block already exists immediately above, skip insert
    above = s[:call_start]
    # Look back a little window to avoid multiple inserts
    window_start = max(0, call_start - 600)
    recent = above[window_start:call_start]
    if "extras = {" not in recent:
        s = s[:call_start] + extras_block + s[call_start:]

    # Ensure the call includes the extras argument
    s = s[:m.start()] + re.sub(r'\)\s*$', ', extras)', s[m.start():m.end()]) + s[m.end():]
    return s

def dedupe_bad_sigma_insertions(s: str) -> str:
    """
    If a previous patch accidentally created a mis-indented insertion like:
       sigma_policy_name = "constant"
           print("STEP 6...")
    we normalize by removing any standalone 'sigma_policy_name' placed outside the function indent.
    This is conservative; we only remove lines that are *not* indented with spaces/tabs.
    """
    # Remove lines starting at column 0 that define sigma_policy_name
    return re.sub(r'(?m)^(?!!)(sigma_policy_name\s*=\s*["\'].*)$', r'# \1  # removed stray global sigma line', s)

# 1) Ensure sigma line has correct indent immediately before STEP 6 print
src = ensure_sigma_before_step6(src)

# 2) Ensure extras dict is correctly indented and passed to write_manifest
src = ensure_extras_block_before_manifest(src)

# 3) Clean up any stray unindented sigma lines that might cause IndentationError elsewhere
src = dedupe_bad_sigma_insertions(src)

p.write_text(src, encoding="utf-8")
print("✅ Fixed indentation for sigma_policy and extras block.")
