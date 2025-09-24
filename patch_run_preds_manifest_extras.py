import hashlib, json, pathlib, re, sys

fn = pathlib.Path("run_predictions.py")
src = fn.read_text(encoding="utf-8")

# Ensure we import hashlib at top
if "import hashlib" not in src:
    src = src.replace("import os, sys, json, time, socket, platform",
                      "import os, sys, json, time, socket, platform, hashlib")

# 1) Track injury source around STEP 5. We look for our STEP 5 block and add tracking vars.
# Add defaults near the start of run_weekly_predictions function body (after the "cfg = _load_json" line)
src = re.sub(
    r'(cfg\s*=\s*_load_json\(CONFIG_PATH\)\n\s*require_env\(os\.environ,\s*REQUIRED_ENV\)\n)',
    r'\1' +
    "    injury_source = 'live'\n"
    "    injuries_live_count = 0\n"
    "    injuries_fallback_count = 0\n",
    src, count=1
)

# After initial injuries fetch, set live count; if fallback path executes, flip source and set counts
# Find the line 'injuries = fetch_injured_players()' inside STEP 5 block
src = re.sub(
    r'(\n\s*injuries\s*=\s*fetch_injured_players\(\)\n\s*inj_ct\s*=\s*len\(injuries\)[^\n]*\n)',
    r'\1' +
    "    injuries_live_count = inj_ct\n",
    src, count=1
)

# In the fallback block (if inj_ct == 0), set injury_source and fallback count
src = re.sub(
    r'(\n\s*if\s+inj_ct\s*==\s*0:\n(?:.|\n)*?inj_ct\s*=\s*len\(injuries\)\n)',
    r"\1" +
    "    injury_source = 'fallback'\n"
    "    injuries_fallback_count = inj_ct\n",
    src, count=1
)

# 2) Compute config hash once (after cfg loaded). We'll add a helper right after cfg load.
if "config_hash =" not in src:
    src = re.sub(
        r'(cfg\s*=\s*_load_json\(CONFIG_PATH\)\n)',
        r"\1" +
        "    try:\n"
        "        config_hash = hashlib.sha256(Path(CONFIG_PATH).read_bytes()).hexdigest()\n"
        "    except Exception:\n"
        "        config_hash = None\n",
        src, count=1
    )

# 3) Provide a sigma policy name (we'll wire real policy later). Default: constant.
if "sigma_policy_name" not in src:
    src = src.replace(
        'print("\\nSTEP 6: Monte Carlo simulations...")',
        'sigma_policy_name = "constant"\n'
        '    print("\\nSTEP 6: Monte Carlo simulations...")'
    )

# 4) Extend write_manifest(...) call to pass extras
# Find the write_manifest call and add a fourth param 'extras' or merge into dict before call
# We will build an 'extras' dict right above the write_manifest call and pass it.
m = re.search(r'(?m)^\s*write_manifest\(\s*OUT_MANIFEST,\s*run_meta,\s*inputs,\s*outputs,\s*audits\s*\)', src)
if m:
    # Build extras dict before the call
    insert_pos = src.rfind('\n', 0, m.start())+1
    extras_block = (
        "    extras = {\n"
        "        'injury_source': injury_source,\n"
        "        'injuries_live_count': injuries_live_count,\n"
        "        'injuries_fallback_count': injuries_fallback_count,\n"
        "        'sigma_policy': sigma_policy_name,\n"
        "        'config_hash': config_hash\n"
        "    }\n"
    )
    if extras_block not in src:
        src = src[:insert_pos] + extras_block + src[insert_pos:]
    # Now modify the call to include extras if not already present at the end
    src = src.replace(
        "write_manifest(OUT_MANIFEST, run_meta, inputs, outputs, audits)",
        "write_manifest(OUT_MANIFEST, run_meta, inputs, outputs, audits, extras)"
    )

fn.write_text(src, encoding="utf-8")
print("Patched run_predictions.py: added extras (injury_source, counts, sigma_policy, config_hash) to manifest.")
