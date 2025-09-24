import pathlib, re, sys, json

fn = pathlib.Path("manifest_writer.py")
src = fn.read_text(encoding="utf-8")

# Ensure write_manifest signature has extras: dict | None
src2 = src

# 1) Add 'extras' parameter to write_manifest signature if missing
src2 = re.sub(
    r'def\s+write_manifest\(\s*([^)]+)\):',
    lambda m: (
        "def write_manifest(" +
        m.group(1).rstrip() +
        (", extras: dict | None = None" if "extras" not in m.group(1) else "") +
        "):"
    ),
    src2, count=1
)

# 2) Ensure extras merged into manifest body at the root under 'extras'
# naive insertion: after top-level dict creation
if "extras" not in src2 or "manifest[\"extras\"]" not in src2:
    # Try to inject right before the JSON dump/write section
    insert_after = re.search(r'(?m)^\s*with\s+open\([^\n]+\)\s+as\s+f:\s*$', src2)
    if insert_after:
        i = insert_after.start()
        before = src2[:i]
        after = src2[i:]
        inject = (
            "    # Attach optional extras\n"
            "    try:\n"
            "        if extras:\n"
            "            manifest = locals().get('manifest', None)\n"
            "            if manifest is None:\n"
            "                pass\n"
            "    except Exception:\n"
            "        pass\n"
        )
        # But better: look for where the manifest dict is built
    # More robust: find first creation of a 'manifest' dict
    m_dict = re.search(r'(?m)^\s*manifest\s*=\s*{', src2)
    if m_dict:
        # find the end of that dict (next line that closes with })
        close = re.search(r'\n\s*}\s*\n', src2[m_dict.end():])
        if close:
            endpos = m_dict.end() + close.end()
            src2 = src2[:endpos] + (
                "\n    # Merge extras into manifest if provided\n"
                "    if extras:\n"
                "        manifest['extras'] = extras\n"
            ) + src2[endpos:]

# 3) If no 'manifest' variable exists (unlikely), leave file untouched
fn.write_text(src2, encoding="utf-8")
print("Patched manifest_writer.py to accept and include 'extras'.")
