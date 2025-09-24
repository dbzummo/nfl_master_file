import io, re, sys, pathlib

p = pathlib.Path("validators.py")
src = p.read_text(encoding="utf-8")

# Remove ALL occurrences of the future import so we can reinsert it once in the right spot.
future_re = re.compile(r'^[ \t]*from[ \t]+__future__[ \t]+import[ \t]+annotations[ \t]*\r?\n', re.MULTILINE)
src_wo_future = future_re.sub('', src)

# Split into lines to detect shebang and an initial module docstring.
lines = src_wo_future.splitlines(keepends=True)

i = 0
out = []

# 1) Preserve shebang if present on very first line (e.g., #!/usr/bin/env python3)
if lines and lines[0].startswith("#!"):
    out.append(lines[0]); i = 1
    # keep immediate following newline-only lines
    while i < len(lines) and lines[i].strip() == "":
        out.append(lines[i]); i += 1

# 2) Preserve module docstring if it is the first non-empty thing
def starts_triple_quote(s: str) -> bool:
    s = s.lstrip()
    return s.startswith('"""') or s.startswith("'''")

def find_docstring_end(idx: int) -> int:
    # returns end index *inclusive* of closing triple quotes; else -1
    if idx >= len(lines): return -1
    text = ''.join(lines[idx:])
    m = re.match(r'\s*([\'"]{3})(?:.|\n)*?\1', text, re.DOTALL)
    if not m: return -1
    end_pos = m.end()
    # compute line index where it ends
    consumed = text[:end_pos]
    consumed_lines = consumed.splitlines(keepends=True)
    return idx + len(consumed_lines) - 1

# Skip blank/comment lines to check docstring start
j = i
while j < len(lines) and lines[j].strip() == "":
    j += 1
ds_end = -1
if j < len(lines) and starts_triple_quote(lines[j]):
    ds_start = j
    ds_end = find_docstring_end(ds_start)
    if ds_end != -1:
        out.extend(lines[i:ds_end+1])
        i = ds_end + 1

# 3) Insert the future import exactly once, then the rest
# Ensure a newline before/after for cleanliness
if not out or (out and not out[-1].endswith('\n')):
    out.append('\n')
out.append('from __future__ import annotations\n')
# Ensure a blank line after future import unless next chunk already has one
if i < len(lines) and lines[i].strip() != "":
    out.append('\n')

out.extend(lines[i:])

new_src = ''.join(out)

# No-op if already identical (but we likely changed it)
if new_src != src:
    p.write_text(new_src, encoding="utf-8")

print("✅ validators.py: moved 'from __future__ import annotations' to the top.")
