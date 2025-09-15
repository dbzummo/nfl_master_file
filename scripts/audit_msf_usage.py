#!/usr/bin/env python3
import os, re, sys, csv, subprocess
from pathlib import Path

OUTDIR = Path("out/_audit")
OUTDIR.mkdir(parents=True, exist_ok=True)
URLS = OUTDIR / "msf_urls.txt"
SUMMARY = OUTDIR / "msf_summary.csv"
FLAGS = OUTDIR / "msf_flags.txt"
AUTH = OUTDIR / "msf_auth_patterns.txt"

print("[msf-audit] scanning for MSF endpoints…", file=sys.stderr)

# 1) Collect candidate files (prefer git; fallback to walk)
files = []
try:
    out = subprocess.run(["git","ls-files"], check=True, capture_output=True, text=True).stdout.splitlines()
    files = [Path(p) for p in out if Path(p).suffix.lower() in {".py",".sh",".bash",".zsh",".md",".txt",".yaml",".yml",".json",""} or Path(p).name.lower()=="makefile"]
except Exception:
    for root, _, fnames in os.walk("."):
        for f in fnames:
            p = Path(root) / f
            if p.suffix.lower() in {".py",".sh",".bash",".zsh",".md",".txt",".yaml",".yml",".json",""} or p.name.lower()=="makefile":
                files.append(p)

url_re = re.compile(r'https?://[^"\')\s]+', re.I)
host_re = re.compile(r'https?://[^/]*mysportsfeeds\.com', re.I)

found = []
for p in files:
    try:
        with open(p, "r", errors="ignore") as fh:
            for i, line in enumerate(fh, 1):
                if "mysportsfeeds.com" in line:
                    for url in url_re.findall(line):
                        if host_re.match(url):
                            found.append((str(p), i, line.rstrip("\n"), url))
    except Exception:
        continue

with open(URLS, "w", encoding="utf-8") as fh:
    for f, ln, src, url in found:
        fh.write(f"{f}:{ln}: {url}\n")

# 2) Summarize paths + query keys
from urllib.parse import urlparse, parse_qs

summary_rows = []
for _, _, _, url in found:
    u = urlparse(url)
    path = u.path
    qkeys = ",".join(sorted(parse_qs(u.query).keys())) if u.query else "none"
    summary_rows.append((path, qkeys))

# unique with counts
from collections import Counter
cnt = Counter(summary_rows)
with open(SUMMARY, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["count","path","query_keys"])
    for (path,qk), c in sorted(cnt.items(), key=lambda x: (-x[1], x[0][0])):
        w.writerow([c, path, qk])

# 3) Flag likely addon/forbidden patterns
flag_terms = re.compile(r'boxscore|playbyplay|gamelog|lineups|injur|/v1\.|latest\b|current\b|upcoming\b', re.I)
with open(FLAGS, "w", encoding="utf-8") as fh:
    any_flag = False
    for f, ln, src, url in found:
        if flag_terms.search(src) or flag_terms.search(url):
            any_flag = True
            fh.write(f"{f}:{ln}: {src.strip()}\n")
    if not any_flag:
        fh.write("(none found)\n")

# 4) Auth usage patterns
auth_hits = []
auth_patterns = [
    re.compile(r'MYSPORTSFEEDS', re.I),
    re.compile(r'curl .* -u ', re.I),
    re.compile(r'Authorization:\s*Basic', re.I),
    re.compile(r'requests\.get\(', re.I),
]
for p in files:
    try:
        with open(p, "r", errors="ignore") as fh:
            for i, line in enumerate(fh, 1):
                if any(r.search(line) for r in auth_patterns):
                    auth_hits.append(f"{p}:{i}: {line.strip()}")
    except Exception:
        continue
with open(AUTH, "w", encoding="utf-8") as fh:
    if auth_hits:
        fh.write("\n".join(auth_hits) + "\n")
    else:
        fh.write("(no explicit patterns found)\n")

print("== SUMMARY (CSV) ==", file=sys.stderr)
print(SUMMARY.read_text(), file=sys.stderr)
print("== POSSIBLE FLAGS (review carefully) ==", file=sys.stderr)
print(FLAGS.read_text(), file=sys.stderr)
print("== AUTH PATTERNS (verify correct usage) ==", file=sys.stderr)
print(AUTH.read_text(), file=sys.stderr)
print(f"[msf-audit] done. Files written to: {OUTDIR}", file=sys.stderr)
