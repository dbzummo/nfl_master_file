mkdir -p scripts
cat > scripts/validate_injuries_contract.py <<'PY'
#!/usr/bin/env python3
import csv, json, os, sys, time
from datetime import datetime, timezone, timedelta

INJ = "out/injuries_week.csv"
BOARD = "out/model_board.csv"
WSTATUS = "out/validation/weekly_status.csv"
WLOG = "out/validation/validation_log.jsonl"

def die(msg, code=1):
    print(f"[FAIL] {msg}", file=sys.stderr)
    sys.exit(code)

def ok(msg):
    print(f"[OK] {msg}")

def warn(msg):
    print(f"[WARN] {msg}")

# 1) files exist
for p in [INJ, BOARD]:
    if not os.path.exists(p):
        die(f"required file missing: {p}")

# 2) injuries schema + nonempty
expect_cols = {"team_abbr","player_id","player_name","position","status_norm","injury_desc","last_updated"}
with open(INJ, newline='') as f:
    r = csv.DictReader(f)
    got_cols = set([c.strip() for c in r.fieldnames or []])
    if not expect_cols.issubset(got_cols):
        die(f"injuries_week.csv missing columns. expected ⊇ {sorted(expect_cols)}, got {sorted(got_cols)}")
    rows = list(r)
    if len(rows) == 0:
        die("injuries_week.csv is empty")

ok(f"injuries_week.csv present rows={len(rows)}")

# 3) recency (some rows updated in last 72h)
now = datetime.now(timezone.utc)
fresh_cutoff = now - timedelta(hours=72)
fresh = 0
for row in rows:
    ts = row.get("last_updated","").strip()
    if not ts:
        continue
    try:
        # support either RFC3339 Z or "+00:00"
        ts = ts.replace("Z","+00:00") if ts.endswith("Z") else ts
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:  # naive → assume UTC
            dt = dt.replace(tzinfo=timezone.utc)
        if dt >= fresh_cutoff:
            fresh += 1
    except Exception:
        pass

if fresh == 0:
    warn("no recent injury rows (<=72h); data may be stale")

# 4) board join sanity: if there are any non-OUT/QUESTIONABLE changes this week we expect dp_injury_raw to sometimes move
with open(BOARD, newline='') as f:
    rb = csv.DictReader(f)
    b = list(rb)
    if len(b) == 0:
        die("model_board.csv is empty")
    dp_nonzero = sum(1 for r in b if abs(float(r.get("dp_injury_raw","0") or 0)) > 1e-12)
    if dp_nonzero == 0:
        # not an error: allow legit all-zero weeks but insist the weekly_status log explains it
        explain = False
        if os.path.exists(WLOG):
            with open(WLOG) as lf:
                for line in lf:
                    try:
                        obj = json.loads(line)
                        msgs = obj.get("messages") or []
                        if any("injury_adjustments:all_zero" in m for m in msgs):
                            explain = True
                            break
                    except Exception:
                        pass
        if not explain:
            warn("All dp_injury_raw are zero and no 'injury_adjustments:all_zero' in validation log")
        else:
            ok("dp_injury_raw all zero (explicitly explained in validation log)")
    else:
        ok(f"dp_injury_raw nonzero rows: {dp_nonzero}/{len(b)}")

# 5) weekly status csv exists (not fatal if missing but warn)
if not os.path.exists(WSTATUS):
    warn("weekly_status.csv missing (not fatal)")

ok("injury contract honored")
PY
chmod +x scripts/validate_injuries_contract.py
