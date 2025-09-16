#!/usr/bin/env bash
set -euo pipefail

# ===============================
# Phase 0 Runner — Week Freeze & Reproducibility
# - Runs WEEK (1 or 2) in a clean git worktree at the current commit
# - Resolves window from config/week_windows_2025.json
# - Calls your existing pipeline (make all)
# - Verifies finals counts (W1=16, W2=13), enforces bit-for-bit reproducibility
# - Installs per-week artifacts into out/<WEEK_TAG>/ and reports/<WEEK_TAG>/
# ===============================

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIG="config/week_windows_2025.json"
[[ -f "$CONFIG" ]] || { echo "[FATAL] Missing $CONFIG"; exit 1; }

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "[FATAL] Git working tree must be clean."; exit 1
fi

if [[ -z "${WEEK:-}" ]]; then
  echo "[FATAL] Usage: WEEK=1 ./scripts/phase0_run.sh"; exit 1
fi

# Resolve window
START="$(jq -r --arg w "$WEEK" '.[$w].start' "$CONFIG")"
END="$(jq -r --arg w "$WEEK" '.[$w].end' "$CONFIG")"
SEASON="$(jq -r --arg w "$WEEK" '.[$w].season' "$CONFIG")"
WEEK_TAG="$(jq -r --arg w "$WEEK" '.[$w].week_tag' "$CONFIG")"
EXPECTED_FINALS="$(jq -r --arg w "$WEEK" '.[$w].expected_finals' "$CONFIG")"

for v in START END SEASON WEEK_TAG EXPECTED_FINALS; do
  [[ -n "${!v}" && "${!v}" != "null" ]] || { echo "[FATAL] Missing $v in $CONFIG for week $WEEK"; exit 1; }
done

# checksum tool
if command -v sha256sum >/dev/null 2>&1; then SHACMD="sha256sum"; else SHACMD="shasum -a 256"; fi

OUT_DIR="$REPO_ROOT/out/$WEEK_TAG"
REPORTS_DIR="$REPO_ROOT/reports/$WEEK_TAG"
ART_DIR="$REPO_ROOT/artifacts/phase0"
mkdir -p "$OUT_DIR" "$REPORTS_DIR" "$ART_DIR"

WORKTREES_DIR="$REPO_ROOT/.phase0_worktrees"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
WT_PATH="$WORKTREES_DIR/$WEEK_TAG-$RUN_ID"
mkdir -p "$WORKTREES_DIR"

GIT_SHA="$(git rev-parse HEAD)"
echo "[INFO] Creating worktree at $WT_PATH (commit $GIT_SHA)"
git worktree add --detach "$WT_PATH" "$GIT_SHA" >/dev/null

cleanup() {
  echo "[INFO] Cleaning worktree $WT_PATH"
  git worktree remove --force "$WT_PATH" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_once() {
  local pass="$1"  # pass1 | pass2
  local tmpdir="$WT_PATH/.tmp_phase0_$pass"
  local runlog="$ART_DIR/${WEEK_TAG}_${pass}.log"
  mkdir -p "$tmpdir"

  pushd "$WT_PATH" >/dev/null
  rm -rf out reports || true
  mkdir -p out reports out/results out/msf_details

  export START="$START" END="$END" SEASON="$SEASON"

  {
    echo "[RUN] $(date -u +%FT%TZ) pass=$pass week=$WEEK tag=$WEEK_TAG start=$START end=$END sha=$GIT_SHA"
    echo "[RUN] Python: $(python3 -V || true)"
    echo "[RUN] Make:   $(make -v | head -1 || true)"

    # Ensure schedule matches the window (prevents W1/W2 cross-contamination)
    if [[ -x scripts/fetch_msf_week.py ]]; then
      echo "[RUN] fetch schedule for window $START..$END"
      python3 scripts/fetch_msf_week.py --start "$START" --end "$END" --season "$SEASON"
    else
      echo "[WARN] scripts/fetch_msf_week.py not found; assuming pipeline produces msf_week.csv"
    fi

    # Finals for this window (if pipeline doesn't produce it, this populates out/results/finals.csv)
    if [[ -x scripts/finals_for_window.py ]]; then
      echo "[RUN] build finals for window"
      python3 scripts/finals_for_window.py
    fi

    echo "[RUN] make all"
    make all

    # Verify finals count
    FINALS="out/results/finals.csv"
    [[ -f "$FINALS" ]] || { echo "[FATAL] finals.csv not found"; exit 70; }
    rows="$(wc -l < "$FINALS" | tr -d ' ')"
    if head -1 "$FINALS" | grep -qi '^game_id'; then rows=$((rows-1)); fi
    echo "[RUN] finals rows=$rows (expected $EXPECTED_FINALS)"
    [[ "$rows" -eq "$EXPECTED_FINALS" ]] || { echo "[FATAL] Finals count mismatch ($rows != $EXPECTED_FINALS)"; exit 71; }

    # Optional sanity: msf_week.csv dates inside window
    if [[ -f out/msf_details/msf_week.csv ]]; then
      bad=$(awk -F, -v s="$START" -v e="$END" 'NR>1{gsub("-","",$1); if($1 < substr(s,1,8) || $1 > substr(e,1,8)) c++} END{print c+0}' out/msf_details/msf_week.csv)
      [[ "$bad" -eq 0 ]] || { echo "[FATAL] msf_week.csv contains dates outside $START..$END"; exit 72; }
    fi

    # Collect artifacts
    mkdir -p "$tmpdir/out" "$tmpdir/reports"
    rsync -a out/ "$tmpdir/out/"
    rsync -a reports/ "$tmpdir/reports/"

    # Checksums (deterministic order)
    (
      cd "$tmpdir"
      : > checksums.txt
      find out -type f -print0 | sort -z | xargs -0 $SHACMD >> checksums.txt
      find reports -type f -print0 | sort -z | xargs -0 $SHACMD >> checksums.txt
    )

    # Run manifest
    {
      printf '{\n'
      printf '  "week": %s,\n' "$WEEK"
      printf '  "week_tag": "%s",\n' "$WEEK_TAG"
      printf '  "window": {"start":"%s","end":"%s"},\n' "$START" "$END"
      printf '  "season": "%s",\n' "$SEASON"
      printf '  "git_sha": "%s",\n' "$GIT_SHA"
      printf '  "timestamps": {"run_started":"%s","run_finished":"%s"},\n' "$(date -u +%FT%TZ)" "$(date -u +%FT%TZ)"
      printf '  "row_counts": {"finals": %s}\n' "$rows"
      printf '}\n'
    } > "$tmpdir/run_manifest.json"

    echo "[RUN] ($pass) completed."
  } | tee "$runlog"

  popd >/dev/null
  echo "$tmpdir"
}

pass1_dir="$(run_once pass1)"
pass2_dir="$(run_once pass2)"

echo "[INFO] Compare checksums…"
if ! diff -u "$pass1_dir/checksums.txt" "$pass2_dir/checksums.txt" >/dev/null; then
  echo "[FATAL] Reproducibility FAILED: checksums differ between pass1 and pass2"
  diff -u "$pass1_dir/checksums.txt" "$pass2_dir/checksums.txt" || true
  exit 73
fi
echo "[OK] Reproducibility PASSED."

echo "[INFO] Install per-week artifacts (dereference symlinks)…"
# -L to copy file contents instead of symlinks (no masking state)
rsync -aL --delete "$pass1_dir/out/" "$OUT_DIR/"
rsync -aL --delete "$pass1_dir/reports/" "$REPORTS_DIR/"

cp -f "$pass1_dir/run_manifest.json" "$OUT_DIR/run_manifest.json"
cp -f "$pass1_dir/checksums.txt" "$OUT_DIR/checksums.txt"

# Ensure no symlinks ended up in the week partition
if find "$OUT_DIR" "$REPORTS_DIR" -type l | grep -q .; then
  echo "[FATAL] Symlinks detected in per-week artifacts (masking state)."; exit 74
fi

echo "[DONE] Week $WEEK ($WEEK_TAG) installed."
