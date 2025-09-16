#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIG="config/week_windows_2025.json"
[[ -f "$CONFIG" ]] || { echo "[FATAL] Missing $CONFIG"; exit 1; }

# Clean tree required
git diff --quiet && git diff --cached --quiet || { echo "[FATAL] Git working tree must be clean."; exit 1; }

: "${WEEK:?Usage: WEEK=1 ./scripts/phase0_run.sh}"

START="$(jq -r --arg w "$WEEK" '.[$w].start' "$CONFIG")"
END="$(jq -r --arg w "$WEEK" '.[$w].end' "$CONFIG")"
SEASON="$(jq -r --arg w "$WEEK" '.[$w].season' "$CONFIG")"
WEEK_TAG="$(jq -r --arg w "$WEEK" '.[$w].week_tag' "$CONFIG")"
EXPECTED_FINALS="$(jq -r --arg w "$WEEK" '.[$w].expected_finals' "$CONFIG")"
for v in START END SEASON WEEK_TAG EXPECTED_FINALS; do
  [[ -n "${!v}" && "${!v}" != "null" ]] || { echo "[FATAL] Missing $v for week=$WEEK"; exit 1; }
done

if command -v sha256sum >/dev/null 2>&1; then SHACMD="sha256sum"; else SHACMD="shasum -a 256"; fi

OUT_DIR="$REPO_ROOT/out/$WEEK_TAG"
REPORTS_DIR="$REPO_ROOT/reports/$WEEK_TAG"
mkdir -p "$OUT_DIR" "$REPORTS_DIR"

WORKTREES_DIR="$REPO_ROOT/.phase0_worktrees"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
WT_PATH="$WORKTREES_DIR/$WEEK_TAG-$RUN_ID"
mkdir -p "$WORKTREES_DIR"

GIT_SHA="$(git rev-parse HEAD)"
git worktree add --detach "$WT_PATH" "$GIT_SHA" >/dev/null

cleanup(){ git worktree remove --force "$WT_PATH" >/dev/null 2>&1 || true; }
trap cleanup EXIT

WT_LOG_DIR="$WT_PATH/_phase0_logs"
mkdir -p "$WT_LOG_DIR"

# Carry MSF creds if present
ENV_EXPORT=()
for name in MSF_KEY MSF_PASS; do
  [[ -n "${!name-}" ]] && ENV_EXPORT+=( "$name=${!name}" )
done

run_once() {
  local pass="$1"
  local tmp="$WT_PATH/.tmp_${pass}"
  local runlog="$WT_LOG_DIR/${WEEK_TAG}_${pass}.log"
  mkdir -p "$tmp"

  pushd "$WT_PATH" >/dev/null

  # Activate repo venv if present
  if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
    # shellcheck disable=SC1091
    source "$REPO_ROOT/.venv/bin/activate"
  fi

  rm -rf out reports || true
  mkdir -p out reports out/results out/msf_details

  export START="$START" END="$END" SEASON="$SEASON" ${ENV_EXPORT[@]+"${ENV_EXPORT[@]}"}

  set -o pipefail
  {
    echo "[RUN] $(date -u +%FT%TZ) pass=$pass week=$WEEK tag=$WEEK_TAG start=$START end=$END sha=$GIT_SHA"
    echo "[ENV] Python: $(python3 -V 2>/dev/null || true)"
    echo "[ENV] MSF_KEY=${MSF_KEY:+SET}${MSF_KEY:-UNSET}"

    # Source-of-truth fetches (idempotent)
    [[ -x scripts/fetch_msf_week.py ]] && python3 scripts/fetch_msf_week.py --start "$START" --end "$END" --season "$SEASON"
    [[ -x scripts/finals_for_window.py ]] && python3 scripts/finals_for_window.py

    # Full pipeline
    make all

    # Finals QA
    FINALS="out/results/finals.csv"
    [[ -f "$FINALS" ]] || { echo "[FATAL] finals.csv not found"; exit 70; }
    rows="$(wc -l < "$FINALS" | tr -d ' ')"
    if head -1 "$FINALS" | grep -qi '^game_id'; then rows=$((rows-1)); fi
    echo "[QA] finals rows=$rows (expected $EXPECTED_FINALS)"
    [[ "$rows" -eq "$EXPECTED_FINALS" ]] || { echo "[FATAL] Finals count mismatch ($rows != $EXPECTED_FINALS)"; exit 71; }

    # Require canonical artifacts exist before checksum
    [[ -f out/model_board.csv ]] || { echo "[FATAL] Missing out/model_board.csv"; exit 72; }
    [[ -f reports/board_week.html ]] || { echo "[FATAL] Missing reports/board_week.html"; exit 73; }
    [[ -f reports/eval_ats.html  ]] || { echo "[FATAL] Missing reports/eval_ats.html";  exit 73; }

    # Stage files for digest
    mkdir -p "$tmp/out" "$tmp/reports"
    rsync -aL out/ "$tmp/out/"
    rsync -aL reports/ "$tmp/reports/"

    # Create stable file list; don't abort if empty → still produce manifest
    (
      cd "$tmp"
      # list → checksums (allow 0 files without non-zero status)
      { find out -type f -print0; find reports -type f -print0; } \
        | sort -z \
        | { xargs -0 $SHACMD || true; } > checksums.txt
      # always create a manifest digest (even if checksums.txt is empty)
      $SHACMD checksums.txt | awk '{print $1}' > manifest.sha256
      echo "[DIGEST] files=$(wc -l < checksums.txt | tr -d ' ') manifest=$(cat manifest.sha256)"
    )

    # Guard: no symlinks in staged artifacts
    if find "$tmp/out" "$tmp/reports" -type l | grep -q .; then
      echo "[FATAL] Symlinks detected in tmp artifacts"; exit 74
    fi

    echo "[RUN] ($pass) completed OK."
  } | tee "$runlog"

  popd >/dev/null

  [[ -s "$tmp/manifest.sha256" && -s "$tmp/checksums.txt" ]] \
    || { echo "[FATAL] Missing manifest/checksums for $pass. See $runlog"; exit 76; }

  echo "$tmp"
}

p1="$(run_once pass1)"
p2="$(run_once pass2)"

d1="$(cat "$p1/manifest.sha256")"
d2="$(cat "$p2/manifest.sha256")"
if [[ "$d1" != "$d2" ]]; then
  echo "[FATAL] Reproducibility FAILED: manifest digests differ"
  echo "pass1: $d1"
  echo "pass2: $d2"
  exit 75
fi
echo "[OK] Reproducibility PASSED."

# Install artifacts (from pass1) into week partitions; copy logs
rsync -aL --delete "$p1/out/" "$OUT_DIR/"
rsync -aL --delete "$p1/reports/" "$REPORTS_DIR/"
mkdir -p "$OUT_DIR/_phase0_logs"
cp -f "$WT_LOG_DIR/"* "$OUT_DIR/_phase0_logs/" 2>/dev/null || true

# Manifest for the installed week
cat > "$OUT_DIR/run_manifest.json" <<JSON
{
  "week": $WEEK,
  "week_tag": "$WEEK_TAG",
  "window": {"start":"$START","end":"$END"},
  "season": "$SEASON",
  "git_sha": "$GIT_SHA",
  "row_counts": {"finals": $(awk 'END{print NR-1}' "$OUT_DIR"/results/finals.csv)},
  "digests": {"manifest": "$d1"}
}
JSON

echo "[DONE] Week $WEEK ($WEEK_TAG) installed."
