#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIG="config/week_windows_2025.json"
[[ -f "$CONFIG" ]] || { echo "[FATAL] Missing $CONFIG"; exit 1; }

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "[FATAL] Git working tree must be clean."; exit 1
fi

: "${WEEK:?Usage: WEEK=1 ./scripts/phase0_run.sh}"

START="$(jq -r --arg w "$WEEK" '.[$w].start' "$CONFIG")"
END="$(jq -r --arg w "$WEEK" '.[$w].end' "$CONFIG")"
SEASON="$(jq -r --arg w "$WEEK" '.[$w].season' "$CONFIG")"
WEEK_TAG="$(jq -r --arg w "$WEEK" '.[$w].week_tag' "$CONFIG")"
EXPECTED_FINALS="$(jq -r --arg w "$WEEK" '.[$w].expected_finals' "$CONFIG")"
for v in START END SEASON WEEK_TAG EXPECTED_FINALS; do
  [[ -n "${!v}" && "${!v}" != "null" ]] || { echo "[FATAL] Missing $v in $CONFIG for week $WEEK"; exit 1; }
done

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
git worktree add --detach "$WT_PATH" "$GIT_SHA" >/dev/null
cleanup(){ git worktree remove --force "$WT_PATH" >/dev/null 2>&1 || true; }
trap cleanup EXIT

run_once() {
  local pass="$1"
  local tmp="$WT_PATH/.tmp_${pass}"
  local runlog="$ART_DIR/${WEEK_TAG}_${pass}.log"
  mkdir -p "$tmp"
  pushd "$WT_PATH" >/dev/null

  rm -rf out reports || true
  mkdir -p out results reports out/msf_details out/results

  export START="$START" END="$END" SEASON="$SEASON"

  {
    echo "[RUN] $(date -u +%FT%TZ) pass=$pass week=$WEEK tag=$WEEK_TAG start=$START end=$END sha=$GIT_SHA"
    # fetch schedule for the resolved window (single source of truth)
    if [[ -x scripts/fetch_msf_week.py ]]; then
      python3 scripts/fetch_msf_week.py --start "$START" --end "$END" --season "$SEASON"
    fi
    # ensure finals.csv is for this window
    if [[ -x scripts/finals_for_window.py ]]; then
      python3 scripts/finals_for_window.py
    fi

    make all

    FINALS="out/results/finals.csv"
    [[ -f "$FINALS" ]] || { echo "[FATAL] finals.csv not found"; exit 70; }
    rows="$(wc -l < "$FINALS" | tr -d ' ')"
    if head -1 "$FINALS" | grep -qi '^game_id'; then rows=$((rows-1)); fi
    echo "[RUN] finals rows=$rows (expected $EXPECTED_FINALS)"
    [[ "$rows" -eq "$EXPECTED_FINALS" ]] || { echo "[FATAL] Finals count mismatch ($rows != $EXPECTED_FINALS)"; exit 71; }

    mkdir -p "$tmp/out" "$tmp/reports"
    rsync -aL out/ "$tmp/out/"
    rsync -aL reports/ "$tmp/reports/"

    (
      cd "$tmp"
      # Stable file list → stable checksums
      { find out -type f -print0; find reports -type f -print0; } \
        | sort -z \
        | xargs -0 $SHACMD \
        > checksums.txt
      # Reduce to a single digest for robust comparison
      $SHACMD checksums.txt | awk '{print $1}' > manifest.sha256
    )

    # Sanity: no symlinks in installed artifacts
    if find "$tmp/out" "$tmp/reports" -type l | grep -q .; then
      echo "[FATAL] Symlinks detected in tmp artifacts"; exit 74
    fi

    echo "[RUN] ($pass) completed."
  } | tee "$runlog"

  popd >/dev/null
  echo "$tmp"
}

p1="$(run_once pass1)"
p2="$(run_once pass2)"

# Reproducibility check (single digest compare)
d1="$(cat "$p1/manifest.sha256")"
d2="$(cat "$p2/manifest.sha256")"
if [[ "$d1" != "$d2" ]]; then
  echo "[FATAL] Reproducibility FAILED: manifest digests differ"
  echo "pass1: $d1"
  echo "pass2: $d2"
  exit 75
fi
echo "[OK] Reproducibility PASSED."

# Install pass1 artifacts
rsync -aL --delete "$p1/out/" "$OUT_DIR/"
rsync -aL --delete "$p1/reports/" "$REPORTS_DIR/"
cp -f "$p1/checksums.txt" "$OUT_DIR/checksums.txt"
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
