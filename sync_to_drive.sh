#!/usr/bin/env bash
set -euo pipefail

# ===== CONFIG: set this to your actual Drive folder =====
DRIVE_PATH="$HOME/Library/CloudStorage/GoogleDrive-<your_account>/My Drive/NFL_SHARE"

usage() {
  cat >&2 <<USAGE
Usage: $0 [--logs | --week N | --all | <file_or_dir> ...]
  --logs         Sync the local logs/ folder
  --week N       Sync out/weekNN/ for week N (zero-padded)
  --all          Sync canonical bundle (repo_tree + scripts + src + config + manifest.json + out + reports)
  no args        Sync default subset (repo_tree + scripts + config + manifest.json + out + reports)
  files...       Sync specific targets you list (and also repo_tree)
USAGE
  exit 1
}

[ -d "$HOME/Library/CloudStorage" ] || { echo "[ERR] CloudStorage base missing"; exit 1; }
mkdir -p "$DRIVE_PATH"

echo "[1/3] Generating repo_tree.txt ..."
LC_ALL=C find . \
  -not -path "./.git/*" \
  -not -path "./.venv/*" \
  -not -path "./__pycache__/*" \
  -not -path "./.ipynb_checkpoints/*" \
  -not -path "./_archive/*" \
  -not -path "./_secrets/*" \
  -not -path "./saves/*" \
  -not -path "./_snapshots/*" \
  -print | sort > repo_tree.txt
echo "      repo_tree.txt lines: $(wc -l < repo_tree.txt)"

case "${1:-}" in
  --logs)
    shift
    set -- logs/
    ;;
  --week)
    shift
    [ $# -ge 1 ] || usage
    wk="$1"; shift
    printf -v wk2 "%02d" "$wk"
    set -- "out/week${wk2}/" "$@"
    ;;
  --all)
    shift
    set -- repo_tree.txt scripts/ src/ config/ manifest.json out/ reports/ "$@"
    ;;
  "")
    set -- repo_tree.txt scripts/ config/ manifest.json out/ reports/
    ;;
esac

echo "[2/3] Syncing targets to Drive mirror ..."
for target in "$@"; do
  if [ -e "$target" ]; then
    echo "[SYNC] $target"
    mkdir -p "$DRIVE_PATH/$(dirname "$target")"
    rsync -av \
      --exclude ".git/" \
      --exclude ".venv/" \
      --exclude "__pycache__/" \
      --exclude ".ipynb_checkpoints/" \
      --exclude "_secrets/" \
      --exclude "_archive/" \
      --exclude "saves/" \
      --exclude "_snapshots/" \
      "$target" "$DRIVE_PATH/$target"
  else
    echo "[WARN] Skipping missing $target"
  fi
done

echo "[3/3] Ensuring latest repo_tree.txt is in Drive ..."
rsync -av repo_tree.txt "$DRIVE_PATH/repo_tree.txt"
echo "✅ Sync complete."
