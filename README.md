## Repo Hygiene & Sharing
- Secrets live in `_secrets/.env` (never committed). Public sample: `.env.example`.
- Archives and heavy files live under `_archive/` or `saves/` (ignored).
- Drive mirror excludes `_secrets`, `_archive`, `.venv`, `__pycache__`, and large binaries.
- To sync the Drive mirror:
  ./sync_to_drive.sh

## Quickstart
- Run weekly pipeline:
  ```bash
  python3 scripts/update_injuries_week.py \
  && python3 scripts/compute_injury_adjustments.py \
  && python3 scripts/make_model_lines_and_board.py \
  && open reports/board_week.html
## CI
![calibration-check](https://github.com/dbzummo/nfl_master_file/actions/workflows/calibration-check.yml/badge.svg)
