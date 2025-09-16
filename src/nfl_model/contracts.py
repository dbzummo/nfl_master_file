from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

@dataclass
class BoardRow:
    # Required by validate_and_manifest.py
    game_id: str
    vegas_line_home: float
    model_line_home: float
    p_home_market: Optional[float] = None
    p_home_model: float = 0.5
    inj_home_pts: float = 0.0
    inj_away_pts: float = 0.0
    confidence: float = 0.0
