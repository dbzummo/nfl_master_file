from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class BoardRow:
    # Minimal schema expected downstream; keep names stable.
    date: str
    week: int
    home_team: str
    away_team: str
    p_home_model: float
    vegas_line_home: float

    # Optional fields we often carry through the board (not strictly required)
    msf_game_id: Optional[str] = None
    elo_exp_home: Optional[float] = None
    elo_diff_pre: Optional[float] = None
    elo_home_pre: Optional[float] = None
    elo_away_pre: Optional[float] = None

    # Post-blend optional fields (Bayes step)
    p_home_model_adj: Optional[float] = None
    model_line_home_adj: Optional[float] = None
    edge_adj: Optional[float] = None
    confidence_adj: Optional[float] = None
    w2025_used: Optional[float] = None

    @staticmethod
    def coerce(row: Dict[str, Any]) -> "BoardRow":
        """
        Lossless, fail-closed coercion from a CSV dict row.
        Raises ValueError if required fields are missing or non-numeric where required.
        """
        def need(k):
            if k not in row or row[k] in (None, ""):
                raise ValueError(f"missing required field '{k}'")
            return row[k]

        def fnum(k, default=None):
            v = row.get(k, default)
            if v in (None, ""):
                if default is None:
                    raise ValueError(f"missing numeric field '{k}'")
                return float(default)
            return float(v)

        def inum(k, default=None):
            v = row.get(k, default)
            if v in (None, ""):
                if default is None:
                    raise ValueError(f"missing integer field '{k}'")
                return int(default)
            return int(float(v))

        return BoardRow(
            date=str(need("date")),
            week=inum("week"),
            home_team=str(need("home_team")).strip().upper(),
            away_team=str(need("away_team")).strip().upper(),
            p_home_model=fnum("p_home_model"),
            vegas_line_home=fnum("vegas_line_home", 0.0),

            msf_game_id=row.get("msf_game_id"),
            elo_exp_home=float(row["elo_exp_home"]) if row.get("elo_exp_home") not in (None, "") else None,
            elo_diff_pre=float(row["elo_diff_pre"]) if row.get("elo_diff_pre") not in (None, "") else None,
            elo_home_pre=float(row["elo_home_pre"]) if row.get("elo_home_pre") not in (None, "") else None,
            elo_away_pre=float(row["elo_away_pre"]) if row.get("elo_away_pre") not in (None, "") else None,

            p_home_model_adj=float(row["p_home_model_adj"]) if row.get("p_home_model_adj") not in (None, "") else None,
            model_line_home_adj=float(row["model_line_home_adj"]) if row.get("model_line_home_adj") not in (None, "") else None,
            edge_adj=float(row["edge_adj"]) if row.get("edge_adj") not in (None, "") else None,
            confidence_adj=float(row["confidence_adj"]) if row.get("confidence_adj") not in (None, "") else None,
            w2025_used=float(row["w2025_used"]) if row.get("w2025_used") not in (None, "") else None,
        )
