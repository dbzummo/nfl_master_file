from __future__ import annotations
import math

def prob_from_home_line(home_line: float, a: float, b: float) -> float:
    """P(home) = sigmoid(a + b * home_line)."""
    z = a + b * float(home_line)
    return 1.0 / (1.0 + math.exp(-z))

def line_from_prob(p: float, a: float, b: float) -> float:
    """Inverse: home_line = (logit(p) - a) / b."""
    p = max(min(float(p), 1 - 1e-12), 1e-12)
    logit = math.log(p / (1.0 - p))
    return (logit - a) / b

def sanity_roundtrip(home_line: float, a: float, b: float, tol: float = 1e-9) -> None:
    """Ensure line -> prob -> line is stable to tolerance; raise if not."""
    p = prob_from_home_line(home_line, a, b)
    lin2 = line_from_prob(p, a, b)
    if not (abs(float(home_line) - lin2) <= tol + 1e-12):
        raise ValueError(f"roundtrip failed: line={home_line}, line'={lin2}, tol={tol}")
