import re, pathlib, sys

FN = "run_predictions.py"
p = pathlib.Path(FN)
src = p.read_text(encoding="utf-8")

# 1) Ensure imports
need_imports = [
    "from aliases import apply_aliases",
    "from validators import validate_odds, validate_ratings, validate_depth, validate_injuries"
]
for line in need_imports:
    if line not in src:
        # insert after existing local module imports block (after run_monte_carlo import ideally)
        anchor = "from run_monte_carlo import run_simulation"
        if anchor in src:
            src = src.replace(anchor, anchor + "\n" + line)
        else:
            # fallback: insert after 'import pandas as pd'
            anchor2 = "import pandas as pd"
            src = src.replace(anchor2, anchor2 + "\n" + line)

# 2) After odds are fetched and schema-checked, apply aliases to home/away
# Find the line 'odds_df = get_consensus_nfl_odds()' and insert after the require_columns call
pattern_odds_block = re.compile(
    r"(odds_df\s*=\s*get_consensus_nfl_odds\(\).*?require_columns\(odds_df,[^\n]+\)\n)",
    re.DOTALL
)
def add_alias_to_odds(m):
    block = m.group(1)
    inject = '    odds_df = apply_aliases(odds_df, cols=["home_team","away_team"])\n'
    if inject in src:
        return block
    return block + inject
src = pattern_odds_block.sub(add_alias_to_odds, src, count=1)

# 3) After ratings_df is created and schema-checked, alias + validate ratings, and validate odds against ratings
pattern_ratings_block = re.compile(
    r"(ratings_df\s*=\s*merge_hfa\([^\n]+\)\n\s*require_columns\(ratings_df[^\n]+\)\n)",
    re.DOTALL
)
def add_validate_ratings(m):
    block = m.group(1)
    inject = (
        '    ratings_df = apply_aliases(ratings_df, cols=["team_code"])\n'
        '    validate_ratings(ratings_df, strict=True)\n'
        '    validate_odds(odds_df, ratings_df, strict=True)\n'
    )
    if inject in src:
        return block
    return block + inject
src = pattern_ratings_block.sub(add_validate_ratings, src, count=1)

# 4) After depth_df is loaded, alias + validate depth
pattern_depth_block = re.compile(
    r"(depth_df\s*=\s*_load_depth_charts\([^\n]+\)\n)",
    re.DOTALL
)
def add_validate_depth(m):
    block = m.group(1)
    inject = (
        '    depth_df = apply_aliases(depth_df, cols=["team_code"])\n'
        '    validate_depth(depth_df, strict=True)\n'
    )
    if inject in src:
        return block
    return block + inject
src = pattern_depth_block.sub(add_validate_depth, src, count=1)

# 5) Before Monte Carlo (after injuries are finalized), alias + validate injuries (lenient)
# Insert right before 'print("\\nSTEP 6: Monte Carlo simulations'
step6_print = re.search(r'(?m)^[ \t]*print\("\\\\nSTEP 6: Monte Carlo simulations', src)
if step6_print:
    insert_at = step6_print.start()
    inj_inject = (
        '    # Normalize injuries team codes and validate leniently\n'
        '    if isinstance(injuries, pd.DataFrame) and not injuries.empty:\n'
        '        injuries = apply_aliases(injuries, cols=["team_code"])\n'
        '        validate_injuries(injuries, strict=False)\n'
    )
    if inj_inject not in src:
        src = src[:insert_at] + inj_inject + src[insert_at:]

p.write_text(src, encoding="utf-8")
print("Patched run_predictions.py: imports + aliasing + validations wired.")
