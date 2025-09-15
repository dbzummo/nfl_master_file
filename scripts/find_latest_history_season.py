#!/usr/bin/env python3
import glob, os, re, sys

def main():
    years = []
    for p in glob.glob("history/season_*_from_site.csv"):
        m = re.search(r"season_(\d{4})_from_site\.csv$", os.path.basename(p))
        if m:
            years.append(int(m.group(1)))
    if not years:
        print("", end="")
        sys.exit(1)
    print(max(years))
if __name__ == "__main__":
    main()
