#!/usr/bin/env python3
# Usage: python3 scripts/run_with_trace.py scripts/fetch_week_msf.py --season 2025-regular --start 20250911 --end 20250915
import os, sys, runpy
os.environ.setdefault("HTTP_TRACE_LOG", "out/_audit/http_trace.log")
# inject tracer before target runs
import importlib.util, importlib.machinery
spec = importlib.util.spec_from_file_location("http_trace", "scripts/http_trace.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
# run the target script as __main__
target = sys.argv[1]
sys.argv = sys.argv[1:]
runpy.run_path(target, run_name="__main__")
