import os, sys, json, time
from urllib.parse import urlparse
import requests

LOG_PATH = os.environ.get("HTTP_TRACE_LOG", "out/_audit/http_trace.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

_orig_request = requests.sessions.Session.request

def _trace_request(self, method, url, *args, **kwargs):
    t0 = time.time()
    try:
        resp = _orig_request(self, method, url, *args, **kwargs)
        ok = True
        return resp
    except Exception as e:
        resp = None
        ok = False
        raise
    finally:
        host = urlparse(url).netloc.lower()
        if "mysportsfeeds.com" in host:
            rec = {
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "method": method,
                "url": url,
                "status": getattr(resp, "status_code", None),
                "elapsed_ms": int((time.time() - t0)*1000),
            }
            # Keep log terse; don’t dump creds/headers
            with open(LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
            print(f"[HTTPTRACE] {rec['method']} {rec['url']} -> {rec['status']} ({rec['elapsed_ms']} ms)", file=sys.stderr)

requests.sessions.Session.request = _trace_request
