"""Transient-error retry: a Ledger API blip must not kill the process.

Unit tests over _request with httpx.request and time.sleep faked, so they are deterministic and
instant. Covers: retry on connection errors, retry on 5xx, fail-fast on 4xx, and give-up after the
configured number of retries.
"""
import pathlib
import sys
from unittest import mock

import httpx

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import trunk_canton.__main__ as tc


def _resp(status, data=None):
    return httpx.Response(status, json=(data or {}), request=httpx.Request("GET", "http://x/y"))


def _seq(responses):
    """Return a fake httpx.request that yields each response/exception in order (last one repeats)."""
    calls = {"n": 0}

    def fake(method, url, headers=None, **kw):
        i = calls["n"]
        calls["n"] += 1
        item = responses[min(i, len(responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    return fake, calls


def _drive(responses, max_retries=3):
    tc.HTTP_MAX_RETRIES = max_retries
    tc.HTTP_BACKOFF_BASE = 0.0
    fake, calls = _seq(responses)
    sleeps = []
    with mock.patch("httpx.request", fake), mock.patch.object(tc.time, "sleep", lambda d: sleeps.append(d)):
        try:
            result = tc._request("GET", "http://x/y")
            error = None
        except Exception as e:
            result, error = None, e
    return result, error, calls["n"], sleeps


def test_retries_connection_error_then_succeeds():
    r, err, attempts, sleeps = _drive([httpx.ConnectError("down"), httpx.ConnectError("down"), _resp(200, {"ok": 1})])
    assert err is None
    assert r.status_code == 200
    assert attempts == 3        # 2 failures + 1 success
    assert len(sleeps) == 2     # backed off between each retry


def test_retries_transient_5xx_then_succeeds():
    r, err, attempts, sleeps = _drive([_resp(503), _resp(502), _resp(200, {"ok": 1})])
    assert err is None and r.status_code == 200 and attempts == 3


def test_permanent_4xx_fails_fast_without_retry():
    r, err, attempts, sleeps = _drive([_resp(404)])
    assert isinstance(err, httpx.HTTPStatusError)
    assert attempts == 1        # no retry on a permanent error
    assert sleeps == []


def test_gives_up_after_max_retries():
    r, err, attempts, sleeps = _drive([_resp(503)], max_retries=2)
    assert isinstance(err, httpx.HTTPStatusError)
    assert attempts == 3        # 1 initial + 2 retries
    assert len(sleeps) == 2


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            import traceback; print(f"FAIL {t.__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
