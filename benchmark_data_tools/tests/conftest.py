import os
import signal
import sys
import contextlib
import pytest
import importlib


DEFAULT_TIMEOUT_SECS = int(os.environ.get("PYTEST_DEFAULT_TIMEOUT", 10))


def _install_alarm(timeout: int):
    def _handler(signum, frame):
        raise TimeoutError(f"Test timed out after {timeout}s")

    prev_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handler)
    # setitimer allows fractional seconds; here integer seconds are fine
    signal.setitimer(signal.ITIMER_REAL, timeout)
    return prev_handler


@pytest.fixture(autouse=True)
def per_test_timeout():
    # If pytest-timeout plugin is present, rely on it (pytest.ini sets --timeout)
    if any("pytest_timeout" in str(m) for m in sys.modules.keys()):
        yield
        return
    # Fallback: POSIX-only SIGALRM based timeout
    if os.name != "posix" or not hasattr(signal, "SIGALRM"):
        yield
        return
    prev = _install_alarm(DEFAULT_TIMEOUT_SECS)
    try:
        yield
    finally:
        # cancel alarm and restore previous handler
        with contextlib.suppress(Exception):
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, prev)


@pytest.fixture(autouse=True)
def clean_duckdb_catalog():
    """Ensure DuckDB starts each test with an empty catalog.

    Tests that import duckdb will share a process-global connection state.
    Drop any existing tables between tests to avoid cross-test contamination.
    """
    yield
    try:
        duckdb = importlib.import_module("duckdb")
    except Exception:
        return
    try:
        tables = duckdb.sql("SHOW TABLES").fetchall()
    except Exception:
        return
    for (tbl,) in tables:
        with contextlib.suppress(Exception):
            duckdb.sql(f"DROP TABLE IF EXISTS {tbl}")


