"""
test_dq_bridge.py
------------------
Proves ingestion/crypto/dq_bridge.py actually works, using synthetic OHLCV
rows shaped exactly like pipeline.extract.binance.Kline output (since this
sandbox cannot reach the real Binance API — see dq_bridge.py's docstring).
"""

import datetime as dt
import unittest

# Imported the same way every other test in this suite imports pipeline
# code (from pipeline.X.Y import ...) — an earlier version instead did
# sys.path.insert(quality_dir) + `from dq_bridge import ...`, which loads
# the file as a *different* module object than pipeline.quality.dq_bridge.
# That's not just inconsistent: it made pytest-cov's `--cov=pipeline`
# attribute zero coverage to this file even though these tests exercise
# it, which was enough to drop total coverage below the CI's
# --cov-fail-under=85 gate. Reproduced that failure, root-caused it to
# this import mismatch, and fixed it here rather than papering over the
# coverage number.
from pipeline.quality.dq_bridge import _DQ_AVAILABLE, klines_to_dataframe, run_ohlcv_quality_gate

# When this crypto project is checked out on its own (its own repo, its own
# CI — see .github/workflows/ci.yml and README's "keep this repo separable"
# note), the sibling dq/ package doesn't exist and run_ohlcv_quality_gate()
# returns None by design (see dq_bridge.py). Skip the scoring assertions in
# that case rather than fail on an expected None; klines_to_dataframe has no
# such dependency and always runs.
_skip_reason = (
    "dq/ package not available next to this checkout (expected for a standalone crypto repo)"
)


def _fake_kline(open_=100.0, high=105.0, low=99.0, close=102.0, volume=1000.0):
    return {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "open_time": dt.datetime(2026, 1, 1),
        "close_time": dt.datetime(2026, 1, 2),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "quote_volume": volume * close,
        "trade_count": 500,
        "taker_buy_base_volume": volume / 2,
    }


class TestDQBridge(unittest.TestCase):
    @unittest.skipUnless(_DQ_AVAILABLE, _skip_reason)
    def test_clean_ohlcv_batch_scores_well(self):
        klines = [
            _fake_kline(open_=100 + i, high=105 + i, low=99 + i, close=102 + i) for i in range(30)
        ]
        result = run_ohlcv_quality_gate(klines, symbol="BTCUSDT", threshold=80.0)
        self.assertTrue(result.passed)
        self.assertGreaterEqual(result.overall_score, 80.0)
        self.assertEqual(result.dataset_name, "crypto.BTCUSDT")

    @unittest.skipUnless(_DQ_AVAILABLE, _skip_reason)
    def test_dirty_ohlcv_batch_scores_low(self):
        # negative prices are impossible for a real candle -> schema violations
        klines = [_fake_kline(open_=-1, high=-1, low=-1, close=-1, volume=-1) for _ in range(30)]
        result = run_ohlcv_quality_gate(klines, symbol="BTCUSDT", threshold=80.0)
        self.assertFalse(result.passed)
        self.assertLess(result.overall_score, 80.0)

    def test_missing_dq_package_returns_none_not_a_crash(self):
        # Always runs, regardless of whether dq/ is present, since it only
        # asserts the *contract*: either a real result, or a clean None.
        klines = [_fake_kline()]
        result = run_ohlcv_quality_gate(klines, symbol="BTCUSDT", threshold=80.0)
        if _DQ_AVAILABLE:
            self.assertIsNotNone(result)
        else:
            self.assertIsNone(result)

    def test_klines_to_dataframe_shape(self):
        klines = [_fake_kline() for _ in range(5)]
        df = klines_to_dataframe(klines)
        self.assertEqual(len(df), 5)
        self.assertIn("close", df.columns)


if __name__ == "__main__":
    unittest.main()
