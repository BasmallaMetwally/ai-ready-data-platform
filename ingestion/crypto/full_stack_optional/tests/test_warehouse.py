"""Tests for the CSV serialisation used by the COPY loader."""

from __future__ import annotations

from pipeline.load.warehouse import KLINE_COLUMNS, _to_csv_buffer


def test_columns_are_emitted_in_order(valid_kline):
    line = _to_csv_buffer([valid_kline], KLINE_COLUMNS).read().strip()
    fields = line.split(",")
    assert len(fields) == len(KLINE_COLUMNS)
    assert fields[0] == "BTCUSDT"
    assert fields[1] == "1d"


def test_none_becomes_empty_string_for_copy_null(valid_kline):
    """COPY reads an empty unquoted field as NULL; 'None' would be a type error."""
    row = {**valid_kline, "taker_buy_base_volume": None}
    fields = _to_csv_buffer([row], KLINE_COLUMNS).read().strip().split(",")
    assert fields[KLINE_COLUMNS.index("taker_buy_base_volume")] == ""


def test_multiple_rows_produce_multiple_lines(valid_kline):
    buffer = _to_csv_buffer([valid_kline, valid_kline], KLINE_COLUMNS)
    assert len(buffer.read().strip().splitlines()) == 2
