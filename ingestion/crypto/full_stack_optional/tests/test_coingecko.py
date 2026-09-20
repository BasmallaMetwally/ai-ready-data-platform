"""Unit tests for the CoinGecko extractor, fully mocked."""

from __future__ import annotations

import datetime as dt

import pytest
import responses

from pipeline.extract.coingecko import SYMBOL_TO_COIN_ID, CoinGeckoClient, CoinGeckoError


@pytest.fixture
def coingecko_payload() -> list[dict]:
    """Shape of a real /coins/markets response, trimmed to the fields we read."""
    return [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "market_cap": 1_300_000_000_000.0,
            "market_cap_rank": 1,
            "circulating_supply": 19_800_000.0,
            "max_supply": 21_000_000.0,
            "ath": 108000.0,
            "ath_date": "2025-01-20T12:00:00.000Z",
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "market_cap": 400_000_000_000.0,
            "market_cap_rank": 2,
            "circulating_supply": 120_000_000.0,
            "max_supply": None,  # ETH has no hard cap
            "ath": 4900.0,
            "ath_date": "2021-11-10T14:24:19.604Z",
        },
    ]


@responses.activate
def test_fetch_asset_profiles_parses_payload(settings, coingecko_payload):
    responses.add(
        responses.GET,
        "https://api.coingecko.com/api/v3/coins/markets",
        json=coingecko_payload,
        status=200,
    )
    profiles = CoinGeckoClient(settings).fetch_asset_profiles(["BTCUSDT", "ETHUSDT"])

    assert len(profiles) == 2
    btc = next(p for p in profiles if p.trading_pair == "BTCUSDT")
    assert btc.coin_id == "bitcoin"
    assert btc.symbol == "BTC"
    assert btc.name == "Bitcoin"
    assert btc.market_cap_rank == 1
    assert btc.all_time_high_usd == pytest.approx(108000.0)
    assert btc.all_time_high_date == dt.date(2025, 1, 20)
    assert btc.extracted_at.tzinfo is dt.UTC


@responses.activate
def test_null_max_supply_is_preserved(settings, coingecko_payload):
    """ETH has no max supply; the field must stay None, not become 0."""
    # The real API only returns coins matching the `ids` param; mock the same.
    responses.add(
        responses.GET,
        "https://api.coingecko.com/api/v3/coins/markets",
        json=[coingecko_payload[1]],  # ethereum only
        status=200,
    )
    profiles = CoinGeckoClient(settings).fetch_asset_profiles(["ETHUSDT"])
    eth = profiles[0]
    assert eth.max_supply is None
    assert eth.circulating_supply == pytest.approx(120_000_000.0)


@responses.activate
def test_unmapped_symbols_are_skipped_not_raised(settings, coingecko_payload):
    """A trading pair with no CoinGecko mapping should degrade, not blow up."""
    responses.add(
        responses.GET,
        "https://api.coingecko.com/api/v3/coins/markets",
        json=[coingecko_payload[0]],  # only bitcoin is a known pair here
        status=200,
    )
    profiles = CoinGeckoClient(settings).fetch_asset_profiles(["BTCUSDT", "NOTAREALPAIR"])
    assert len(profiles) == 1
    assert profiles[0].trading_pair == "BTCUSDT"


@responses.activate
def test_response_item_outside_the_requested_ids_is_ignored(settings, coingecko_payload):
    """If an upstream ever pads its response, we drop the extra rather than crash.

    This is the scenario that originally surfaced the bug: requesting one coin
    but receiving the full unfiltered payload back.
    """
    responses.add(
        responses.GET,
        "https://api.coingecko.com/api/v3/coins/markets",
        json=coingecko_payload,  # both bitcoin and ethereum, though only BTC was asked for
        status=200,
    )
    profiles = CoinGeckoClient(settings).fetch_asset_profiles(["BTCUSDT"])
    assert [p.trading_pair for p in profiles] == ["BTCUSDT"]


def test_no_known_pairs_short_circuits_without_a_call(settings):
    """If nothing maps, fetch_asset_profiles must not even hit the network."""
    client = CoinGeckoClient(settings)
    assert client.fetch_asset_profiles(["NOTAREALPAIR"]) == []


@responses.activate
def test_server_error_raises_coingecko_error(settings):
    responses.add(
        responses.GET,
        "https://api.coingecko.com/api/v3/coins/markets",
        json={"error": "internal"},
        status=500,
    )
    settings.http_max_retries = 0
    with pytest.raises(CoinGeckoError, match="coins/markets failed"):
        CoinGeckoClient(settings).fetch_asset_profiles(["BTCUSDT"])


@responses.activate
def test_query_requests_only_the_mapped_coin_ids(settings, coingecko_payload):
    """Regression guard: we must not ask CoinGecko for ids we cannot map back."""
    responses.add(
        responses.GET,
        "https://api.coingecko.com/api/v3/coins/markets",
        json=coingecko_payload,
        status=200,
    )
    CoinGeckoClient(settings).fetch_asset_profiles(["BTCUSDT", "ETHUSDT", "UNMAPPEDUSDT"])

    sent_ids = responses.calls[0].request.params["ids"].split(",")
    assert set(sent_ids) == {"bitcoin", "ethereum"}


def test_symbol_map_is_uppercase_binance_pairs():
    """Every key must be a real Binance-style pair, since that's what callers pass."""
    assert all(pair.endswith("USDT") for pair in SYMBOL_TO_COIN_ID)
    assert all(pair == pair.upper() for pair in SYMBOL_TO_COIN_ID)
