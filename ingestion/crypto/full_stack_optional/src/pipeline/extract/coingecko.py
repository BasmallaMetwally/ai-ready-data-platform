"""CoinGecko client for asset *reference* data.

Binance gives us prices but almost no context: it does not know that BTCUSDT is
"Bitcoin", a proof-of-work chain launched in 2009. CoinGecko fills that gap and
feeds ``dim_asset``, which is modelled as a slowly changing dimension (type 2)
because attributes such as market-cap rank change over time.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import asdict, dataclass

from pipeline.config import Settings, get_settings
from pipeline.http import ResilientSession

log = logging.getLogger(__name__)

# Binance trading pair -> CoinGecko asset id.
SYMBOL_TO_COIN_ID: dict[str, str] = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin",
    "XRPUSDT": "ripple",
    "ADAUSDT": "cardano",
    "DOGEUSDT": "dogecoin",
    "AVAXUSDT": "avalanche-2",
    "DOTUSDT": "polkadot",
    "LINKUSDT": "chainlink",
}


class CoinGeckoError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AssetProfile:
    coin_id: str
    symbol: str
    trading_pair: str
    name: str
    market_cap_usd: float | None
    market_cap_rank: int | None
    circulating_supply: float | None
    max_supply: float | None
    all_time_high_usd: float | None
    all_time_high_date: dt.date | None
    extracted_at: dt.datetime

    def as_dict(self) -> dict:
        return asdict(self)


class CoinGeckoClient:
    def __init__(self, settings: Settings | None = None, session: ResilientSession | None = None):
        self.settings = settings or get_settings()
        # CoinGecko's free tier is roughly 10-30 calls/minute. Be conservative:
        # this DAG runs weekly, so throughput is irrelevant and a ban is not.
        self.session = session or ResilientSession(
            requests_per_second=0.3,
            burst=2.0,
            max_retries=self.settings.http_max_retries,
        )

    def fetch_asset_profiles(self, trading_pairs: list[str]) -> list[AssetProfile]:
        """One call for all requested assets — CoinGecko's free tier is strict."""
        pairs = [p for p in trading_pairs if p in SYMBOL_TO_COIN_ID]
        unknown = set(trading_pairs) - set(pairs)
        if unknown:
            log.warning(
                "no CoinGecko mapping for %s; they will fall back to Binance-only metadata",
                sorted(unknown),
            )
        if not pairs:
            return []

        coin_ids = [SYMBOL_TO_COIN_ID[p] for p in pairs]
        reverse = {SYMBOL_TO_COIN_ID[p]: p for p in pairs}

        response = self.session.get(
            f"{self.settings.coingecko_base_url}/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(coin_ids),
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
            },
            timeout=self.settings.http_timeout_seconds,
        )
        if not response.ok:
            raise CoinGeckoError(
                f"coins/markets failed [{response.status_code}]: {response.text[:300]}"
            )

        now = dt.datetime.now(tz=dt.UTC)
        payload = response.json()

        # Defensive: build on `reverse.get`, not `reverse[...]`. The `ids`
        # query param should make CoinGecko return exactly the coins we asked
        # for, but treating that as a hard guarantee turned a mocking quirk in
        # this module's own test suite into an unhandled KeyError — a good
        # signal that production traffic (a proxy cache, a future API version
        # that pads its response) could hit the same thing.
        unexpected = [item["id"] for item in payload if item.get("id") not in reverse]
        if unexpected:
            log.warning("CoinGecko returned unrequested coin ids, ignoring: %s", unexpected)

        profiles = [
            self._parse(item, reverse, now) for item in payload if item.get("id") in reverse
        ]
        log.info("fetched %s asset profiles from CoinGecko", len(profiles))
        return profiles

    @staticmethod
    def _parse(item: dict, reverse: dict[str, str], now: dt.datetime) -> AssetProfile:
        ath_date_raw = item.get("ath_date")
        ath_date = (
            dt.datetime.fromisoformat(ath_date_raw.replace("Z", "+00:00")).date()
            if ath_date_raw
            else None
        )
        return AssetProfile(
            coin_id=item["id"],
            symbol=item["symbol"].upper(),
            trading_pair=reverse[item["id"]],
            name=item["name"],
            market_cap_usd=item.get("market_cap"),
            market_cap_rank=item.get("market_cap_rank"),
            circulating_supply=item.get("circulating_supply"),
            max_supply=item.get("max_supply"),
            all_time_high_usd=item.get("ath"),
            all_time_high_date=ath_date,
            extracted_at=now,
        )
