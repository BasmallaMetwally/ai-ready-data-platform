# Data dictionary

Grain, keys and column meanings for every published table. Generated
descriptions also live in dbt — run `make dbt-docs` for the interactive version
with full lineage graphs.

## `analytics.dim_date`

Calendar dimension. **Grain:** one row per calendar day.

| Column | Type | Description |
|---|---|---|
| `date_key` | `date` | Primary key. The calendar date. |
| `calendar_year` | `int` | Four-digit year. |
| `calendar_quarter` | `int` | 1–4. |
| `year_month` | `text` | `YYYY-MM`, convenient for grouping. |
| `is_weekend` | `bool` | Present for convention; crypto trades every day. |
| `is_past` | `bool` | The spine extends a year forward for forecasting joins. |

## `analytics.dim_asset`

Asset reference data. **Grain:** one row per asset per version (SCD type 2).

| Column | Type | Description |
|---|---|---|
| `asset_key` | `text` | Surrogate key over `(coin_id, extracted_at)`. |
| `coin_id` | `text` | CoinGecko identifier, e.g. `bitcoin`. |
| `trading_pair` | `text` | Binance symbol, e.g. `BTCUSDT`. The join key to facts. |
| `market_cap_rank` | `int` | Rank at `valid_from`. The attribute that drives versioning. |
| `supply_issued_pct` | `numeric` | `circulating / max_supply`. Null for uncapped assets. |
| `valid_from` / `valid_to` | `timestamptz` | Validity window. `valid_to` is `9999-12-31` for the open version. |
| `is_current` | `bool` | Exactly one `true` per `coin_id` — enforced by a singular test. |

## `analytics.fct_ohlcv_daily`

Core price fact. **Grain:** one row per asset per trading day.

| Column | Type | Description |
|---|---|---|
| `date_key` | `date` | FK to `dim_date`. |
| `asset_key` | `text` | FK to the `dim_asset` version valid on that date. |
| `open_price` … `close_price` | `numeric(38,12)` | OHLC. Decimal, never float. |
| `base_volume` | `numeric` | Volume in the base asset (e.g. BTC). |
| `quote_volume_usd` | `numeric` | Volume in the quote asset (USDT ≈ USD). |
| `daily_return` | `numeric` | Simple close-to-close return, as a decimal fraction. |
| `log_return` | `numeric` | Natural-log return. Use this one for volatility — log returns are additive over time. |
| `taker_buy_ratio` | `numeric` | Share of volume from aggressive buyers. A crude sentiment proxy. |
| `is_up_day` | `bool` | `close >= open`. |

## `analytics.fct_asset_metrics_daily`

Derived risk and momentum. **Grain:** one row per asset per trading day.

| Column | Type | Description |
|---|---|---|
| `sma_7d` / `sma_30d` / `sma_200d` | `numeric` | Simple moving averages of close. |
| `annualised_volatility_30d` | `numeric` | 30-day stddev of log returns × √365. |
| `drawdown_from_peak` | `numeric` | `(close − running_peak) / running_peak`. Always ≤ 0. |
| `trend_regime` | `text` | `bullish` when SMA30 > SMA200, else `bearish`, or `insufficient_history`. |
| `observation_number` | `int` | Row index within the asset. Use it to filter out warm-up periods. |

## `analytics.mart_market_overview`

Reporting mart the dashboard reads. **Grain:** one row per asset, latest state
only. Joins the two facts to the current dimension version and adds
whole-history aggregates (`days_of_history`, `up_day_ratio`,
`cumulative_volume_usd`).

## Known caveats

- **USDT is not USD.** All quote volumes assume a 1:1 peg. It has historically deviated by a few percent under stress.
- **Warm-up periods.** `sma_200d` is null for an asset's first 199 days. Filter on `observation_number >= 200` before comparing trend regimes across assets.
- **Binance-only coverage.** Prices reflect one exchange. Thin pairs can diverge from the global composite price.
