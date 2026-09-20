{% snapshot snap_asset_profiles %}

{{
  config(
    target_schema='snapshots',
    unique_key='coin_id',
    strategy='check',
    check_cols=['market_cap_rank', 'circulating_supply', 'max_supply', 'asset_name'],
    invalidate_hard_deletes=True
  )
}}

/*
  dim_asset tracks history for the one attribute we model deliberately
  (market-cap rank). This snapshot tracks history for *everything else*, as an
  audit record.

  The distinction matters: a dimension is a modelling decision you make for
  analysts, a snapshot is an insurance policy you take out for yourself. When
  someone asks "when did the circulating supply change, and did we notice?",
  the snapshot answers it without having to widen the dimension and rebuild
  every downstream fact.

  Strategy is `check` rather than `timestamp` because CoinGecko's payload has no
  reliable last-modified field — extracted_at is when *we* looked, not when the
  value changed.
*/

select
    coin_id,
    asset_symbol,
    trading_pair,
    asset_name,
    market_cap_usd,
    market_cap_rank,
    circulating_supply,
    max_supply,
    all_time_high_usd,
    all_time_high_date
from {{ ref('stg_assets') }}
where recency_rank = 1

{% endsnapshot %}
