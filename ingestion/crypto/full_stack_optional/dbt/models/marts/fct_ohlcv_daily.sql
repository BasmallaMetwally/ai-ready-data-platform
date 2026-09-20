{{
  config(
    materialized='incremental',
    unique_key=['trading_pair', 'date_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    tags=['mart', 'fact'],
    contract={'enforced': true},
    indexes=[
      {'columns': ['date_key']},
      {'columns': ['asset_key']},
      {'columns': ['trading_pair', 'date_key'], 'unique': true}
    ]
  )
}}

/*
  The grain: one row per asset per trading day.

  delete+insert rather than merge because the reload window is contiguous by
  date — deleting a date range and reinserting it is both cheaper on Postgres
  and trivially idempotent under Airflow retries.

  The unique_key is (trading_pair, date_key), NOT (asset_key, date_key).
  asset_key is a type-2 surrogate that changes whenever the dimension versions,
  so keying the incremental load on it would leave the pre-version row orphaned
  in the table on the next run, silently duplicating that day.
*/

with ohlcv as (

    select * from {{ ref('int_ohlcv_with_returns') }}

    {% if is_incremental() %}
    -- Reprocess a rolling window so late exchange corrections are picked up.
    where trade_date >= (
        select coalesce(max(date_key), date '1900-01-01') - interval '{{ var("reload_window_days") }} day'
        from {{ this }}
    )
    {% endif %}

),

assets as (

    select * from {{ ref('dim_asset') }} where not is_unknown_member

),

joined as (

    select
        ohlcv.trade_date                                as date_key,

        -- Fall back to the unknown member rather than fabricating a key that
        -- has no matching dimension row. This keeps the relationships test
        -- meaningful instead of quietly unenforceable.
        coalesce(assets.asset_key, '-1')                as asset_key,
        ohlcv.trading_pair,

        ohlcv.open_price,
        ohlcv.high_price,
        ohlcv.low_price,
        ohlcv.close_price,
        ohlcv.prior_close_price,

        ohlcv.base_volume,
        ohlcv.quote_volume_usd,
        ohlcv.trade_count,

        ohlcv.daily_return,
        ohlcv.log_return,
        ohlcv.return_7d,
        ohlcv.return_30d,

        ohlcv.high_price - ohlcv.low_price              as price_range,
        case
            when ohlcv.low_price > 0
            then (ohlcv.high_price - ohlcv.low_price) / ohlcv.low_price
        end                                             as range_pct,

        case
            when ohlcv.base_volume > 0
            then ohlcv.quote_volume_usd / ohlcv.base_volume
        end                                             as vwap,

        case
            when ohlcv.quote_volume_usd > 0
            then ohlcv.taker_buy_quote_volume / ohlcv.quote_volume_usd
        end                                             as taker_buy_ratio,

        ohlcv.close_price >= ohlcv.open_price           as is_up_day,
        assets.asset_key is null                        as has_unknown_asset,
        ohlcv.loaded_at,
        current_timestamp                                as dbt_updated_at

    from ohlcv
    left join assets
        on  ohlcv.trading_pair = assets.trading_pair
        and ohlcv.bar_opened_at >= assets.valid_from
        and ohlcv.bar_opened_at <  assets.valid_to

)

select * from joined
