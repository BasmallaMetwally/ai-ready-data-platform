{{
  config(
    materialized='ephemeral',
    tags=['intermediate']
  )
}}

/*
  Window functions that several marts need. Materialised as ephemeral so it is
  inlined as a CTE rather than persisted — it has no standalone meaning.
*/

with base as (

    select * from {{ ref('stg_ohlcv') }}

),

with_lags as (

    select
        *,
        lag(close_price) over w    as prior_close_price,
        lag(close_price, 7)  over w as close_price_7d_ago,
        lag(close_price, 30) over w as close_price_30d_ago,
        lag(base_volume) over w    as prior_base_volume

    from base
    window w as (partition by trading_pair order by trade_date)

),

with_returns as (

    select
        *,

        case
            when prior_close_price > 0
            then (close_price - prior_close_price) / prior_close_price
        end as daily_return,

        case
            when prior_close_price > 0
            then ln(cast(close_price as double precision)
                    / cast(prior_close_price as double precision))
        end as log_return,

        case
            when close_price_7d_ago > 0
            then (close_price - close_price_7d_ago) / close_price_7d_ago
        end as return_7d,

        case
            when close_price_30d_ago > 0
            then (close_price - close_price_30d_ago) / close_price_30d_ago
        end as return_30d

    from with_lags

)

select * from with_returns
