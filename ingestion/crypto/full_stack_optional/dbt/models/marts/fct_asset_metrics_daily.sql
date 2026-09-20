{{
  config(
    materialized='table',
    tags=['mart', 'fact', 'analytics'],
    indexes=[{'columns': ['trading_pair', 'date_key'], 'unique': true}]
  )
}}

/*
  Derived risk and momentum measures. Built as a full table rather than
  incrementally: every window here looks back up to 200 days, so an incremental
  build would produce wrong values at the boundary of each new slice — a
  mistake that is very easy to make and very hard to notice.
*/

with facts as (

    select * from {{ ref('fct_ohlcv_daily') }}

),

windowed as (

    select
        date_key,
        asset_key,
        trading_pair,
        close_price,
        base_volume,
        quote_volume_usd,
        daily_return,
        log_return,

        avg(close_price) over (w rows between 6   preceding and current row) as sma_7d,
        avg(close_price) over (w rows between 29  preceding and current row) as sma_30d,
        avg(close_price) over (w rows between 199 preceding and current row) as sma_200d,

        avg(quote_volume_usd) over (w rows between 29 preceding and current row)
                                                                             as avg_volume_30d,

        stddev_samp(log_return) over (w rows between 29 preceding and current row)
                                                                             as volatility_30d,
        stddev_samp(log_return) over (w rows between 89 preceding and current row)
                                                                             as volatility_90d,

        max(close_price) over (w rows between unbounded preceding and current row)
                                                                             as running_peak_price,

        count(*) over (w rows between unbounded preceding and current row)    as observation_number

    from facts
    window w as (partition by trading_pair order by date_key)

),

final as (

    select
        date_key,
        asset_key,
        trading_pair,
        close_price,
        sma_7d,
        sma_30d,
        sma_200d,
        avg_volume_30d,

        -- Annualised with 365 days: crypto has no market close.
        volatility_30d * sqrt({{ var('annualisation_days') }}) as annualised_volatility_30d,
        volatility_90d * sqrt({{ var('annualisation_days') }}) as annualised_volatility_90d,

        running_peak_price,
        case
            when running_peak_price > 0
            then (close_price - running_peak_price) / running_peak_price
        end                                                    as drawdown_from_peak,

        -- Golden/death cross, the textbook trend signal.
        case
            when sma_30d is null or sma_200d is null then 'insufficient_history'
            when sma_30d > sma_200d then 'bullish'
            else 'bearish'
        end                                                    as trend_regime,

        case
            when close_price > sma_200d then true
            when sma_200d is null then null
            else false
        end                                                    as is_above_200d_average,

        observation_number

    from windowed

)

select * from final
