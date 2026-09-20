{{
  config(
    materialized='table',
    tags=['mart', 'reporting']
  )
}}

/*
  The table the dashboard actually queries: one row per asset, latest state.
  Pre-aggregating here keeps the BI layer to a simple SELECT *.
*/

with latest_date as (

    select max(date_key) as as_of_date from {{ ref('fct_ohlcv_daily') }}

),

latest_facts as (

    select f.*
    from {{ ref('fct_ohlcv_daily') }} f
    cross join latest_date d
    where f.date_key = d.as_of_date

),

latest_metrics as (

    select m.*
    from {{ ref('fct_asset_metrics_daily') }} m
    cross join latest_date d
    where m.date_key = d.as_of_date

),

asset_details as (

    select * from {{ ref('dim_asset') }} where is_current

),

history as (

    select
        trading_pair,
        min(date_key)                                as first_trade_date,
        count(*)                                     as days_of_history,
        sum(case when is_up_day then 1 else 0 end)::numeric / nullif(count(*), 0)
                                                     as up_day_ratio,
        max(high_price)                              as all_time_high_in_data,
        sum(quote_volume_usd)                        as cumulative_volume_usd
    from {{ ref('fct_ohlcv_daily') }}
    group by 1

)

select
    latest_facts.date_key                            as as_of_date,
    latest_facts.trading_pair,
    asset_details.asset_name,
    asset_details.asset_symbol,
    asset_details.market_cap_usd,
    asset_details.market_cap_rank,

    latest_facts.close_price,
    latest_facts.daily_return,
    latest_facts.return_7d,
    latest_facts.return_30d,
    latest_facts.quote_volume_usd                    as volume_usd_24h,
    latest_facts.taker_buy_ratio,

    latest_metrics.sma_30d,
    latest_metrics.sma_200d,
    latest_metrics.annualised_volatility_30d,
    latest_metrics.drawdown_from_peak,
    latest_metrics.trend_regime,
    latest_metrics.is_above_200d_average,

    history.first_trade_date,
    history.days_of_history,
    history.up_day_ratio,
    history.all_time_high_in_data,
    history.cumulative_volume_usd,

    current_timestamp                                as refreshed_at

from latest_facts
left join latest_metrics using (trading_pair)
left join asset_details  using (trading_pair)
left join history        using (trading_pair)
