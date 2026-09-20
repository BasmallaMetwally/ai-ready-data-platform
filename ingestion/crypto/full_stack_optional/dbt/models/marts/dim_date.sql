{{
  config(
    materialized='table',
    tags=['mart', 'dimension'],
    indexes=[{'columns': ['date_key'], 'unique': true}]
  )
}}

/*
  A conventional calendar dimension. Crypto trades every day, so there is no
  is_trading_day flag — but the quarter/month grain still saves every analyst
  from writing date_trunc by hand.
*/

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('earliest_trade_date') ~ "' as date)",
        end_date="cast(current_date + interval '365 day' as date)"
    ) }}

),

enriched as (

    select
        cast(date_day as date)                              as date_key,
        extract(year    from date_day)::int                 as calendar_year,
        extract(quarter from date_day)::int                 as calendar_quarter,
        extract(month   from date_day)::int                 as calendar_month,
        extract(week    from date_day)::int                 as iso_week,
        extract(day     from date_day)::int                 as day_of_month,
        extract(isodow  from date_day)::int                 as day_of_week,
        to_char(date_day, 'Day')                            as day_name,
        to_char(date_day, 'Mon')                            as month_name,
        to_char(date_day, 'YYYY-MM')                        as year_month,
        date_trunc('month',   date_day)::date               as first_day_of_month,
        (date_trunc('month', date_day) + interval '1 month - 1 day')::date
                                                            as last_day_of_month,
        extract(isodow from date_day) in (6, 7)             as is_weekend,
        date_day <= current_date                            as is_past

    from spine

)

select * from enriched
