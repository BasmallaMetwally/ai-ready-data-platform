{{
  config(
    materialized='view',
    tags=['staging']
  )
}}

/*
  Staging does one job: rename, cast and lightly clean. No business logic,
  no joins. Keeping this boundary strict is what makes the marts refactorable.
*/

with source as (

    select * from {{ source('raw', 'binance_klines') }}

),

renamed as (

    select
        symbol                                  as trading_pair,
        interval                                as bar_interval,
        open_time                               as bar_opened_at,
        close_time                              as bar_closed_at,
        cast(open_time as date)                 as trade_date,

        cast(open   as numeric(38, 12))         as open_price,
        cast(high   as numeric(38, 12))         as high_price,
        cast(low    as numeric(38, 12))         as low_price,
        cast(close  as numeric(38, 12))         as close_price,

        cast(volume       as numeric(38, 12))   as base_volume,
        cast(quote_volume as numeric(38, 12))   as quote_volume_usd,
        trade_count,

        cast(taker_buy_base_volume  as numeric(38, 12)) as taker_buy_base_volume,
        cast(taker_buy_quote_volume as numeric(38, 12)) as taker_buy_quote_volume,
        loaded_at

    from source
    where open_time >= '{{ var("earliest_trade_date") }}'::timestamptz
      and close > 0

)

select * from renamed
