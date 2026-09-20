{{
  config(
    materialized='view',
    tags=['staging', 'reference-data']
  )
}}

with source as (

    select * from {{ source('raw', 'coingecko_assets') }}

),

renamed as (

    select
        coin_id,
        upper(symbol)                           as asset_symbol,
        upper(trading_pair)                     as trading_pair,
        name                                    as asset_name,
        cast(market_cap_usd as numeric(38, 2))  as market_cap_usd,
        market_cap_rank,
        cast(circulating_supply as numeric(38, 6)) as circulating_supply,
        cast(max_supply as numeric(38, 6))      as max_supply,
        cast(all_time_high_usd as numeric(38, 12)) as all_time_high_usd,
        all_time_high_date,
        extracted_at,

        -- Snapshot ordering drives the SCD2 validity windows downstream.
        row_number() over (
            partition by coin_id order by extracted_at desc
        ) as recency_rank

    from source

)

select * from renamed
