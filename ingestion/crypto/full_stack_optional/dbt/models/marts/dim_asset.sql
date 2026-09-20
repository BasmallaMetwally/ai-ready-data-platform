{{
  config(
    materialized='table',
    tags=['mart', 'dimension', 'scd2'],
    contract={'enforced': false},
    indexes=[
      {'columns': ['asset_key'], 'unique': true},
      {'columns': ['trading_pair', 'is_current']},
      {'columns': ['trading_pair', 'valid_from', 'valid_to']}
    ]
  )
}}

/*
  Slowly changing dimension, type 2.

  Market-cap rank changes constantly. If we overwrote it, a report rebuilt in
  2027 would restate 2026's "top 3 by rank" — a classic silent data bug. Instead
  every snapshot gets a validity window and facts join on the row that was
  current at the time.

  ORDER OF OPERATIONS MATTERS HERE. The validity window must be derived *after*
  collapsing unchanged snapshots, not before. Computing lead() first and then
  filtering leaves each surviving version pointing at a snapshot that no longer
  exists, which opens a gap in coverage: any fact landing in that gap finds no
  matching dimension row and silently falls through to the unknown member.
*/

with snapshots as (

    select * from {{ ref('stg_assets') }}

),

flagged as (

    select
        *,
        lag(market_cap_rank) over (partition by coin_id order by extracted_at) as prior_rank
    from snapshots

),

changes_only as (

    -- Collapse consecutive snapshots where nothing we track actually changed.
    select *
    from flagged
    where prior_rank is distinct from market_cap_rank
       or prior_rank is null

),

versioned as (

    -- Only now is it safe to close each window against the *next surviving*
    -- version, which guarantees the windows tile the timeline with no gaps.
    select
        *,
        lead(extracted_at) over (partition by coin_id order by extracted_at) as next_valid_from
    from changes_only

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['coin_id', 'extracted_at']) }} as asset_key,
        coin_id,
        asset_symbol,
        trading_pair,
        asset_name,
        market_cap_usd,
        market_cap_rank,
        circulating_supply,
        max_supply,
        all_time_high_usd,
        all_time_high_date,

        case
            when max_supply > 0 then circulating_supply / max_supply
        end                                                       as supply_issued_pct,

        -- The first version is backdated so that facts predating our very first
        -- CoinGecko snapshot still resolve to a real dimension row.
        case
            when lag(extracted_at) over (partition by coin_id order by extracted_at) is null
            then '{{ var("earliest_trade_date") }}'::timestamptz
            else extracted_at
        end                                                       as valid_from,

        coalesce(next_valid_from, '9999-12-31'::timestamptz)       as valid_to,
        next_valid_from is null                                    as is_current,
        false                                                      as is_unknown_member

    from versioned

),

/*
  The unknown member. Kimball's standard answer to "a fact arrived for an entity
  the dimension has never heard of". Without it, the fact table either carries an
  orphan key that breaks referential integrity, or the row is dropped and the
  measures silently stop adding up.
*/
unknown_member as (

    select
        '-1'                          as asset_key,
        'unknown'                     as coin_id,
        'UNKNOWN'                     as asset_symbol,
        'UNKNOWN'                     as trading_pair,
        'Unknown asset'               as asset_name,
        cast(null as numeric(38, 2))  as market_cap_usd,
        cast(null as integer)         as market_cap_rank,
        cast(null as numeric(38, 6))  as circulating_supply,
        cast(null as numeric(38, 6))  as max_supply,
        cast(null as numeric(38, 12)) as all_time_high_usd,
        cast(null as date)            as all_time_high_date,
        cast(null as numeric)         as supply_issued_pct,
        '1900-01-01'::timestamptz     as valid_from,
        '9999-12-31'::timestamptz     as valid_to,
        true                          as is_current,
        true                          as is_unknown_member

)

select * from final
union all
select * from unknown_member
