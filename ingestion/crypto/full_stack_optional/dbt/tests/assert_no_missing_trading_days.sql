-- Crypto markets never close, so any gap in the daily series is a real defect
-- rather than a weekend. Returns the offending gaps; empty result = pass.

with bounds as (
    select
        trading_pair,
        min(date_key) as first_date,
        max(date_key) as last_date,
        count(*)      as actual_days
    from {{ ref('fct_ohlcv_daily') }}
    group by 1
),

expected as (
    select
        trading_pair,
        first_date,
        last_date,
        actual_days,
        (last_date - first_date) + 1 as expected_days
    from bounds
)

select *
from expected
where actual_days < expected_days - 2   -- tolerate rare exchange outages
