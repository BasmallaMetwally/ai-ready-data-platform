-- A >90% single-day move is almost always a data error (wrong decimal, a
-- delisted pair, or a stale prior close) rather than a real market event.

with moves as (
    select
        trading_pair,
        date_key,
        close_price,
        prior_close_price,
        abs(daily_return) as abs_return
    from {{ ref('fct_ohlcv_daily') }}
    where prior_close_price is not null
)

select *
from moves
where abs_return > 0.9
