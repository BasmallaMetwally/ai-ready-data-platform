-- The defining invariant of an SCD2: exactly one open version per entity.

select
    coin_id,
    count(*) as current_versions
from {{ ref('dim_asset') }}
where is_current
group by 1
having count(*) > 1
