{{
  config(
    materialized='view',
    tags=['mart', 'observability']
  )
}}

/*
  Operational metrics as a first-class model.

  Pipeline health is data, so it belongs in the warehouse next to everything
  else — queryable with SQL, joinable to the facts, and visible on the same
  dashboard. Reading it out of the Airflow UI one task at a time does not scale
  past about three DAGs, and the Airflow metadata database answers "did the task
  run?" rather than "did it load what it should have?".
*/

with audit as (

    select * from {{ source('raw', 'pipeline_audit') }}

),

per_task as (

    select
        dag_id,
        task_id,
        count(*)                                                     as total_runs,
        sum(case when status = 'success' then 1 else 0 end)          as successful_runs,
        sum(case when status = 'failed'  then 1 else 0 end)          as failed_runs,

        round(
            100.0 * sum(case when status = 'success' then 1 else 0 end)
            / nullif(count(*), 0)
        , 2)                                                         as success_rate_pct,

        round(avg(duration_sec)::numeric, 2)                         as avg_duration_sec,
        round(
            (percentile_cont(0.95) within group (order by duration_sec))::numeric
        , 2)                                                         as p95_duration_sec,

        avg(row_count)                                               as avg_row_count,
        max(logical_date)                                            as last_run_date,
        current_date - max(logical_date)                             as days_since_last_run

    from audit
    group by 1, 2

),

recent as (

    -- Trailing 7 days, so a task that has been failing since yesterday is
    -- visible even if its all-time success rate still looks healthy.
    select
        dag_id,
        task_id,
        sum(case when status = 'failed' then 1 else 0 end)           as failures_last_7d,
        max(case when status = 'failed' then logical_date end)       as last_failure_date
    from audit
    where logical_date >= current_date - 7
    group by 1, 2

)

select
    per_task.dag_id,
    per_task.task_id,
    per_task.total_runs,
    per_task.successful_runs,
    per_task.failed_runs,
    per_task.success_rate_pct,
    per_task.avg_duration_sec,
    per_task.p95_duration_sec,
    per_task.avg_row_count,
    per_task.last_run_date,
    per_task.days_since_last_run,
    coalesce(recent.failures_last_7d, 0)                             as failures_last_7d,
    recent.last_failure_date,

    case
        when per_task.days_since_last_run > 2            then 'stale'
        when coalesce(recent.failures_last_7d, 0) >= 3   then 'degraded'
        when per_task.success_rate_pct < 90              then 'unreliable'
        else 'healthy'
    end                                                              as health_status

from per_task
left join recent using (dag_id, task_id)
