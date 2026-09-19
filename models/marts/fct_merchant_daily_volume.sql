{{
    config(
        materialized = 'incremental',
        unique_key = ['merchant_id', 'auth_date'],
        incremental_strategy = 'delete+insert'
    )
}}

-- Merchant-by-day authorized vs settled volume.
--
-- THE POINT OF THIS MODEL
--   A settlement can land up to `settlement_lag_days` after its
--   authorization. So on any given run, days that were already "finished"
--   can still change. An incremental model that only processes rows where
--   auth_date = today is WRONG here: the late settlement arrives, belongs to
--   a day the model will never revisit, and that day under-reports forever.
--
--   The fix is to reprocess a trailing window on every run and replace those
--   days wholesale (delete+insert on the unique key). It costs a few extra
--   days of compute and makes the number reproducible, which is the trade
--   any finance consumer wants.

with matched as (
    select * from {{ ref('int_auth_settlement_matched') }}

    {% if is_incremental() %}
    -- Reprocess the trailing window, not just the newest day, so late
    -- settlements can still correct the day they belong to.
    where auth_date >= (
        select coalesce(max(auth_date), '1900-01-01'::date)
                 - interval '{{ var("settlement_lag_days") }} days'
        from {{ this }}
    )
    {% endif %}
)

select
    merchant_id,
    auth_date,

    count(*)                                             as auth_count,
    sum(auth_amount)                                     as authorized_amount,

    count(settlement_id)                                 as settled_count,
    coalesce(sum(settled_amount), 0)                     as settled_amount,

    count(*) - count(settlement_id)                      as open_auth_count,
    sum(auth_amount) - coalesce(sum(settled_amount), 0)  as settlement_gap,

    -- Guard the divide: a day can legitimately have zero authorized amount.
    case
        when sum(auth_amount) > 0
        then round(coalesce(sum(settled_amount), 0) / sum(auth_amount), 4)
    end                                                  as settled_ratio,

    avg(settlement_lag_days)                             as avg_settlement_lag_days,
    max(settlement_lag_days)                             as max_settlement_lag_days,
    current_timestamp                                    as _loaded_at

from matched
group by 1, 2
