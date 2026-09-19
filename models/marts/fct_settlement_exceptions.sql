-- Exceptions worth a human looking at. This is the model that replaces the
-- Presto reconciliation checks: rather than a pass/fail test that blocks the
-- build, it surfaces the rows finance actually chases.
--
-- Deliberately a table, not a test: these are business exceptions (expected
-- to be non-zero every day), not data-quality failures.

with matched as (
    select * from {{ ref('int_auth_settlement_matched') }}
)

select
    auth_id,
    merchant_id,
    auth_date,
    settled_date,
    auth_amount,
    settled_amount,
    settlement_delta,
    settlement_lag_days,
    settlement_status,

    case
        -- Settled for materially more than authorized. Tips explain ~20%;
        -- beyond that it is worth asking why.
        when settlement_delta > auth_amount * 0.25 then 'OVER_CAPTURE'
        -- Partial capture: less than half the authorized amount taken.
        when settled_amount < auth_amount * 0.5   then 'PARTIAL_CAPTURE'
        -- Outside the agreed settlement window but did eventually land.
        when settlement_lag_days > {{ var('settlement_lag_days') }} then 'LATE_SETTLEMENT'
        -- Past the window with nothing at all.
        when settlement_status = 'UNSETTLED'      then 'NEVER_SETTLED'
    end as exception_type

from matched
where
    settlement_delta > auth_amount * 0.25
    or settled_amount < auth_amount * 0.5
    or settlement_lag_days > {{ var('settlement_lag_days') }}
    or settlement_status = 'UNSETTLED'
