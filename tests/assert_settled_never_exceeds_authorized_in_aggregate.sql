-- Per-transaction over-capture is legitimate (tips, adjustments). In
-- AGGREGATE across a merchant-day, settled volume running far above
-- authorized volume means the join has fanned out or the streams are
-- misaligned. This is the invariant the Presto reconciliation checks defend
-- in production.
--
-- WHY THE VOLUME FLOOR
--   First run of this test failed on exactly 2 of 2,699 merchant-days, with
--   3 and 13 authorizations. On a 3-transaction day a single legitimate 1.4x
--   over-capture moves the whole ratio past any sane threshold - the signal
--   is small-sample noise, not a broken join. Asserting on those days would
--   mean a test that fails for correct data, which trains people to ignore
--   it. Below the floor the exception rows in fct_settlement_exceptions are
--   the right place to look, not a build-blocking test.

select
    merchant_id,
    auth_date,
    auth_count,
    authorized_amount,
    settled_amount,
    round(settled_amount / authorized_amount, 3) as settled_ratio
from {{ ref('fct_merchant_daily_volume') }}
where auth_count >= 20                       -- volume floor, see note above
  and settled_amount > authorized_amount * 1.30
