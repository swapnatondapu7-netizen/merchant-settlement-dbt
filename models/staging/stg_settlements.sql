-- Settlement stream. Arrives 0-5 days after the authorization it belongs to,
-- and the amount can differ (tips, partial captures). Both facts are why the
-- downstream join cannot be a naive equality on the same day.
with source as (
    select * from {{ ref('raw_settlements') }}
)

select
    cast(settlement_id as varchar)        as settlement_id,
    cast(auth_id as bigint)               as auth_id,
    cast(merchant_id as integer)          as merchant_id,
    cast(settled_amount as decimal(12,2)) as settled_amount,
    cast(settled_ts as timestamp)         as settled_at,
    cast(settled_date as date)            as settled_date,
    cast(currency as varchar)             as currency
from source
