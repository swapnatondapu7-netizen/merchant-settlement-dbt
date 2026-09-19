-- Authorization stream: one row per auth attempt, at the moment the card was
-- presented. Nothing is joined here on purpose - staging only renames, casts
-- and lightly cleans, so that every downstream model reads the same shapes.
with source as (
    select * from {{ ref('raw_authorizations') }}
)

select
    cast(auth_id as bigint)            as auth_id,
    cast(merchant_id as integer)       as merchant_id,
    cast(card_token as varchar)        as card_token,
    cast(auth_amount as decimal(12,2)) as auth_amount,
    cast(auth_ts as timestamp)         as authorized_at,
    cast(auth_date as date)            as auth_date,
    cast(currency as varchar)          as currency
from source
