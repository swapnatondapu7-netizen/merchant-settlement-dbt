-- Match each authorization to its settlement, if one has arrived yet.
--
-- WHY A LEFT JOIN AND NOT AN INNER JOIN
--   An inner join silently drops two populations that the business cares
--   about most: authorizations that have not settled YET (normal, they are
--   inside the lag window) and ones that will NEVER settle (reversed,
--   abandoned). Both look identical to a naive join - they just vanish - and
--   the merchant's reported volume comes out too low with no error anywhere.
--
-- WHY THE GRAIN IS THE AUTHORIZATION
--   One auth produces at most one settlement in this model. Keeping the grain
--   on the auth side means downstream counts of "authorized" are never
--   inflated by the join, which is the classic fan-out bug in this domain.

with auth as (
    select * from {{ ref('stg_authorizations') }}
),

settled as (
    select * from {{ ref('stg_settlements') }}
),

joined as (
    select
        a.auth_id,
        a.merchant_id,
        a.card_token,
        a.auth_amount,
        a.authorized_at,
        a.auth_date,
        s.settlement_id,
        s.settled_amount,
        s.settled_at,
        s.settled_date,

        -- How long the settlement took. Null while still outstanding.
        case
            when s.settled_date is not null
            then date_diff('day', a.auth_date, s.settled_date)
        end as settlement_lag_days,

        -- The money question: authorized vs actually captured.
        case
            when s.settled_amount is not null
            then s.settled_amount - a.auth_amount
        end as settlement_delta,

        case
            when s.settlement_id is not null then 'SETTLED'
            when date_diff('day', a.auth_date, current_date)
                 <= {{ var('settlement_lag_days') }} then 'PENDING'
            else 'UNSETTLED'
        end as settlement_status

    from auth a
    left join settled s
        on a.auth_id = s.auth_id
)

select * from joined
