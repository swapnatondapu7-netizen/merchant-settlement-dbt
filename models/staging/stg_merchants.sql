with source as (
    select * from {{ ref('raw_merchants') }}
)

select
    cast(merchant_id as integer)     as merchant_id,
    cast(merchant_name as varchar)   as merchant_name,
    cast(mcc_code as varchar)        as mcc_code,
    cast(mcc_description as varchar) as mcc_description,
    cast(region as varchar)          as region,
    cast(risk_tier as varchar)       as risk_tier,
    cast(updated_at as timestamp)    as updated_at
from source
