{% snapshot snap_merchants %}
{{
    config(
        target_schema = 'main',
        unique_key = 'merchant_id',
        strategy = 'timestamp',
        updated_at = 'updated_at',
        invalidate_hard_deletes = True
    )
}}

-- SCD Type 2 history for the merchant dimension.
--
-- WHY THIS IS NOT AN OVERWRITE
--   Risk tier and region change over time. If the dimension is overwritten,
--   every historical fact silently re-attributes itself to the merchant's
--   CURRENT attributes - so last quarter's volume by risk tier changes every
--   time someone re-tiers a merchant, and a report run twice gives two
--   answers. The snapshot keeps dbt_valid_from / dbt_valid_to so a fact can
--   join to the version of the merchant that was true on its own date.

select * from {{ ref('stg_merchants') }}

{% endsnapshot %}
