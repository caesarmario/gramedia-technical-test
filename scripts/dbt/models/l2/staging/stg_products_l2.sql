-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script for stg products in l2
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################


{{ config(schema='l2', materialized='view') }}

-- L1 source
with src as (
    select
        product_id,
        title as product_name,
        {{ clean_category('category') }} as category_clean,
        price::numeric as price,
        ds as load_ds
    from {{ source('fakestore_l1','products') }}
    {% if var('batch_ds', none) is not none %}
      where ds::date = {{ "'" ~ var('batch_ds') ~ "'" }}::date
    {% endif %}
    
)
select * from src
