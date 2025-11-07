-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script for dim product table
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(schema='l2', materialized='table') }}

with p as (
    select
        product_id,
        product_name,
        category_clean as category,
        price,
        max(load_ds) over (partition by product_id) as last_ingested_ds
    from {{ ref('stg_products_l2') }}
)
select distinct
    product_id,
    product_name,
    category,
    price,
    last_ingested_ds
from p
