-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script for fact sales table
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(schema='l2', materialized='table') }}

with c as (
    select * from {{ ref('stg_carts_l2') }}
),
p as (
    select * from {{ ref('stg_products_l2') }}
),
joined as (
    select
        -- synthetic row-level id (cart line item)
        md5(concat_ws('||',
            c.cart_id::text,
            c.product_id::text,
            coalesce(to_char(c.transaction_date,'YYYYMMDDHH24MISS'),'')
        )) as transaction_id,

        c.product_id,
        p.product_name,
        p.category_clean as category,
        c.quantity,
        p.price,
        (c.quantity * p.price) as total_sales,

        c.transaction_date,
        c.load_ds
    from c
    left join p using (product_id)
)
select * from joined
