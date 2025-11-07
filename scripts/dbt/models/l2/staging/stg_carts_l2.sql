-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script for stg carts in l2
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{{ config(schema='l2', materialized='view') }}
{% set rel = source('fakestore_l1','carts') %}
{% set cols = adapter.get_columns_in_relation(rel) %}
{% set colnames = cols | map(attribute='name') | map('lower') | list %}

with src as (
    select
        cart_id,
        product_id,
        quantity::numeric as quantity,

        {% if 'order_date' in colnames %}
            order_date::timestamp as transaction_date,
        {% elif 'date' in colnames %}
            "date"::timestamp as transaction_date,
        {% else %}
            ds::timestamp as transaction_date,
        {% endif %}

        ds as load_ds
    from {{ rel }}
    {% if var('batch_ds', none) is not none %}
      where ds::date = {{ "'" ~ var('batch_ds') ~ "'" }}::date
    {% endif %}
)
select * from src