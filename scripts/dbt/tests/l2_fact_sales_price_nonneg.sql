-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script to test l2.fact_sales price
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

select *
from {{ ref('fact_sales') }}
where price < 0
