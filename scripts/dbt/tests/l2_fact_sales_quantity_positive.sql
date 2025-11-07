-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script to test l2.fact_sales quantity
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

select *
from {{ ref('fact_sales') }}
where quantity <= 0