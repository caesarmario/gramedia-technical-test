-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script to test l1.products rating not negative
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

select *
from {{ source('fakestore_l1','products') }}
where rating_count < 0
