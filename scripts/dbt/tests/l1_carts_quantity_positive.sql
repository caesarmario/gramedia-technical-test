-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script to test l1.carts quantity not negative
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

select *
from {{ source('fakestore_l1','carts') }}
where quantity is not null
  and quantity < 1
