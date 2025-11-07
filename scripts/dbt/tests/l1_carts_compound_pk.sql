-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- dbt sql script to test l1.carts uniqueness cart id & product id
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

with dupes as (
  select cart_id, product_id, count(*) as cnt
  from {{ source('fakestore_l1','carts') }}
  group by 1,2
  having count(*) > 1
)
select * from dupes
