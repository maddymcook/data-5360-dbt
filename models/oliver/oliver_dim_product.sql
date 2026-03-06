{{ config(materialized='view') }}

select
  product_id,
  product_name,
  description,
  unit_price
from {{ source('oliver', 'product') }}