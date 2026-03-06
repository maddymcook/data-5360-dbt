{{ config(materialized='view') }}

select
  store_id,
  store_name,
  street,
  city,
  state
from {{ source('oliver', 'store') }}