{{ config(materialized='view') }}

select
  customer_id,
  first_name,
  last_name,
  email,
  phone_number,
  state
from {{ source('oliver', 'customer') }}