{{ config(materialized='view') }}

select
  employee_id,
  first_name,
  last_name,
  email,
  phone_number,
  position,
  hire_date
from {{ source('oliver', 'employee') }}