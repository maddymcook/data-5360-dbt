{{ config(materialized='view') }}

select
  f.order_id,
  f.order_date,
  f.order_line_id,
  f.quantity,
  f.unit_price,
  f.line_amount,
  c.first_name as customer_first_name,
  c.last_name as customer_last_name,
  c.state as customer_state,
  p.product_name,
  s.store_name,
  s.city as store_city,
  s.state as store_state
from {{ ref('fact_sales') }} f
join {{ ref('oliver_dim_customer') }} c
  on f.customer_id = c.customer_id
join {{ ref('oliver_dim_product') }} p
  on f.product_id = p.product_id
join {{ ref('oliver_dim_store') }} s
  on f.store_id = s.store_id