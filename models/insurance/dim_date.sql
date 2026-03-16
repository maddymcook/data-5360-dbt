{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
) }}

with dates as (

    select distinct
        cast(interaction_date as date) as date_key
    from {{ ref('stg_customer_service_interactions') }}

)

select
    date_key
from dates