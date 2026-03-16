{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
) }}

with db_source as (

    select
        customer_id,
        first_name,
        last_name,
        cast(null as date) as dob,
        cast(null as varchar) as address,
        cast(null as varchar) as city,
        state,
        cast(null as varchar) as zipcode,
        email as customer_email
    from {{ source('insurance_landing', 'customer') }}

),

cs_interactions_source as (

    select distinct
        customer_first_name,
        customer_last_name,
        customer_email
    from {{ ref('stg_customer_service_interactions') }}

),

final_cte as (

    select
        db.customer_id,
        coalesce(db.first_name, cs.customer_first_name) as firstname,
        coalesce(db.last_name, cs.customer_last_name) as lastname,
        db.dob,
        db.address,
        db.city,
        db.state,
        db.zipcode,
        coalesce(db.customer_email, cs.customer_email) as customer_email
    from db_source db
    full join cs_interactions_source cs
        on db.first_name = cs.customer_first_name
       and db.last_name = cs.customer_last_name

)

select
    md5(coalesce(firstname, '') || '|' || coalesce(lastname, '')) as customer_key,
    *
from final_cte