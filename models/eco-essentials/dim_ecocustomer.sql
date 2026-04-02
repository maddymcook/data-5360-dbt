{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_phone,
    customer_email,
    customer_address,
    customer_city,
    customer_state,
    customer_zip,
    customer_country
FROM {{ source('ecoessentials_landing', 'customer') }}
