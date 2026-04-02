{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    product_type
FROM {{ source('ecoessentials_landing', 'product') }}
