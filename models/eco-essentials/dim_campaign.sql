{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_key,
    campaign_id,
    campaign_name,
    campaign_discount
FROM {{ source('ecoessentials_landing', 'promotional_campaign') }}
