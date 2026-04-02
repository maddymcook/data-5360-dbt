{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['EMAILID']) }} as email_key,
    EMAILID,
    EMAILNAME
from {{ source('ecoessentials_landing', 'marketingemails') }}
where EMAILID is not null