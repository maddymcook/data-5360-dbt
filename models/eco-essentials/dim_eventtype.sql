{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['EVENTTYPE']) }} as eventtype_key,
    EVENTTYPE
from {{ source('ecoessentials_landing', 'marketingemails') }}
where EVENTTYPE is not null