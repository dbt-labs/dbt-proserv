{{ config(
        materialized='incremental',
        unique_key='id',
        merge_exclude_columns = ['dbt_inserted_at']
) }}

with 

source as (
    select * from {{ ref('mock_events') }}
    {%- if is_incremental() %}
    where updated_at > (select max(source_updated_at) from {{ this }})
    {%- endif %}
),

transformed as (
    select
        id,
        order_status,
        updated_at as source_updated_at, 
        current_timestamp() as dbt_inserted_at,
        current_timestamp() as dbt_updated_at
    from source
)

select * from transformed