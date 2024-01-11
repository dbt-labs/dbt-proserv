{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id', 'valid_from']
    )
}}

with

{%- if is_incremental() %}

ids_with_new_records as (
    select id from {{ ref('mock_scd4') }}
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
),

{%- endif %}

delta_window_records as (
    select * from {{ ref('mock_scd4') }}
    {%- if is_incremental() %}
    where id in (select id from ids_with_new_records)
    {%- endif %}
),

transform_delta as (
	select 
	    id,
        order_status,
        ingestion_ts,
	    ingestion_ts as valid_from,
	    lead(ingestion_ts) over(partition by id order by ingestion_ts) as valid_to
	from delta_window_records
)

select * from transform_delta