with 

source as (
    select * from {{ source('tpch', 'part') }}
),

renamed as (
    select
        -- ids
        p_partkey as part_id,

        -- dimensions
        p_name as name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as type,
        p_size as size,
        p_container as container,
        p_comment as comment,

        -- measures
        p_retailprice as retail_price
    from source
)

select * from renamed
