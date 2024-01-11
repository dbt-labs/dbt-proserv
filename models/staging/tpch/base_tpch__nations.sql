with 

source as (
    select * from {{ source('tpch', 'nation') }}
),

renamed as (
    select
        -- ids
        n_nationkey as nation_id,
        n_regionkey as region_id,
        
        -- dimensions
        n_name as name,
        n_comment as comment,
        n_comment || 'TEST' as comment2
    from source
    
)

select * from renamed
