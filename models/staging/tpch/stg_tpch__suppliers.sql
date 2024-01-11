with 

source as (
    select * from {{ source('tpch', 'supplier') }}
),

renamed as (
    select
        -- ids
        s_suppkey as supplier_id,
        s_nationkey as nation_id,

        -- dimensions
        s_name as supplier_name,
        s_address as supplier_address,
        s_phone as phone_number,
        s_comment as comment,

        -- measures
        s_acctbal as account_balance
    from source
)

select * from renamed
