with 

source as (
    select * from {{ source('tpch', 'orders') }}
),

renamed as (
    select
        -- ids
        o_orderkey as order_id,
        o_custkey as customer_id,

        -- dimensions
        o_orderstatus as status_code,
        o_totalprice as total_price,
        o_orderpriority as priority_code,
        o_clerk as clerk_name,
        o_shippriority as ship_priority,
        o_comment as comment,

        -- date/times
        o_orderdate as order_date
    from source
)

select * from renamed
