with 

source as (
    select * from {{ source('tpch', 'lineitem') }}
),

cleaned as (
    select
        -- ids
        {{ dbt_utils.generate_surrogate_key(
            ['l_orderkey', 
            'l_linenumber']
        ) }} as order_item_sk,

        l_orderkey as order_id,
        l_linenumber as line_number,
        l_partkey as part_id,
        l_suppkey as supplier_id,

        -- dimensions
        l_returnflag as return_flag,
        l_linestatus as status_code,
        l_shipinstruct as ship_instructions,
        l_shipmode as ship_mode,
        l_comment as comment,
        
        -- measures
        nullif(l_quantity, 0) as quantity,
        l_extendedprice as extended_price,
        l_discount as discount_percentage,
        l_tax as tax_rate,

        -- date/times
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date
    from source
),

base_calcs as (
    select
        *,
        extended_price/quantity as base_price,
        1 - discount_percentage as compliment_percentage,
        -(extended_price * discount_percentage) as item_discount_amount
    from cleaned
),

amounts as (
    select
        *,
        base_price * compliment_percentage as discounted_price,
        extended_price * compliment_percentage as discounted_item_sales_amount,
        (extended_price + item_discount_amount) * tax_rate as item_tax_amount
    from base_calcs
),

net as (
    select
        *,
        extended_price + item_discount_amount + item_tax_amount as net_item_sales_amount
    from amounts
)

select * from net
