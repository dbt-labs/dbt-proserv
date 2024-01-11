with 

orders as (
    select * from {{ ref('stg_tpch__orders') }}
),

line_items as (
    select * from {{ ref('stg_tpch__line_items') }}
),

final as (
    select 
        orders.order_id,
        orders.customer_id,

        {{ dbt_utils.generate_surrogate_key([
            'line_items.part_id',
            'line_items.supplier_id'
        ]) }} as part_supplier_sk,

        orders.order_date,
        orders.status_code,
        orders.priority_code,
        orders.clerk_name,
        orders.ship_priority,
        line_items.order_item_sk,
        line_items.part_id,
        line_items.supplier_id,
        line_items.return_flag,
        line_items.line_number,
        line_items.status_code as order_item_status_code,
        line_items.ship_date,
        line_items.commit_date,
        line_items.receipt_date,
        line_items.ship_mode,
        line_items.extended_price,
        line_items.quantity,
        line_items.base_price,
        line_items.discount_percentage, 
        line_items.discounted_price,
        line_items.extended_price as gross_item_sales_amount,
        line_items.discounted_item_sales_amount,
        line_items.item_discount_amount,
        line_items.tax_rate,
        line_items.item_tax_amount,
        line_items.net_item_sales_amount
    from orders
    left join line_items
            on orders.order_id = line_items.order_id
)

select * from final
