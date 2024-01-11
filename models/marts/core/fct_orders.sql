with 

orders as (
    select * from {{ ref('int_order_items') }}
),

order_item_summary as (
    select 
        order_id,
        any_value(order_date) as order_date,
        any_value(customer_id) as customer_id,
        any_value(status_code) as status_code,
        any_value(priority_code) as priority_code,
        any_value(clerk_name) as clerk_name,
        any_value(ship_priority) as ship_priority,
        1 as order_count,
        round(sum(gross_item_sales_amount), 2) as gross_item_sales_amount,
        round(sum(item_discount_amount), 2) as item_discount_amount,
        round(sum(item_tax_amount), 2) as item_tax_amount,
        round(sum(net_item_sales_amount), 2) as net_item_sales_amount
    from orders
    group by 1
)

select * from order_item_summary
