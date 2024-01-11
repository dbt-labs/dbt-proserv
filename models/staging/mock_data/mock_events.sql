with

data as (

    {# ---------------------------------------------------------------------------- #
     # Day 1:
     # + Two people submit an order (records 1 and 2)
     # ---------------------------------------------------------------------------- #}
    select 1 as id, 'Pending' as order_status, '2023-11-29'::timestamp as updated_at
    union all
    select 2 as id, 'Pending' as order_status, '2023-11-29'::timestamp as updated_at

    {# ---------------------------------------------------------------------------- #
     # Day 2:
     # + Record 2 has a change to it's order status. 
     # + Another person submits an order (record 3).
     # ---------------------------------------------------------------------------- #}
    -- select 1 as id, 'Pending' as order_status, '2023-11-29'::timestamp as updated_at
    -- union all
    -- select 2 as id, 'Shipped' as order_status, '2023-11-30'::timestamp as updated_at
    -- union all
    -- select 3 as id, 'Pending' as order_status, '2023-11-30'::timestamp as updated_at

    {# ---------------------------------------------------------------------------- #
     # Day 3:
     # + Record 1 and 3 have a change to their order statuses.
     # + Two more people submit an order (records 4 and 5).
     # ---------------------------------------------------------------------------- #}
    -- select 1 as id, 'Shipped' as order_status, '2023-12-01'::timestamp as updated_at
    -- union all
    -- select 2 as id, 'Shipped' as order_status, '2023-11-30'::timestamp as updated_at
    -- union all
    -- select 3 as id, 'Shipped' as order_status, '2023-12-01'::timestamp as updated_at
    -- union all
    -- select 4 as id, 'Pending' as order_status, '2023-12-01'::timestamp as updated_at
    -- union all
    -- select 5 as id, 'Pending' as order_status, '2023-12-01'::timestamp as updated_at
)

select * from data