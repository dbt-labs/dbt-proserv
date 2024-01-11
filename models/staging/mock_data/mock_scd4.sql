with

data as (

    {# ---------------------------------------------------------------------------- #
     # Day 1:
     # + Two people submit an order (records 1 and 2)
     # ---------------------------------------------------------------------------- #}
    select 1 as id, 'Pending' as order_status, cast('2023-11-29' as timestamp) as ingestion_ts
    union all
    select 2 as id, 'Pending' as order_status, cast('2023-11-29' as timestamp) as ingestion_ts

    {# ---------------------------------------------------------------------------- #
     # Day 2:
     # + Record 2 has a change to it's order status and adds a row for the change.
     # + Another person submits an order (record 3).
     # ---------------------------------------------------------------------------- #}
    -- select 1 as id, 'Pending' as order_status, cast('2023-11-29' as timestamp) as ingestion_ts
    -- union all
    -- select 2 as id, 'Pending' as order_status, cast('2023-11-29' as timestamp) as ingestion_ts
    -- union all
    -- select 2 as id, 'Shipped' as order_status, cast('2023-11-30' as timestamp) as ingestion_ts
    -- union all
    -- select 3 as id, 'Pending' as order_status, cast('2023-11-30' as timestamp) as ingestion_ts

    {# ---------------------------------------------------------------------------- #
     # Day 3:
     # + Record 1 and 3 have a change to their order statuses and add rows for 
     #   the changes.
     # + Two more people submit an order (records 4 and 5).
     # ---------------------------------------------------------------------------- #}
    -- select 1 as id, 'Pending' as order_status, cast('2023-11-29' as timestamp) as ingestion_ts
    -- union all
    -- select 1 as id, 'Shipped' as order_status, cast('2023-12-01' as timestamp) as ingestion_ts
    -- union all
    -- select 2 as id, 'Pending' as order_status, cast('2023-11-29' as timestamp) as ingestion_ts
    -- union all
    -- select 2 as id, 'Shipped' as order_status, cast('2023-11-30' as timestamp) as ingestion_ts
    -- union all
    -- select 3 as id, 'Pending' as order_status, cast('2023-11-30' as timestamp) as ingestion_ts
    -- union all
    -- select 3 as id, 'Shipped' as order_status, cast('2023-12-01' as timestamp) as ingestion_ts
    -- union all
    -- select 4 as id, 'Pending' as order_status, cast('2023-12-01' as timestamp) as ingestion_ts
    -- union all
    -- select 5 as id, 'Pending' as order_status, cast('2023-12-01' as timestamp) as ingestion_ts
)

select * from data