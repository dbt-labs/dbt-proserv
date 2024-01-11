with

data as (
    {# ---------------------------------------------------------------------------- #
     # Day 1:
     # + Three customers have records in a customers table.
     # ---------------------------------------------------------------------------- #}
    select 1 as id, 'Christine' as first_name, '123 Yellow Street' as address, '2023-11-30'::timestamp as updated_at
    union all
    select 2 as id, 'John' as first_name, '456 Red Street' as address, '2023-11-29'::timestamp as updated_at
    union all
    select 3 as id, 'Dexter' as first_name, '789 Blue Street' as address, '2023-11-29'::timestamp as updated_at

    {# ---------------------------------------------------------------------------- #
     # Day 2:
     # + Customer 1 has a change to their address.
     # ---------------------------------------------------------------------------- #}
    -- select 1 as id, 'Christine' as first_name, '1234 Orange Grove Ln.' as address, '2023-12-05'::timestamp as updated_at
    -- union all
    -- select 2 as id, 'John' as first_name, '456 Red Street' as address, '2023-11-29'::timestamp as updated_at
    -- union all
    -- select 3 as id, 'Dexter' as first_name, '789 Blue Street' as address, '2023-11-29'::timestamp as updated_at

    {# ---------------------------------------------------------------------------- #
     # Day 3:
     # + Customer 1 has another change to their address.
     # ---------------------------------------------------------------------------- #}
    -- select 1 as id, 'Christine' as first_name, '5678 Purple Plum Blvd.' as address, '2023-12-10'::timestamp as updated_at
    -- union all
    -- select 2 as id, 'John' as first_name, '456 Red Street' as address, '2023-11-29'::timestamp as updated_at
    -- union all
    -- select 3 as id, 'Dexter' as first_name, '789 Blue Street' as address, '2023-11-29'::timestamp as updated_at
)

select * from data