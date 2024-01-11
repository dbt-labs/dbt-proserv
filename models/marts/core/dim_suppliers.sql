with 

suppliers as (
    select * from {{ ref('stg_tpch__suppliers') }}
),

locations as (
    select * from {{ ref('stg_tpch__locations') }}
),

final as (
    select
        suppliers.supplier_id,
        suppliers.supplier_name,
        suppliers.supplier_address,
        locations.nation,
        locations.region,
        suppliers.phone_number,
        suppliers.account_balance
    from suppliers
    inner join locations
        on suppliers.nation_id = locations.nation_id
)

select * from final
