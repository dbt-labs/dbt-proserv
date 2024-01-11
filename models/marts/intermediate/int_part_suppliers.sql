with 

parts as (
    select * from {{ ref('stg_tpch__parts') }}
),

suppliers as (
    select * from {{ ref('stg_tpch__suppliers') }}
),

part_suppliers as (
    select * from {{ ref('stg_tpch__part_suppliers') }}
),

final as (
    select 
        part_suppliers.part_supplier_sk,
        parts.part_id,
        parts.name as part_name,
        parts.manufacturer,
        parts.brand,
        parts.type as part_type,
        parts.size as part_size,
        parts.container,
        parts.retail_price,
        suppliers.supplier_id,
        suppliers.supplier_name,
        suppliers.supplier_address,
        suppliers.phone_number,
        suppliers.account_balance,
        suppliers.nation_id,
        part_suppliers.available_quantity,
        part_suppliers.cost
    from parts
    inner join part_suppliers
        on parts.part_id = part_suppliers.part_id
    inner join suppliers
        on part_suppliers.supplier_id = suppliers.supplier_id
)

select * from final
