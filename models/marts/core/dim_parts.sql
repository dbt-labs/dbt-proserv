with 

parts as (
    select * from {{ref('int_part_suppliers')}}
),

final as (
    select distinct
        part_id,
        manufacturer,
        part_name,
        brand,
        part_type,
        part_size,
        container,
        retail_price
    from parts
)

select * from final
