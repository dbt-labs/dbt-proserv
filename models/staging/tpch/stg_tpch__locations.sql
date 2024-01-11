with 

nations as (
    select * from {{ ref('base_tpch__nations') }}
),

regions as (
    select * from {{ ref('base_tpch__regions') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'regions.region_id',            
            'nations.nation_id'
        ]) }} as location_sk,

        regions.region_id,
        regions.name as region,
        regions.comment as region_comment,

        nations.nation_id,
        nations.name as nation,
        nations.comment as nation_comment
    from regions
    left join nations
        on regions.region_id = nations.region_id
)

select * from final
