{% snapshot dbt_artifacts_exposures %}
    {{
        config(
            target_database='development',
            target_schema='dbt_pkearns_snapshots',
            unique_key='node_id',
            strategy='timestamp',
            updated_at='run_started_at',
            invalidate_hard_deletes=true
        )
    }}

with
    
base as (
    select * from {{ ref("exposures") }}
),

enhanced as (
    select
        node_id,
        run_started_at,
        name,
        type,
        owner,
        maturity,
        path,
        description,
        url,
        package_name,
        depends_on_nodes,
        tags,
        checksum
    from base
    where
        node_id in 
        {{ get_unique_nodes(type='exposures') }}
    qualify ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY run_started_at desc) = 1
    )

select *
from enhanced

{% endsnapshot %}