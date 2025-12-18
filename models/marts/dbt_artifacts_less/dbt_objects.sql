{{
    config(
        materialized='view'
    )
}}

with
model_raw as (
    select
        dbt_scd_id,
        node_id,
        'model' as execution_type,
        run_started_at,
        database,
        schema,
        name,
        alias,
        null as source_name,
        null as test_name,
        null as test_severity_config,
        null as test_depends_on_columns,
        depends_on_nodes,
        package_name,
        path,
        materialization,
        tags,
        meta,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref("dbt_artifacts_models") }}
),

test_raw as (
    select
        dbt_scd_id,
        node_id,
        'test' as execution_type,
        run_started_at,
        'test' as database,
        'test' as schema,
        name,
        null as alias,
        null as source_name,
        test_name,
        test_severity_config,
        column_names as test_depends_on_columns,
        depends_on_nodes,
        package_name,
        test_path as path,
        concat('test ', test_type) as materialization,
        tags,
        null as meta,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('dbt_artifacts_tests') }}
),

snapshot_raw as (
    select
        dbt_scd_id,
        node_id,
        'snapshot' as execution_type,
        run_started_at,
        database,
        schema,
        name,
        alias,
        null as source_name,
        null as test_name,
        null as test_severity_config,
        null as test_depends_on_columns,
        depends_on_nodes,
        package_name,
        path,
        concat('snapshot ', strategy) as materialization,
        null as tags,
        meta,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref("dbt_artifacts_snapshots") }}
),

seed_raw as (
    select
        dbt_scd_id,
        node_id,
        'seed' as execution_type,
        run_started_at,
        database,
        schema,
        name,
        alias,
        null as source_name,
        null as test_name,
        null as test_severity_config,
        null as test_depends_on_columns,
        null as depends_on_nodes,
        package_name,
        path,
        'seed' as materialization,
        null as tags,
        meta,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref("dbt_artifacts_seeds") }} 
),

source_raw as (
    select
        dbt_scd_id,
        node_id,
        'source' as execution_type,
        run_started_at,
        database,
        schema,
        name,
        identifier as alias,
        source_name,
        null as test_name,
        null as test_severity_config,
        null as test_depends_on_columns,
        null as depends_on_nodes,
        null as package_name,
        null as path,
        'source' as materialization,
        null as tags,
        null as meta,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref("dbt_artifacts_sources") }}
),

exposure_raw as (
    select
        dbt_scd_id,
        node_id,
        'exposure' as execution_type,
        run_started_at,
        null as database,
        null as schema,
        name,
        null as alias,
        null as source_name,
        null as test_name,
        null as test_severity_config,
        null as test_depends_on_columns,
        depends_on_nodes,
        package_name,
        path,
        'exposure' as materialization,
        tags,
        null as meta,
        null as dbt_cloud_environment_name,
        null as dbt_cloud_environment_type,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref("dbt_artifacts_exposures") }}
),

unioned as (
    select * from model_raw
        union all
    select * from test_raw
        union all
    select * from snapshot_raw
        union all
    select * from seed_raw
        union all
    select * from source_raw
        union all
    select * from exposure_raw
)

select * from unioned