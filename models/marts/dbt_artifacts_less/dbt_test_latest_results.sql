{{
    config(
        materialized='table'
    )
}}

with 
source_models as (
    select
        node_id,
        name as model_name,
        database, 
        schema,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type
    from 
        {{ ref('dbt_artifacts_models') }}
    where dbt_valid_to is null

    union all
    
    select
        node_id,
        name as model_name,
        database, 
        schema,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type
    from {{ ref('dbt_artifacts_sources') }}
    where dbt_valid_to is null
),
    
source_tests as (
    select
        node_id,
        name as test_long_name,
        test_name as test_short_name,
        test_severity_config,
        column_names as test_depends_on_columns,
        test_type,
        depends_on_nodes,
        package_name,
        test_path,
        tags,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type
    from {{ ref('dbt_artifacts_tests') }}
    where dbt_valid_to is null
),

test_executions as (
    select 
        command_invocation_id,
        node_id,
        run_started_at,
        status,
        failures,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type
    from {{ ref('dbt_artifacts', 'test_executions') }}
    left join {{ ref('invocations') }} using (command_invocation_id)
    qualify row_number() over (partition by node_id, dbt_cloud_environment_name, dbt_cloud_environment_type order by run_started_at desc) = 1
),

failed as (
    select distinct
        command_invocation_id,
        node_id,
        min(run_started_at) over (partition by node_id) as first_failed_at,
        status,
        failures
    from test_executions
    where status in ('error','warn')
),

model_test as (
    select 
        source_models.model_name,
        source_models.node_id,
        source_models.database,
        source_models.schema,
        source_models.dbt_cloud_environment_name,
        source_models.dbt_cloud_environment_type,
        source_tests.test_long_name,
        source_tests.test_short_name,
        source_tests.test_severity_config,
        source_tests.test_depends_on_columns,
        source_tests.test_type,
        source_tests.depends_on_nodes,
        source_tests.package_name,
        source_tests.test_path,
        source_tests.tags,
        test_executions.node_id as test_node_id,
        test_executions.status,
        test_executions.failures,
        test_executions.run_started_at,
        failed.first_failed_at
    from source_models
    inner join source_tests
        on source_models.node_id = source_tests.depends_on_nodes[0]
        and source_models.dbt_cloud_environment_name = source_tests.dbt_cloud_environment_name
        and source_models.dbt_cloud_environment_type = source_tests.dbt_cloud_environment_type
    left join test_executions
        on source_tests.node_id = test_executions.node_id
        and test_executions.dbt_cloud_environment_name = source_tests.dbt_cloud_environment_name
        and test_executions.dbt_cloud_environment_type = source_tests.dbt_cloud_environment_type
    left join failed
        on test_executions.command_invocation_id = failed.command_invocation_id
        and test_executions.node_id = failed.node_id
)

select
    model_name,
    node_id,
    database,
    schema,
    dbt_cloud_environment_name,
    dbt_cloud_environment_type,
    test_long_name,
    test_short_name,
    test_severity_config,
    test_depends_on_columns,
    test_type,
    depends_on_nodes,
    package_name,
    test_path,
    tags,
    node_id as test_node_id,
    status,
    failures,
    run_started_at,
    first_failed_at
from 
    model_test