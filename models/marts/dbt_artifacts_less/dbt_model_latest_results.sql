{{
    config(
        materialized='table'
    )
}}

with
models as (
    select
        node_id,
        name as model_name,
        database, 
        schema,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type,
    from {{ ref('dbt_artifacts_models') }}
    where dbt_valid_to is null
),

model_executions as (
    select
        node_id,
        run_started_at,
        total_node_runtime,
        status,
        materialization,
        rows_affected,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type
    from {{ ref('model_executions') }}
    left join {{ ref('invocations') }} using (command_invocation_id)
    qualify row_number() over (partition by node_id, dbt_cloud_environment_name, dbt_cloud_environment_type order by run_started_at desc) = 1
),

executions as (
    select 
        models.node_id,
        models.model_name,
        models.database,
        models.schema,
        model_executions.materialization,
        model_executions.status,
        model_executions.run_started_at,
        model_executions.total_node_runtime,
        sum(rows_affected) over (partition by models.node_id, model_executions.run_started_at) as rows_affected
    from models
    left join model_executions
        on models.node_id = model_executions.node_id
        and models.dbt_cloud_environment_name = model_executions.dbt_cloud_environment_name
        and models.dbt_cloud_environment_type = model_executions.dbt_cloud_environment_type
)
    
select * from executions
