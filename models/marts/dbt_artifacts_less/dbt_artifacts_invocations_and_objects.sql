with

invocations as (
    select * from {{ ref('dbt_invocations') }}
),
objects as (
    select * from {{ ref('dbt_objects') }}
),

final as (
    select
        invocations.execution_id,
        invocations.execution_type,
        invocations.command_invocation_id,
        invocations.node_id,
        invocations.run_started_at,
        invocations.was_full_refresh,
        invocations.thread_id,
        invocations.status,
        invocations.compile_started_at,
        invocations.query_completed_at,
        invocations.total_node_runtime,
        invocations.rows_affected,
        invocations.materialization,
        invocations.schema,
        invocations.name,
        invocations.alias,
        invocations.failures,
        invocations.message,
        invocations.query_id,
        invocations.dbt_version,
        invocations.project_name,
        invocations.invocation_started_at,
        invocations.dbt_command,
        invocations.full_refresh_flag,
        invocations.target_profile_name,
        invocations.target_name,
        invocations.target_schema,
        invocations.target_threads,
        invocations.dbt_cloud_project_id,
        invocations.dbt_cloud_job_id,
        invocations.dbt_cloud_run_id,
        invocations.dbt_cloud_run_reason_category,
        invocations.dbt_cloud_run_reason,
        invocations.env_vars,
        invocations.dbt_vars,
        invocations.invocation_args,
        invocations.dbt_custom_envs,
        invocations.dbt_cloud_environment_name,
        invocations.dbt_cloud_environment_type,
        objects.dbt_scd_id,
        objects.database as object_database,
        objects.schema as object_schema,
        objects.name as object_name,
        objects.alias as object_alias,
        objects.source_name,
        objects.test_name,
        objects.test_severity_config,
        objects.test_depends_on_columns,
        objects.depends_on_nodes,
        objects.package_name,
        objects.path,
        objects.materialization as object_materialization,
        objects.tags,
        objects.meta,
        objects.dbt_updated_at,
        objects.dbt_valid_from

    from invocations 
    left join objects
        on invocations.node_id = objects.node_id
        and invocations.execution_type = objects.execution_type
        and invocations.dbt_cloud_environment_name = objects.dbt_cloud_environment_name
        and invocations.dbt_cloud_environment_type = objects.dbt_cloud_environment_type
        and invocations.run_started_at >= objects.dbt_valid_from
        and (invocations.run_started_at < objects.dbt_valid_to or objects.dbt_valid_to is null)
)

select * from final
