{{
    config(
        materialized='incremental',
        unique_key=['execution_id'],
        on_schema_change='append_new_columns',
    )
}}

with
model_raw as (
    select
        {{ dbt_artifacts.generate_surrogate_key(["command_invocation_id", "node_id"]) }} as execution_id,
        'model' as execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        {{ split_part("thread_id", "'-'", 2) }} as thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        materialization,
        schema,
        name,
        alias,
        message,
        adapter_response
    from {{ ref("model_executions") }}
    where 1=1
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and run_started_at > (select max(tablex.run_started_at) from {{ this }} as tablex)
    {% endif %}    
),

test_raw as (
    select
        {{ dbt_artifacts.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_id,
        'test' as execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        {{ split_part('thread_id', "'-'", 2) }} as thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        failures,
        message,
        adapter_response
    from {{ ref('test_executions') }}
    where 1=1
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and run_started_at > (select max(tablex.run_started_at) from {{ this }} as tablex)
    {% endif %}
),

snapshot_raw as (
    select
        {{ dbt_artifacts.generate_surrogate_key(["command_invocation_id", "node_id"]) }} as execution_id,
        'snapshot' as execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        {{ split_part("thread_id", "'-'", 2) }} as thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        materialization,
        schema,
        name,
        alias,
        message,
        adapter_response
    from {{ ref("snapshot_executions") }}
    where 1=1
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and run_started_at > (select max(tablex.run_started_at) from {{ this }} as tablex)
    {% endif %}    
),

seed_raw as (
    select
        {{ dbt_artifacts.generate_surrogate_key(["command_invocation_id", "node_id"]) }} as execution_id,
        'seed' as execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        {{ split_part("thread_id", "'-'", 2) }} as thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        materialization,
        schema,
        name,
        alias,
        message,
        adapter_response
    from {{ ref("seed_executions") }}
    where 1=1
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and run_started_at > (select max(tablex.run_started_at) from {{ this }} as tablex)
    {% endif %}    
),

invocation_raw as (
    select * from {{ ref('invocations') }}
    where 1=1
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and run_started_at > (select max(tablex.run_started_at) from {{ this }} as tablex)
    {% endif %}     
),

all_invocations as (
    select
        execution_id,
        execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        materialization,
        schema,
        name,
        alias,
        null as failures,
        message,
        adapter_response:query_id::string as query_id 
    from model_raw

    union all 

    select
        execution_id,
        execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        'test' as materialization,
        'test' as schema,
        'test' as name,
        'test' as alias,
        failures,
        message,
        adapter_response:query_id::string as query_id
    from test_raw
    
    union all

    select
        execution_id,
        execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        materialization,
        schema,
        name,
        alias,
        null as failures,
        message,
        adapter_response:query_id::string as query_id 
    from snapshot_raw     

    union all

    select
        execution_id,
        execution_type,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        materialization,
        schema,
        name,
        alias,
        null as failures,
        message,
        adapter_response:query_id::string as query_id 
    from seed_raw     
),

final as (
    select 
        all_invocations.*,
        invocation_raw.dbt_version,
        invocation_raw.project_name,
        invocation_raw.run_started_at as invocation_started_at,
        invocation_raw.dbt_command,
        invocation_raw.full_refresh_flag,
        invocation_raw.target_profile_name,
        invocation_raw.target_name,
        invocation_raw.target_schema,
        invocation_raw.target_threads,
        invocation_raw.dbt_cloud_project_id,
        invocation_raw.dbt_cloud_job_id,
        invocation_raw.dbt_cloud_run_id,
        invocation_raw.dbt_cloud_run_reason_category,
        invocation_raw.dbt_cloud_run_reason,
        invocation_raw.dbt_cloud_environment_name,
        invocation_raw.dbt_cloud_environment_type,
        invocation_raw.env_vars,
        invocation_raw.dbt_vars,
        invocation_raw.invocation_args,
        invocation_raw.dbt_custom_envs
    from all_invocations 
    left join invocation_raw
        on all_invocations.command_invocation_id = invocation_raw.command_invocation_id
)
 
select * from final
