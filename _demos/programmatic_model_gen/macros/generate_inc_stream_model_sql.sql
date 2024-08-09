{% macro generate_inc_stream_model_sql(database_name, schema_name, table_pattern='%', exclude='') %}

{% set table_list = dbt_utils.get_relations_by_pattern(
    schema_pattern=schema_name,
    database=database_name,
    table_pattern=table_pattern,
    exclude=exclude
) %}

{%- set objects = [] %}
{%- for table in table_list %}
    {%- set column_response = adapter.get_columns_in_relation(adapter.get_relation(
        database=table.database,
        schema=table.schema,
        identifier=table.identifier
    )) -%}
    {%- set column_names = column_response | map(attribute='name') %}

{%- set model_sql %}
{% raw %}{{- config({%- endraw %}
        materialized="incremental_stream",
        unique_key=["id"]
    )
    select 
        {%- for column in column_names %}
        {{ column | lower }},
        {%- endfor %}
        {% raw %}{{ incr_stream.get_stream_metadata_columns() }}{%- endraw %}
    from incr_stream.stream_source({{ table.schema | lower }}, {{ table.identifier | lower }})
{% raw %}-}}{% endraw %}
{%- endset %}
    
    {%- do objects.append({
        "name": "stg_" ~ table.schema | lower ~ "__" ~ table.identifier | lower ~ ".sql",
        "sql": model_sql
    }) %}
{%- endfor %}

{% if execute %}{{ print(objects) }}{% endif %}

{% endmacro %}