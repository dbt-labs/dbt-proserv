{# output a set of nodes by type #}
{# 
    This is used in dbt_artifacts snapshots
    to determine what nodes currently exist, 
    so we can invalidate hard deletes from source
#}

{% macro get_unique_nodes(type='model') %}
    {%- set unique_nodes=[] %}
    {% if execute %}
        {%- if type in ['model', 'test', 'snapshot', 'seed', 'models', 'tests', 'snapshots', 'seeds'] %}
            {%- set nodes = graph.nodes.values() | selectattr('resource_type', "equalto", type) %}
            {%- for node in nodes %}
                {%- do unique_nodes.append(node.unique_id) %}
            {%- endfor %}
        {%- elif type in ['sources', 'source'] %}
            {%- set nodes = graph.sources.values() %}
            {%- for node in nodes %}
                {%- do unique_nodes.append(node.unique_id) %}
            {%- endfor %}
        {%- elif type in ['exposures', 'exposure'] %}
            {%- set nodes = graph.exposures.values() %}
            {%- for node in nodes %}
                {%- do unique_nodes.append(node.unique_id) %}
            {%- endfor %}
        {%- else %}
            {{ exceptions.raise_compiler_error("Invalid type input: `" ~ type ~ "`. Type should be one of ['model', 'test', 'snapshot', 'seed', 'source', 'snapshot']") }}
        {%- endif %}

        {% if unique_nodes | length > 0 %}        
            -- return nodes for sql where clause
            (
            {%- for unique_node in unique_nodes %}
                '{{ unique_node }}' {%- if not loop.last %},{% endif -%}
            {%- endfor %}
            )
        {% else %}
            ('no_nodes_of_this_type')
        {% endif %}
    {% else %}
        (true)
    {% endif %}


{% endmacro %}