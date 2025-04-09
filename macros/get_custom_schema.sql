{% macro generate_schema_name(custom_schema_name, node) -%}
    {% set dev_override_all = true %}
    {% set dev_nodes_to_override = {} %}

    {% if var("dev_nodes_to_override", "") != "" %}
        {% set dev_override_all = false %}
        {% for t in var('dev_nodes_to_override', '').split(',') %}
            {% do dev_nodes_to_override.update({t: 1}) %}
        {% endfor %}
    {% endif %}

    {% if custom_schema_name is none  and var('dbt_default_production_schema', '') != '' %}
        {% set custom_schema_name = var('dbt_default_production_schema', '') %}
    {% endif %}

    {% if dev_override_all or node.unique_id in dev_nodes_to_override  or custom_schema_name is none %}
        {{ generate_schema_name_for_env(custom_schema_name, node) }}
    {% else %}
        {{ custom_schema_name | trim }}
    {% endif %}
{%- endmacro %}