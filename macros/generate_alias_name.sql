{% macro generate_alias_name(custom_alias_name=None, node=None) -%}
    {% set dev_override_all = true %}
    {% set dev_nodes_to_override = {} %}

    {% if var("dev_nodes_to_override", "") != "" %}
        {% set dev_override_all = false %}
        {% for t in var('dev_nodes_to_override', '').split(',') %}
            {% do dev_nodes_to_override.update({t: 1}) %}
        {% endfor %}
    {% endif %}

    {% set table_name = (custom_alias_name | trim) if custom_alias_name is not none else node.name %}

    {% if target.name != "prod"
        and custom_alias_name is not none
        and node.unrendered_config.schema is not none
        and node.unrendered_config.schema | length
        and (dev_override_all or node.unique_id in dev_nodes_to_override)
    %}
        {% set schema_prefix = node.unrendered_config.schema | trim %}
        {{ schema_prefix ~ "__" ~ table_name }}
    {% else %}
        {{ table_name }}
    {% endif %}
{%- endmacro %}
