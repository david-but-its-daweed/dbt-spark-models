{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none -%}
        {%- set table_name = node.name -%}
    {%- else -%}
        {%- set table_name = custom_alias_name | trim -%}
    {%- endif -%}

    {% if target.name != 'prod' and custom_alias_name is not none  %}
        {%- set schema_prefix = node.unrendered_config.schema | trim %}

        {{ schema_prefix ~ "__" ~ table_name }}

    {%- else -%}
        {{ table_name }}
    {%- endif -%}
{%- endmacro %}