{%- macro spark__create_table_as(temporary, relation, compiled_code, language='sql') -%}

  {% set tables_to_copy_from_prod = {} %}
  {% if var('tables_to_copy_from_prod', '') != '' %}
    {% for t in var('tables_to_copy_from_prod', '').split(',') %}
      {% do tables_to_copy_from_prod.update({t.split(':')[0]: t.split(':')[1]}) %}
    {% endfor %}
  {% endif %}

  {% set relation_name = relation.render() %}
  {% set is_delta = config.get('file_format', validator=validation.any[basestring]) == 'delta' %}

  {%- if language == 'sql' -%}
    {%- if temporary -%}
      {{ create_temporary_view(relation, compiled_code) }}
    {%- elif relation_name in tables_to_copy_from_prod and not is_delta -%}
      create or replace view {{ relation }}
      as
      select * from {{ tables_to_copy_from_prod[relation_name] }}
    {%- else -%}
      {% if is_delta %}
        create or replace table {{ relation }}
      {% else %}
        create table {{ relation }}
      {% endif %}
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause() }}
      {{ comment_clause() }}
      {{ tblproperties_clause()}}
      as
      {% if relation_name in tables_to_copy_from_prod %}
        select * from {{ tables_to_copy_from_prod[relation_name] }}
      {% else %}
        {{ compiled_code }}
      {% endif %}
    {%- endif -%}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
{%- endmacro -%}
