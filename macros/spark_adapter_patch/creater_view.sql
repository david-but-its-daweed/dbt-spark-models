{% macro spark__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
  {{ comment_clause() }}
  {{ tblproperties_clause() }}

  as
    {{ sql }}
{% endmacro %}