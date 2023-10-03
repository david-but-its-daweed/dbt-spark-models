{% macro spark__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', validator=validation.any[basestring]) in ['delta', 'hudi'] %}
    {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        alter table {{ relation }} change column
            {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
            comment '{{ escaped_comment }}';
      {% endset %}

      {% call statement("spark__alter_column_comment", fetch_result=false, auto_begin=false) %}
        {{ comment_query }}
      {% endcall %}
    {% endfor %}
  {% endif %}
{% endmacro %}