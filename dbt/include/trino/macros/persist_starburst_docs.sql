{% macro persist_starburst_docs(relation, model, for_relation=true, for_columns=true) %}
  {% if target.starburst_url %}
    {% set persist = config.get('persist_docs', {}) %}
    {% set do_relation = for_relation and persist.get('relation', false) %}
    {% set do_columns = for_columns and persist.get('columns', false) %}
    {% if do_relation or do_columns %}
      {% do adapter.persist_starburst_docs(relation, model, do_relation, do_columns) %}
    {% endif %}
  {% endif %}
{% endmacro %}
