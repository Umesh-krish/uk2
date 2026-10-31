{#
  Deterministic surrogate key, hand-rolled so the project carries no package
  dependency. Nulls are coalesced to a sentinel so two rows differing only in a
  null do not collide into the same key.
#}

{% macro surrogate_key(columns) %}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '_null_')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{% endmacro %}
