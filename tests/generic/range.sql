{% set column_name = var('column_name') %}
{% set min_val = var('min') %}
{% set max_val = var('max') %}

select *
from {{ model }}
where {{ column_name }} < {{ min_val }}
   or {{ column_name }} > {{ max_val }};