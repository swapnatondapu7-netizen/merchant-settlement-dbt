{% test dbt_utils_positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} <= 0
{% endtest %}

{% test dbt_utils_unique_combination(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
