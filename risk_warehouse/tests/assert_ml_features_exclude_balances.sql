-- No balance column, and no balance-mismatch flag, may appear in the ML feature table. The
-- after-balances are post-transaction, and the dataset's author says none of the four balance
-- columns should be used for fraud detection. Checks the built table's real columns, so a column
-- added later under any of these names fails the build. Returns one row per forbidden column.

-- The ref below sits inside `if execute`, which dbt doesn't see at parse time. This line declares
-- the dependency so the test is attached to the model and runs after it:
-- depends_on: {{ ref('ml_transaction_features') }}

{% set forbidden_patterns = ['balance', 'mismatch'] %}

{% if execute %}
    {% set columns = adapter.get_columns_in_relation(ref('ml_transaction_features')) %}
    {% set forbidden = [] %}
    {% for column in columns %}
        {% for pattern in forbidden_patterns %}
            {% if pattern in column.name | lower and column.name not in forbidden %}
                {% do forbidden.append(column.name) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
{% else %}
    {% set forbidden = [] %}
{% endif %}

{% if forbidden %}
    {% for name in forbidden %}
select '{{ name }}' as forbidden_column
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% else %}
select cast(null as {{ dbt.type_string() }}) as forbidden_column
where 1 = 0
{% endif %}
