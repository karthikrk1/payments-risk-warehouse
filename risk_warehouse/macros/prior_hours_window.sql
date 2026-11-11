{#
    Window frame over an account's hourly activity covering the `hours` hours *before* the current
    hour, excluding the current hour itself. PaySim timestamps are hour precision and transactions
    within an hour have no reliable order, so same-hour activity is excluded: a feature never sees
    anything from its own hour or later.

    Integer RANGE offsets on paysim_step (one step = one hour). Supported by DuckDB and Snowflake;
    a portable fallback is a self-join on paysim_step between s - hours and s - 1.
#}
{% macro prior_hours_window(hours, partition_by='account_id', order_by='paysim_step') -%}
    partition by {{ partition_by }}
    order by {{ order_by }}
    range between {{ hours }} preceding and 1 preceding
{%- endmacro %}
