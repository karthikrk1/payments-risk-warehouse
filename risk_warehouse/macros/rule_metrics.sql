{#
    Score a yes/no rule against the fraud label: how many transactions it flags, how many of those
    are fraud (precision), what share of all fraud it catches (recall), and lift (precision divided
    by the overall fraud rate). Used by the analyses in analyses/ so every rule is scored the same way.

    rule_name:  label for the output row
    condition:  SQL boolean expression over `relation`
    relation:   a ref() or source() with an is_fraud column
    signal:     what kind of signal the rule uses (real, planted, leak, baseline)
#}
{% macro rule_metrics(rule_name, condition, relation, signal) -%}
    select
        '{{ rule_name }}' as rule_name,
        '{{ signal }}' as signal,
        sum(case when {{ condition }} then 1 else 0 end) as flagged_count,
        sum(case when ({{ condition }}) and is_fraud then 1 else 0 end) as fraud_caught_count,
        round(
            100.0 * sum(case when ({{ condition }}) and is_fraud then 1 else 0 end)
            / nullif(sum(case when {{ condition }} then 1 else 0 end), 0),
            2
        ) as precision_pct,
        round(
            100.0 * sum(case when ({{ condition }}) and is_fraud then 1 else 0 end)
            / nullif(sum(case when is_fraud then 1 else 0 end), 0),
            2
        ) as recall_pct,
        round(
            (
                1.0 * sum(case when ({{ condition }}) and is_fraud then 1 else 0 end)
                / nullif(sum(case when {{ condition }} then 1 else 0 end), 0)
            ) / nullif(1.0 * sum(case when is_fraud then 1 else 0 end) / count(*), 0),
            1
        ) as lift
    from {{ relation }}
{%- endmacro %}
