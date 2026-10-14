# risk_warehouse (dbt project)

The dbt project for the payments risk warehouse. See the [top-level README](../README.md) for the
project overview and [`CLAUDE.md`](../CLAUDE.md) for modelling conventions.

```bash
cd risk_warehouse
dbt deps     # install dbt_utils and dbt_expectations
dbt build    # run and test all models
```

The profile in `profiles.yml` writes to `../warehouse/risk.duckdb`, so run dbt from this directory.

| Folder | Prefix | Purpose |
|---|---|---|
| `models/staging/` | `stg_` | 1:1 with raw sources: rename, cast, clean. No joins. |
| `models/intermediate/` | `int_` | Joins, window features, business logic. |
| `models/marts/core/` | `dim_`, `fct_` | Star schema. |
| `models/marts/ml/` | `ml_` | Point-in-time-correct feature tables. |
