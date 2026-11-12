"""Triage agent: diagnoses why a dbt build failed.

Read-only: it reads dbt's run results and manifest, samples the failing rows, looks at what
changed in git, and asks Claude for a structured diagnosis (category, cause, evidence, suggested
fix, confirming check). It never edits files or writes to the warehouse. See triage/README.md.
"""
