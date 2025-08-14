{% macro compare_aggregates(
    source_table=None,
    target_table=None,
    group_columns=None,
    measure_columns=None,
    date_column=None,
    start_date=None,
    end_date=None,
    column_mapping=None,
    config_file=None
) %}

{# set up directories and config #}
{% set project_root = var('project_root', env_var('DBT_PROJECT_DIR')) %}
{% set sql_queries_dir = './sql_queries/' %}
{% set config_file_name = sql_queries_dir ~ "compare_config.json" %}

{# reading params from schema file #}
{% set params_from_yml = {
    "source_table": source_table,
    "target_table": target_table,
    "group_columns": group_columns,
    "measure_columns": measure_columns,
    "date_column": date_column,
    "start_date": start_date,
    "end_date": end_date,
    "column_mapping": column_mapping,
    "config_file": config_file
} %}

{% set all_params_none = params_from_yml.values() | select("equalto", None) | list | length == params_from_yml | length %}
