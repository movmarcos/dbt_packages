# dbt_package

A dbt project targeting dbt Core 1.10.15.

## Setup

1. Install dbt Core and your warehouse adapter, e.g.:
   ```
   pip install dbt-core==1.10.15 dbt-snowflake==1.10.*
   ```
2. Configure your profile in `~/.dbt/profiles.yml` under the key `dbt_package`.
3. Install packages:
   ```
   dbt deps
   ```
4. Verify:
   ```
   dbt debug
   ```

## Packages

- `dbt-labs/dbt_utils`
- `calogica/dbt_expectations`
- `calogica/dbt_date`
