# goonj_dalgo
Dalgo project repository for Goonj

DBT project with macros, CI/CD Templates, and folder structure commonly used in Dalgo's DBT Projects.

## Models Folder Structure

This project follows a three-layer data architecture:

### 📥 **Staging** (`models/staging/`)
- **Purpose**: Read and perform minimal basic cleanup of raw data from staging schemas that Dalgo has pushed data into
- **Data Source**: Raw data from source systems (e.g., `staging_salesforce` for Salesforce data)
- **Transformations**: Basic cleaning, column renaming, data type casting, simple filtering
- **Example**: `staging/salesforce/staging_kit.sql` reads from `staging_salesforce.Kit__c` table

### 🔄 **Intermediate** (`models/intermediate/`)
- **Purpose**: Deep data cleanup, aggregations, disaggregations, and business logic transformations
- **Data Source**: Staging models
- **Transformations**: Complex business rules, data enrichment, calculated fields, aggregations
- **Example**: `intermediate/salesforce/int_kit_base.sql` performs advanced transformations on staging kit data

### 🎯 **Prod** (`models/prod/`)
- **Purpose**: Final models used for visualization and reporting with joins, final cleanups, and business-ready datasets
- **Data Source**: Intermediate models (and sometimes staging models for simple cases)
- **Transformations**: Final joins across entities, presentation-layer formatting, KPIs and metrics
- **Output**: Business-ready tables and views for dashboards and analytics

## Schema Naming Convention

The project uses a custom `generate_schema_name` macro that creates schemas based on:
1. **Target environment** (`dev` for local development, `prod` for production)
2. **Model layer** (staging, intermediate, prod)
3. **Data source** (subfolder name like `salesforce`)

### Schema Pattern:
- **Local Development**: `dev_{layer}_{source}` (e.g., `dev_staging_salesforce`, `dev_intermediate_salesforce`)
- **Production**: `prod_{layer}_{source}` (e.g., `prod_staging_salesforce`, `prod_intermediate_salesforce`)

### Examples:
| Model Path | Local Schema | Production Schema |
|------------|--------------|-------------------|
| `staging/salesforce/staging_kit.sql` | `dev_staging_salesforce` | `prod_staging_salesforce` |
| `intermediate/salesforce/int_kit_base.sql` | `dev_intermediate_salesforce` | `prod_intermediate_salesforce` |
| `prod/salesforce/kit_summary.sql` | `dev_salesforce` | `prod_salesforce` |

## Target Configuration

- **Local Development**: Use `dev` target - creates models in `dev_*` schemas for isolated development
- **Dalgo Production**: Uses `prod` target - creates models in `prod_*` schemas for production data

This ensures development work doesn't interfere with production data and allows multiple developers to work independently.

## Sources Configuration

The `sources.yml` file defines external data sources that staging models reference. To make staging models work:

1. **Add source definitions** for each external table:
```yaml
sources:
  - name: staging_salesforce
    schema: staging_salesforce
    tables:
      - name: your_table_name
        identifier: Actual_Table_Name__c  # Real table name in database
        description: <table_description>
```

2. **Reference sources in staging models** using:
```sql
FROM {{ source('staging_salesforce', 'your_table_name') }}
```

3. **Required fields**:
   - `name`: Source system name (matches schema where Dalgo pushes data)
   - `schema`: Database schema containing the raw tables
   - `identifier`: Exact table name in the database (often includes Salesforce `__c` suffix)

### Using the project

Try running the following commands:
- dbt run
- dbt test

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
