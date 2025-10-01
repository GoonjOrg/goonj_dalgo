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

## dbt Project Components

### 🏗️ **Generate Schema Name Macro**

The project includes a custom `generate_schema_name` macro (`macros/generate_schema_name.sql`) that automatically creates schema names based on:

**Logic:**
- Detects the folder structure in your model path (staging, intermediate, prod)
- Combines target environment (dev/prod) with layer and source system
- Handles special cases like elementary package models

**Schema Creation Pattern:**
```
{target}_{layer}_{source_system}
```

**Examples:**
- `models/staging/salesforce/staging_kit.sql` → `dev_staging_salesforce` (local) / `prod_staging_salesforce` (production)
- `models/intermediate/salesforce/int_kit.sql` → `dev_intermediate_salesforce` (local) / `prod_intermediate_salesforce` (production)
- `models/prod/salesforce/kit_summary.sql` → `dev` (local) / `prod` (production)

### 📋 **Sources Configuration (sources.yml)**

The `models/sources.yml` file defines external data sources that staging models reference. This file maps dbt source names to actual database tables.

**Structure:**
```yaml
sources:
  - name: staging_salesforce           # Source system name
    schema: staging_salesforce         # Database schema name
    tables:
      - name: kit                      # dbt reference name
        identifier: Kit__c             # Actual table name in database
        description: <table_description>
```

**When to Update sources.yml:**
1. **New data source**: Add a new source when Dalgo pushes data from a new system
2. **New table**: Add table definitions when new tables are available in existing sources
3. **Table changes**: Update `identifier` if actual table names change in the database

**How to Reference Sources in Models:**
```sql
FROM {{ source('staging_salesforce', 'kit') }}
-- This resolves to: staging_salesforce.Kit__c
```

**Key Requirements:**
- `name`: Must match the schema where Dalgo pushes raw data
- `identifier`: Must exactly match the table name in the database (including Salesforce `__c` suffixes)
- `description`: Document what each table contains

## dbt Commands Reference

### 🚀 **Core Development Commands**

**`dbt run`**
- Executes all models in dependency order
- Creates/updates tables and views in your database
- Use `dbt run --select model_name` to run specific models

**`dbt test`**
- Runs data quality tests defined in schema.yml files
- Validates relationships, uniqueness, null checks, etc.
- Essential for ensuring data integrity

**`dbt build`**
- Combines `dbt run` and `dbt test` in one command
- Runs models and tests together, stopping on failures
- Recommended for comprehensive validation

### 📚 **Documentation Commands**

**`dbt docs generate`**
- Creates interactive documentation from your project
- Generates lineage graphs showing data flow
- Includes model descriptions, column details, and relationships

**`dbt docs serve`**
- Serves documentation locally at http://localhost:8080
- Run after `dbt docs generate` to view documentation

### 🧹 **Project Management Commands**

**`dbt deps`**
- Installs packages defined in `packages.yml`
- Downloads dependencies like dbt_utils, dbt_expectations
- Run this after cloning the project or updating packages

**`dbt clean`**
- Removes `target/` and `dbt_packages/` directories
- Cleans up generated files and cached data
- Useful for fresh starts or troubleshooting

**`dbt compile`**
- Compiles models to raw SQL without executing
- Useful for debugging Jinja logic and checking generated SQL
- Output appears in `target/compiled/` directory

### 🎯 **Selective Execution**

**Model Selection:**
```bash
dbt run --select staging_kit                    # Run specific model
dbt run --select staging.salesforce            # Run all models in folder
dbt run --select +staging_kit                  # Run model and upstream dependencies
dbt run --select staging_kit+                  # Run model and downstream dependencies
```

**Tag-based Selection:**
```bash
dbt run --select tag:salesforce                # Run models with specific tag
dbt test --select tag:staging                  # Test models with staging tag
```

### 🔧 **Development Workflow**

**Typical Development Cycle:**
1. `dbt deps` - Install dependencies (first time setup)
2. `dbt run --select your_model` - Test your changes
3. `dbt test --select your_model` - Validate data quality
4. `dbt build --select your_model+` - Run model and downstream tests
5. `dbt docs generate && dbt docs serve` - Update and view documentation

**Before Committing:**
```bash
dbt build                    # Run all models and tests
dbt docs generate           # Update documentation
```

### 🚨 **Troubleshooting Commands**

**`dbt debug`**
- Validates your dbt installation and profiles
- Checks database connections
- First command to run when having issues

**`dbt ls`**
- Lists all models, tests, and sources in your project
- Useful for understanding project structure
- Use `dbt ls --select` to preview what would run

### Using the Project

**Getting Started:**
```bash
dbt deps                    # Install package dependencies
dbt debug                   # Verify setup
dbt run                     # Build all models
dbt test                    # Run all tests
dbt docs generate           # Generate documentation
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
