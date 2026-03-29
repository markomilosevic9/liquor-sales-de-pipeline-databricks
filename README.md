# Liquor sales DE pipeline
<a href="#"><p align="left">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/databricks-dark.svg" width="50">

## About project
Project features ELT pipeline ingesting ~2.5M rows from API through medallion layers. 
- Auto Loader handles ingestion into Bronze layer.
- Silver layer features a Kimball star schema, DLT streaming table for data quality, manual MERGE for SCD Type 2 dimensions.
- Gold layer contains data marts built as DLT materialized views. 

Databricks Free Edition is used (serverless compute, Unity Catalog, DLT, Workflows) and the project is deployed as code with DABs with complementary GitHub Actions for CI/CD.

## Architecture

The high-level architecture diagram is shown below:
<img width="7075" height="1776" alt="picture_1" src="https://github.com/user-attachments/assets/00a99c29-6976-43a4-a64e-eeab698f7c27" />

Tree diagrams of project structure and Unity Catalog are provided below:
<details>
<summary>Tree diagram of project structure</summary>
<pre>
├── .github/workflows/
│   ├── validate_on_pr.yml                    # PR check - DAB validation
│   └── deploy_on_merge.yml                   # Auto-deploy - on merge to main 
│
├── resources/
│   ├── jobs.yml                              # Workflow DAG (8 tasks)
│   └── pipelines.yml                         # 3 DLT pipelines (Bronze, Silver, Gold)
│
├── notebooks/
│   ├── _config.ipynb                         # Shared env config
│   ├── 01_ingest_api_to_volume.ipynb         # Data ingestion from API to Volume (Python)
│   ├── 02_bronze_auto_loader.ipynb           # Auto Loader writing to Bronze layer (Python)
│   ├── 03_dim_dates.ipynb                    # Static 2025 calendar generation (SQL)
│   ├── 04_silver_cleaned_sales.ipynb         # Type casting, DQ expectations (SQL)
│   ├── 05_silver_dims_and_facts.ipynb        # MERGE logic for dimensions, fact table loading (Python)
│   ├── 06_gold_sales_trends.ipynb            # Sales trends and seasonality (SQL)
│   ├── 07_gold_geographic.ipynb              # Geographic analysis (SQL)
│   ├── 08_gold_vendor_category.ipynb         # Vendor and category performance (SQL)
│   ├── 09_gold_store_performance.ipynb       # Store performance and pipeline health (SQL)
│   ├── 10_delta_maintenance.ipynb            # OPTIMIZE, VACUUM... (SQL)
│   └── 11_quality_check.ipynb                # Pipeline health (Python)
│
└── databricks.yml                            # Bundle config
</pre>
</details>

<details>
<summary>Tree diagram of Unity Catalog structure</summary>
<pre>
dev (catalog)
├── bronze
│   ├── raw_liquor_sales            DLT streaming table (Auto Loader)
│   ├── ingestion_metadata          API/watermark tracking table
│   └── landing_zone (Volume)       Raw .json files
├── silver
│   ├── cleaned_sales               DLT streaming table (quality-checked)
│   ├── dim_stores                  SCD Type 2 
│   ├── dim_products                SCD Type 2 
│   ├── dim_vendors                 SCD Type 2 
│   ├── dim_dates                   Static calendar (plain Delta table)
│   └── fct_sales                   Fact table
└── gold
    ├── gold_monthly_sales          DLT materialized view
    ├── gold_dow_patterns           DLT materialized view
    ├── gold_seasonal_categories    DLT materialized view
    ├── gold_county_performance     DLT materialized view
    ├── gold_city_rankings          DLT materialized view
    ├── gold_vendor_scorecard       DLT materialized view
    ├── gold_category_market_share  DLT materialized view
    ├── gold_store_rankings         DLT materialized view
    └── pipeline_health             DLT materialized view
</pre>
</details>

## Data source
Dataset contains liquor transaction in the state of Iowa, published via the official API. This project fetches all transactions that occured during 2025. 

The pipeline supports incremental processing via `last_loaded_date` stored in `bronze.ingestion_metadata` table, ensuring that subsequent runs fetch only new data from the API, if the defined time span changes.

The data flow is illustrated in the diagram below:

<img width="5576" height="1978" alt="picture_2" src="https://github.com/user-attachments/assets/df658f3b-dbf0-4e33-b725-e61a2dc3b97e" />


## Bronze layer 
The ingestion notebook (`01_ingest_api_to_volume`) fetches .json files from the API and writes files to a Unity Catalog Volume.
A DLT pipeline (`02_bronze_auto_loader`) uses Auto Loader (`cloudFiles` format) to stream these .json files into a single append-only DLT streaming table. 
The schema is explicitly provided for all 23 source columns (all as STRING, since type casting happens in Silver layer). 
Every source column is preserved with no transformations. Also, the Bronze table includes metadata/technical columns: `_ingestion_ts` (when Auto Loader processed the row), `_source_file` (which .json file it came from).

## Silver layer
The Silver layer runs in two stages: a DLT pipeline for data cleaning, followed by a regular notebook that handles dimensional modeling.

### Stage 1
A `cleaned_sales` DLT streaming table reads from Bronze layer and applies type casting (strings to INT, DECIMAL, DATE), text standardization (UPPER/TRIM), and data quality expectations via DLT's EXPECT syntax. 
Rows that violate constraints are dropped and pass/fail rates are logged in the DLT event log.

### Stage 2
A standalone notebook (`05_silver_dims_and_facts`) runs as a regular Workflow task and handles:

- DDL - creates dimension and fact tables. Each dimension has a surrogate key (`BIGINT GENERATED ALWAYS AS IDENTITY`, supporting SCD 2 implementation)
- Staging - extracts the most recent attribute combination per business key from `cleaned_sales` into temp views.
- SCD 2 - for each dimension, compares staged attributes against current records. When tracked columns differ, existing records are expired and new versions are inserted; while new business keys are inserted as new records.

The ERD of the implemented dimensional model is shown below:

![picture_5](https://github.com/user-attachments/assets/4d17b9a1-8f7b-4afd-90d9-ca95b00f68ec)


## Gold layer
Gold consists of DLT materialized views that fully recompute on each DLT pipeline refresh. These views cover sales trends (monthly revenue, day-of-week patterns, seasonal category performance), city and county-level geographic analysis, category market share and vendor scorecards, sales performance of stores. Moreover, a view summarizing pipeline health is included.

### Data quality checks
The final Workflow task (`11_quality_check`) runs DQ checks as SQL queries whose results indicate whether the pipeline run is valid. These checks cover:
- Cross-layer checks - compares row counts (e.g. between `cleaned_sales` and `fct_sales`) and verify that Gold monthly revenue matches the Silver fact table total)
- FK and SCD 2 integrity - checks for orphan FKs across dimensions, validates that no SCD 2 record has `valid_from >= valid_to`.
- Business rules - chekcs consistent fact table measure-related calculations, no duplicate invoice line numbers, no sales dated in future.

## Delta Lake maintenance
The maintenance notebook (`10_delta_maintenance`) demonstrates few Delta Lake operations for table versioning, query performance optimization, change tracking and file cleanup.

## Workflow
An 8-task Databricks Workflow consisting of DLT pipeline tasks and regular notebook tasks is implemented.

Workflow run overview (Databricks UI):
<img width="800" alt="picture_4" src="https://github.com/user-attachments/assets/4444175b-efdf-46a0-89a6-766e0b8c0613" />


| Task               | Type         | Purpose                                     |
|--------------------|--------------|---------------------------------------------|
| ingest_api         | Notebook     | API to .json files in Volume                |
| bronze_pipeline    | DLT pipeline | Auto Loader to `raw_liquor_sales`           |
| dim_dates          | Notebook     | Static 2025 calendar                        |
| silver_pipeline    | DLT pipeline | `cleaned_sales` streaming table             |
| dims_and_facts     | Notebook     | SCD2 MERGE + fact load                      |
| gold_pipeline      | DLT pipeline | Materialized views (Gold layer)             |
| delta_maintenance  | Notebook     | OPTIMIZE, Z-ORDER, VACUUM                   |
| quality_check      | Notebook     | Row counts, FK integrity, Gold validation   |

Tasks view (Databricks UI):
<img width="800" alt="picture_3" src="https://github.com/user-attachments/assets/c46180be-39ce-43f8-bcf3-645289e0d247" />


Executed task list (Databricks UI):
<img width="800" alt="picture_6" src="https://github.com/user-attachments/assets/9ab8503c-6ab9-4efa-bca9-41653672b25d" />


## Deployment - DABs
The entire infrastructure is defined as code (via `databricks.yml`, `jobs.yml` and `pipelines.yml`). A single command creates all 3 DLT pipelines, the Workflow with all task dependencies, and wires them together. The `${var.env}` variable controls which Unity Catalog catalog all tables land in. Deploying to a different environment is a single flag:

```bash
databricks bundle deploy --target dev    # dev.bronze, dev.silver, dev.gold
databricks bundle deploy --target qa     # qa.bronze, qa.silver, qa.gold
databricks bundle deploy --target prod   # prod.bronze, prod.silver, prod.gold
```

### Environment parameterization 
Two mechanisms handle catalog references across notebook types: 
- DLT notebooks (02, 04, 06–09) use `${source_catalog}`; a pipeline configuration variable set in file `pipelines.yml` that DLT substitutes at runtime.
- Regular notebooks (03, 05, 10, 11) use `%run ./_config` which creates a Databricks widget from the task parameter.

Both resolve to the same catalog (dev, qa, or prod).

## GitHub Actions
Two workflows automate validation and deployment:
- Validate DAB on PR (`validate_on_pr.yml`) - runs `databricks bundle validate --target dev` on every pull request targeting main. Catches broken notebook references, invalid YAML and schema issues before merge.
- Deploy on merge (`deploy_on_merge.yml`) - runs `databricks bundle deploy --target dev` when code is merged to main. Automatically updates all pipelines and jobs in the workspace.

Validation is triggered by an opened PR, deployment by merge to main. After that, the pipeline can be run via the CLI or the Databricks UI.
