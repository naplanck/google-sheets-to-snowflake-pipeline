# google-sheets-to-snowflake-pipeline
A production-style data ingestion pipeline that extracts messy, manually maintained Google Sheets data, validates and normalizes it through JSON-based schema rules, and loads it into Snowflake using a transactional staging-table pattern.

This project is designed around a common real-world data engineering problem: business teams often maintain operational data in spreadsheets, but analytics platforms require stable schemas, clean typing, repeatable loads, and clear lineage.

---
## Project Overview

This pipeline ingests data from one or more Google Sheets tabs into Snowflake target tables.

The core design principle is that the JSON configuration acts as a **schema contract** between unstable spreadsheet inputs and stable Snowflake tables.

The pipeline:

- Reads source tabs from Google Sheets using a Google service account
- Supports configurable header rows and starting columns for messy sheet layouts
- Removes blank rows and empty unnamed columns
- Fails on duplicate source columns
- Fails on missing required columns
- Supports optional columns by filling missing values with nulls (or empty strings)
- Ignores and logs extra spreadsheet columns not included in the config
- Renames source columns to Snowflake-compatible target names
- Converts dates, strings, integers, and numeric values
- Handles common spreadsheet number formats such as `$1,200`, `15%`, `(450)`, `450-`, `N/A`, and `TBD`
- Adds lineage metadata such as source workbook and source tab
- Loads data into Snowflake through a temporary staging table
- Uses explicit target-column inserts instead of `SELECT *`
- Logs pipeline progress and failures to CloudWatch when deployed on AWS Lambda

---

## Why This Project Exists

Many data pipelines are demonstrated using clean API data or static CSV files. In real business environments, however, important operational data often lives in spreadsheets that are manually edited by non-technical users.

That creates problems such as:

- Inconsistent header rows
- Extra notes or unused columns
- Blank rows and columns
- Mixed date formats
- Currency and percent formatting stored as text
- Missing required fields
- Unexpected new columns added by business users
- Static Snowflake tables that cannot safely absorb arbitrary spreadsheet drift

This project addresses those issues by treating the pipeline configuration as the authoritative contract for what should be loaded.

---

---

## Tech Stack

| Area | Tools |
|---|---|
| Language | Python |
| Data processing | pandas, numpy |
| Source system | Google Sheets |
| Google authentication | Google service account, gspread, google-auth |
| Cloud runtime | AWS Lambda container image |
| Container registry | Amazon ECR |
| Config storage | Amazon S3 |
| Secret storage | AWS Secrets Manager |
| Logging | Python logging, CloudWatch Logs |
| Data warehouse | Snowflake |
| Snowflake loading | snowflake-connector-python, write_pandas |
| Scheduling | Amazon EventBridge |

---

## Repository Structure

```text
.
├── README.md
├── Dockerfile
├── requirements.txt
├── pytest.ini
├── .gitignore
├── docs/
│   ├── Arch_Diagram.png
│   └── proof-of-run/
│       ├── proof-of-run.md
│       ├── lambda-test-success.png
│       ├── cloudwatch-log-events.csv
│       ├── queries.sql
│       └── query-results/
│           ├── 01_raw_layer_row_counts.csv
│           ├── 02_source_sheet_lineage.csv
│           ├── 03_raw_to_stg_row_count_comparison.csv
│           ├── 04_stg_paid_media_cleanup_sample.csv
│           ├── 05_stg_leads_aggregate.csv
│           ├── 06_monthly_budget_unpivot_sample.csv
│           ├── 07_paid_media_quality_issues.csv
│           ├── 08_monthly_budget_quality_issues.csv
│           ├── 09_analytics_object_row_counts.csv
│           ├── 10_client_monthly_funnel_mart.csv
│           ├── 11_spend_vs_budget_mart.csv
│           └── 12_campaign_mapping_sample.csv
├── examples/
│   ├── transform_rules.json
│   └── example_messy_google_sheets_inputs_expanded.xlsx
├── sql/
│   ├── Table_Setup.sql
│   ├── Staging_Level.sql
│   └── Analytics_Level.sql
├── src/
│   └── GS_Uploader.py
└── tests/
    └── test_lamb.py
```

---

## Configuration Design

The pipeline is driven by a JSON config file stored in S3.

Each top-level object represents a Snowflake target table. Each table can ingest from one or more Google Sheets tabs.

Example:

```json
{
 "LEADS_RAW":{
		"DB":"EXAMPLE",
		"SCHEMA":"RAW",
		"SKIP_DELETE": false,
		"T_COLS":["LEAD_DATE","CLIENT","FIRST_NAME","LAST_NAME","EMAIL","PHONE","SOURCE","SOURCE2","QUALIFIED","REVENUE_ESTIMATE","ADDRESS", "DATASOURCE", "DATASOURCE_SHEET", "LOAD_DATE"],
		"SHEETS":[
			{
				"LINK": "https://docs.google.com/spreadsheets/d/1vqaH-AAwUbBmAphs8XFjeUScpvbNHbGckpItfMQ_UyE/edit?gid=478926885#gid=478926885",
				"SHEET": "Leads Raw",
				"HEADER_ROW": "5",
                "COLUMN_START": "1",
				"COLS": {
					"Lead Date": {"NAME":"LEAD_DATE", "TYPE": "DATE", "REQUIRED": true},
					"Client": {"NAME":"CLIENT", "TYPE": "VARCHAR", "REQUIRED": true},
					"First Name": {"NAME":"FIRST_NAME", "TYPE": "VARCHAR", "REQUIRED": true},
					"Last Name": {"NAME":"LAST_NAME", "TYPE": "VARCHAR", "REQUIRED": true},
					"Email": {"NAME":"EMAIL", "TYPE": "VARCHAR", "REQUIRED": true},
					"Phone": {"NAME":"PHONE", "TYPE": "VARCHAR", "REQUIRED": true},
					"Source": {"NAME":"SOURCE", "TYPE": "VARCHAR", "REQUIRED": true},
					"Source2": {"NAME":"SOURCE2", "TYPE": "VARCHAR", "REQUIRED": true},
					"Qualified?": {"NAME":"QUALIFIED", "TYPE": "VARCHAR", "REQUIRED": true},
					"Revenue Estimate": {"NAME":"REVENUE_ESTIMATE", "TYPE": "FLOAT", "REQUIRED": true},
					"Address": {"NAME":"ADDRESS", "TYPE": "VARCHAR", "REQUIRED": false}
				}
			}
		]
	}
}
```

### Important Config Fields

| Field | Purpose |
|---|---|
| `DB` | Snowflake database |
| `SCHEMA` | Snowflake schema |
| `SKIP_DELETE` | If true, skips deleting existing target records before insert |
| `T_COLS` | Ordered list of Snowflake target columns |
| `SHEETS` | List of source Google Sheets tabs feeding the target table |
| `LINK` | Google Sheet URL |
| `SHEET` | Worksheet/tab name |
| `HEADER_ROW` | 1-based row number where headers begin |
| `COLUMN_START` | Optional starting column offset |
| `COLS` | Source-to-target column mapping and type rules |
| `REQUIRED` | If true or omitted, missing source columns fail the load |

---

## Schema Contract Rules

The pipeline intentionally handles spreadsheet drift in a strict but practical way.

| Source condition | Pipeline behavior |
|---|---|
| Extra unmapped columns | Ignored and logged |
| Duplicate source columns | Load fails |
| Missing required columns | Load fails |
| Missing optional columns | Column is added with null values |
| Configured columns | Renamed, type-converted, and loaded |
| Final output columns | Ordered using `T_COLS` |

This protects Snowflake from accidental schema drift while still allowing source spreadsheets to contain extra notes, unused fields, or temporary manual columns.

---

## Data Cleaning and Type Handling

### Header and row cleanup

The extraction layer supports spreadsheets where the actual data table does not start in cell A1.

It can:

- Start from a configured header row
- Start from a configured column offset
- Pad uneven rows
- Remove fully blank rows
- Remove fully blank unnamed columns

### Date parsing

Date fields are parsed using flexible date parsing and converted into date-compatible values before loading to Snowflake.

Examples of accepted source values may include:

```text
1/15/2026
2026-01-15
Jan 15 2026
1-15-26
```

Invalid or blank date values are converted to null.

### Numeric parsing

Numeric fields are normalized from common spreadsheet formats.

Examples:

| Source value | Parsed value |
|---|---:|
| `$1,250.50` | `1250.50` |
| `15%` | `15` |
| `(450)` | `-450` |
| `450-` | `-450` |
| `N/A` | null |
| `TBD` | null |

---

## Snowflake Loading Pattern

The pipeline uses a staging-table pattern for safer loads.

For each configured target table:

1. Create a temporary staging table using the target table structure
2. Load the cleaned DataFrame to staging with `write_pandas()`
3. Validate the row count returned by Snowflake
4. Optionally delete existing target records
5. Insert configured target columns from staging into the target table
6. Commit the transaction
7. Roll back if an error occurs

The final insert uses explicit target columns from `T_COLS` instead of relying on `SELECT *`.

Example pattern:

```sql
INSERT INTO target_table (COL_A, COL_B, COL_C)
SELECT COL_A, COL_B, COL_C
FROM staging_table;
```

---

## Environment Variables

The Docker image contains code and dependencies only. Deployment-specific values are provided through Lambda environment variables.

| Variable | Purpose |
|---|---|
| `TRANSFORM_RULES_BUCKET` | S3 bucket containing the transform rules JSON |
| `GSHEET_SECRET_ID` | Secrets Manager ID for Google service account credentials |
| `SNOWFLAKE_SECRET_ID` | Secrets Manager ID for Snowflake credentials |
| `AWS_REGION_VAR` | AWS region, for example `us-east-1` |

Example:

```text
TRANSFORM_RULES_BUCKET=your-config-bucket
GSHEET_SECRET_ID=portfolio/google-sheets/service-account
SNOWFLAKE_SECRET_ID=portfolio/snowflake/etl-user
AWS_REGION_VAR=us-east-1
```

---

## Secrets

### Google Sheets secret

The Google Sheets secret should contain the full Google service account JSON.

The service account must also be shared directly on the source Google Sheet with at least Viewer access.

### Snowflake secret

The Snowflake secret should contain the connection details used by the Snowflake Python connector.

Example structure:

```json
{
  "User": "ETL_USER",
  "Private_Key": "example-private-key",
  "Private_Key_Passphrase": "example-private-key-passphrase",
  "Acct": "your-org-your-account",
  "WH": "COMPUTE_WH"
}
```

---

## IAM Permissions

The Lambda execution role needs permission to read the transform-rules file from S3 and retrieve the required secrets from Secrets Manager.

Example policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadTransformRulesFromS3",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::YOUR_CONFIG_BUCKET/*"
    },
    {
      "Sid": "ReadPipelineSecrets",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": [
        "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:YOUR_GSHEET_SECRET-*",
        "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:YOUR_SNOWFLAKE_SECRET-*"
      ]
    }
  ]
}
```

---

## Docker Deployment

This project is designed to run as an AWS Lambda container image.

### Dockerfile

```dockerfile
FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/GS_Uploader.py ${LAMBDA_TASK_ROOT}/

CMD ["GS_Uploader.lambda_handler"]
```

### Build image

```bash
docker build --no-cache -t gsheet_loader .
```

### Tag image for ECR

```bash
docker tag gsheet_loader:latest ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/gsheet_loader:latest
```

### Push image to ECR

```bash
docker push ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/gsheet_loader:latest
```

### Update Lambda function

```bash
aws lambda update-function-code \
  --function-name gsheet_loader \
  --image-uri ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/gsheet_loader:latest
```

---

## Lambda Invocation Event

The Lambda event specifies which transform-rules file to load from S3.

Example:

```json
{
  "File": "transform_rules.json"
}
```

This allows the same deployed function to run different ingestion configurations without rebuilding the image.

---

## Scheduling

The pipeline can be scheduled with Amazon EventBridge.

Example schedule:

```text
Run once per day at 6:00 AM UTC
```

The EventBridge target should invoke the Lambda function with a payload such as:

```json
{
  "File": "transform_rules.json"
}
```

---

## Example Source Workbook

The included [example workbook](https://docs.google.com/spreadsheets/d/1vqaH-AAwUbBmAphs8XFjeUScpvbNHbGckpItfMQ_UyE/edit?gid=1312154947#gid=1312154947) intentionally contains messy, business-user-style spreadsheet data: inconsistent campaign names, missing values, formatting issues, blank rows, and partial cross-sheet mismatches.

This is intentional. The primary goal is to demonstrate ingestion, schema normalization, validation, and warehouse loading patterns for imperfect operational data.

Because the sample data includes campaign-name mismatches, some downstream analytics views may return sparse or partially unmatched results. In a production setting, these mismatches would be handled through stricter naming governance, mapping tables, data quality alerts, and alignment with business users.

Because the sample workbook intentionally includes inconsistent campaign names, the analytics layer includes `EXAMPLE.ANALYTICS.UNMAPPED_PAID_MEDIA_CAMPAIGNS` to surface paid media campaigns that do not yet have a governed mapping in `DIM_CAMPAIGN_MAP`.

The linked Google Sheet is a public, view-only sample workbook containing synthetic data for demonstration purposes.

The workbook includes examples such as:

- Multiple tabs
- Inconsistent header rows
- Blank rows
- Blank columns
- Currency values stored as text
- Percent values stored as text
- Parenthetical negative values
- Missing optional fields
- Extra unmapped columns
- Notes and exception tabs

This file is designed to simulate the kind of manual spreadsheet inputs a data engineer may need to operationalize.

---

## Testing Strategy

The test suite covers:

| Test area | What to validate |
|---|---|
| `sheet_values_to_dataframe()` | Header offsets, column offsets, blank row removal, uneven row padding |
| `align_to_target_schema()` | Duplicate column failure, missing required column failure, optional column filling |
| `parse_date_value()` | Valid dates, blank values, invalid dates |
| `parse_numeric_series()` | Currency, commas, percents, placeholder values, negative formats |
| Snowflake load prep | Final DataFrame columns match `T_COLS` |

Example test cases:

```python
def test_parse_numeric_series_invalid_numbers_become_missing():
    result = app.parse_numeric_series(pd.Series(["abc", "$not-a-number"]), "FLOAT")

    assert pd.isna(result.iloc[0])
    assert pd.isna(result.iloc[1])

def test_parse_numeric_series_unsupported_type_raises_error():
    with pytest.raises(ValueError, match="Unsupported numeric type"):
        app.parse_numeric_series(pd.Series(["1"]), "BOOLEAN")
```

---

## Design Decisions

### Why use a config-driven approach?

Manual spreadsheets change frequently. Hard-coding every source column and table mapping into Python would make the pipeline brittle and difficult to extend.

The JSON config allows new source tabs and target tables to be added by updating configuration rather than rewriting pipeline logic.

### Why ignore extra spreadsheet columns?

Business users might add temporary columns, notes, or calculations to spreadsheets. Loading every source column could make the Snowflake target schema unstable.

The pipeline only loads columns explicitly defined in the config.

### Why fail on missing required columns?

Missing required fields usually indicate that the source sheet contract has changed. Failing loudly prevents silent bad loads.

### Why use a temporary Snowflake staging table?

The staging-table pattern allows the pipeline to validate the incoming DataFrame before replacing target data. If the load fails, the transaction can be rolled back before the target table is partially updated.

### Why add lineage columns?

Manual spreadsheet pipelines need traceability. `DATASOURCE` and `DATASOURCE_SHEET` make it easier to identify where a problematic record originated.

---

## Warehouse Modeling

The Snowflake layer follows a simple medallion-style pattern:

- `RAW`: one-to-one loaded copies of the normalized Google Sheets tabs.
- `STG`: cleaned and standardized views with consistent naming, date parsing, and business-key assumptions.
- `ANALYTICS`: reporting-ready views that combine campaign metadata, paid media spend, leads, and budget targets.

### Modeling Assumptions

- `CAMPAIGN_NAME` is treated as the campaign business key and is expected to be unique by naming convention.
- `NOTES` is retained in RAW for operational review but excluded from STG and ANALYTICS because it is unstructured.
- RAW tables are full-refreshed from source Google Sheets.
- STG views do not mutate source data; they standardize naming and types for downstream analytics.
- ANALYTICS views are intended for lightweight reporting and portfolio demonstration, not a fully governed semantic layer.

---

## Portfolio Highlights

This project demonstrates practical data engineering skills, including:

- Cloud-based pipeline deployment
- Containerized Lambda development
- AWS IAM permissions and runtime configuration
- Secrets Manager integration
- Google service-account authentication
- Messy spreadsheet ingestion
- Config-driven transformation logic
- Data validation and schema enforcement
- Snowflake staging-table loading
- Transaction handling and rollback behavior
- Operational logging through CloudWatch

---

## Running Tests

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
pytest
```

