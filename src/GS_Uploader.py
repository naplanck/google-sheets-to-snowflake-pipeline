# -*- coding: utf-8 -*-
# =============================================================================
# Imports and global configuration
# =============================================================================
from snowflake import connector
from snowflake.connector.pandas_tools import write_pandas
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials as GoogleCredentials
from datetime import datetime, timezone
import pandas as pd
import json
import boto3
import base64
import time
import dateparser
import re
import uuid
import logging
import random
import os
from cryptography.hazmat.primitives import serialization


logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALID_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}



# =============================================================================
# AWS configuration and secrets
# =============================================================================

def load_transform_rules_from_s3(file):
    logger.info("Starting S3 Client")
    s3 = boto3.client('s3')
    bucket = os.environ["TRANSFORM_RULES_BUCKET"]
    try:
        logger.info(f"Fetching from S3 Bucket {bucket}")
        response = s3.get_object(Bucket=bucket, Key=file)
        logger.info("Reading Transformation Rules")
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logger.exception(f"ERROR Getting Transform Rules - {e}")
        raise RuntimeError("Failed to load transformation file from s3") from e

def read_secret_json(sm, secret_id):
    try:
        resp = sm.get_secret_value(SecretId=secret_id)
        if "SecretString" in resp:
            sec = json.loads(resp["SecretString"])
        else: 
            sec = json.loads(base64.b64decode(resp["SecretBinary"]).decode("utf-8"))
        return sec
    except Exception as e:
        logger.exception(f"ERROR Getting Secrets - {e}")
        raise RuntimeError("Failed to get Secret") from e

def get_secrets():
    secrets = {}
    logger.info("Starting Boto3")
    sm = boto3.client("secretsmanager", region_name=os.environ['AWS_REGION_VAR'])
    # Get Gsheet Secrets
    logger.info("Fetching Secrets for Google Sheets")
    secrets['GSheet'] = read_secret_json(sm, os.environ['GSHEET_SECRET_ID'])
    
    #Get SF Secrets
    logger.info("Fetching Secrets for SnowFlake")
    secrets['SF'] = read_secret_json(sm, os.environ['SNOWFLAKE_SECRET_ID'])
    return secrets


# =============================================================================
# Transform rule validation
# =============================================================================

def validate_sql_identifier(identifier, label="SQL identifier"):
    if not isinstance(identifier, str):
        raise TypeError(f"{label} must be a string. Got {type(identifier).__name__}")

    if not VALID_SQL_IDENTIFIER.fullmatch(identifier):
        raise ValueError(
            f"Invalid {label}: {identifier!r}. "
            "Use only letters, numbers, and underscores, and do not start with a number."
        )

    return identifier

def validate_transform_rules(transform_rules):
    if not isinstance(transform_rules, dict):
        raise TypeError("Transformation rules must be a dictionary")

    for tbl, table_config in transform_rules.items():
        validate_sql_identifier(tbl, "target table")

        validate_sql_identifier(table_config["DB"], f"{tbl}.DB")
        validate_sql_identifier(table_config["SCHEMA"], f"{tbl}.SCHEMA")
        
        if not table_config["SHEETS"]:
            raise ValueError(f"ERROR - No sheets configured for table {tbl}")

        for col in table_config["T_COLS"]:
            validate_sql_identifier(col, f"{tbl}.T_COLS column")

        for sheet in table_config["SHEETS"]:
            for source_col, col_config in sheet["COLS"].items():
                validate_sql_identifier(
                    col_config["NAME"],
                    f"{tbl}.{source_col}.NAME"
                )
        
        for sheet in table_config["SHEETS"]:
            target_names = [
                col_config["NAME"]
                for col_config in sheet["COLS"].values()
            ]
        
            duplicate_targets = [
                col
                for col in set(target_names)
                if target_names.count(col) > 1
            ]
        
            if duplicate_targets:
                raise ValueError(
                    f"ERROR - Duplicate target column names in {tbl}: {duplicate_targets}"
                )
        
        target_cols = set(table_config["T_COLS"])
        for sheet in table_config["SHEETS"]:
            mapped_cols = {
                col_config["NAME"]
                for col_config in sheet["COLS"].values()
            }
        
            missing_from_t_cols = mapped_cols - target_cols
        
            if missing_from_t_cols:
                raise ValueError(f"Mapped columns missing from {tbl}.T_COLS: {sorted(missing_from_t_cols)}")


# =============================================================================
# Google Sheets extraction
# =============================================================================

def connect_gsheet(sec):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    logger.info("Setting Credentials for Google Sheets")
    creds = GoogleCredentials.from_service_account_info(sec, scopes=scope)
    logger.info("Auth for Google Sheets")
    return gspread.authorize(creds)

def get_sheet_values_with_retry(worksheet, max_attempts=3, base_sleep=5, max_sleep=30):
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("Fetching Google Sheet data")
            return worksheet.get()

        except APIError as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)

            if status_code not in TRANSIENT_STATUS_CODES:
                logger.exception(f"ERROR - Non-retryable Google Sheets API error. status_code={status_code}")
                raise

            if attempt == max_attempts:
                logger.exception(f"ERROR - Google Sheets API error after max retries. status_code={status_code} attempts={attempt}")
                raise

            sleep_seconds = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_seconds += random.uniform(0, 2)

            logger.warning(f"Transient Google Sheets API error. status_code={status_code} attempt={attempt}/{max_attempts}")
            logger.warning(f"Sleeping {sleep_seconds} seconds before retry")
            time.sleep(sleep_seconds)

        except Exception:
            logger.exception("ERROR while fetching Google Sheet data")
            raise


# =============================================================================
# DataFrame cleaning and schema alignment
# =============================================================================

def sheet_values_to_dataframe(header, column, values):
    logger.info("Cleaning up Data")
    # Check if header is greater than the rows we have
    if header >= len(values):
        logger.error("ERROR - no data in GSheet beyond headers (or header value is wrong)")
        raise ValueError("No data found beyond header row; check HEADER_ROW config")
    
    # Slice rows down to where the header is
    logger.info("Slicing Rows")
    sliced_rows = [
        row[column:] if len(row) > column else []
        for row in values[header:]
    ]
    
    # Check that there is still rows
    if not sliced_rows:
        logger.error("ERROR - no data in GSheet left beyond headers")
        raise ValueError("No rows found after applying HEADER_ROW/COLUMN_START")
    
    # Get the widest row (some might be missing values, e.g. a column is empty)
    logger.info("Getting Max Width and appending to Rows")
    max_width = max(len(row) for row in sliced_rows)
    # Append to the rows if it's length doesn't match the widest one
    headers = sliced_rows[0] + [""] * (max_width - len(sliced_rows[0]))
    data_rows = [
        row + [""] * (max_width - len(row))
        for row in sliced_rows[1:]
    ]
    
    # Convert to Dataframe
    logger.info("Converting to DataFrame")
    df = pd.DataFrame(data_rows, columns=headers)
    
    logger.info("Removing Empty Rows and Columns")
    # Remove empty rows
    df = df.loc[~df.eq("").all(axis=1)].reset_index(drop=True)
    # Remove empty columns
    df = df.loc[:, ~(df.eq("").all(axis=0) & pd.Series(df.columns).eq("").to_numpy())]
    
    return df

def parse_date_value(date):
    # If NA already - return None
    if pd.isna(date):
        return None
    
    # Strip and check
    value = str(date).strip()
    if value == "":
        return None
    
    # Use Date Parser to try and Parse dates
    settings = {
        "DATE_ORDER": "MDY",
        "STRICT_PARSING": False
        }
    parsed = dateparser.parse(value, settings=settings)
    if parsed is None:
        return None
    return parsed.date()

def parse_numeric_series(num, num_type):
    #common_number_replacements = ["$", ",","N/A","TBD","%", ""]
    number_placeholders = ["", "n/a", "tbd", "na"]
    number_format_chars = ["$", ",", "%"]
    cleaned = num.astype('string').str.strip()
    # Convert known non-number placeholders to missing values
    cleaned = cleaned.replace(number_placeholders, pd.NA)

    # Remove common formatting characters
    for char in number_format_chars:
        cleaned = cleaned.str.replace(re.escape(char), "", regex=True)
    
    negative_mask = (
        cleaned.str.contains(r"^\(.*\)$", regex=True, na=False)  # whole value in parentheses
        | cleaned.str.contains(r"^-", regex=True, na=False)       # starts with minus
        | cleaned.str.contains(r"-$", regex=True, na=False)       # ends with minus
    )
    cleaned = cleaned.str.replace(r"^\((.*)\)$", r"\1", regex=True)
    cleaned = cleaned.str.replace("-", "", regex=False)
    
    # Convert anything still invalid into NaN/<NA>
    numeric = pd.to_numeric(cleaned, errors="coerce")
    numeric = numeric.mask(negative_mask & numeric.notna(), -numeric.abs())

    if num_type in ("INT", "INTEGER"):
        return numeric.astype("Int64")

    if num_type in ("FLOAT", "DOUBLE", "NUMBER", "NUMERIC"):
        return numeric.astype("Float64")
    
    raise ValueError(f"Unsupported numeric type: {num_type}")

def align_to_target_schema(df, rules):
    logger.info("Aligning Data")
    # Check for duplicate columns
    logger.info("Checking for Duplicate Columns")
    duplicates = df.columns[df.columns.duplicated()].tolist()
    if duplicates:
        raise ValueError(f"Duplicate columns found: {duplicates}")
        
    logger.info("Checking for required Columns")
    source_cols = list(rules.keys())
    required_source_cols = [
        source_col
        for source_col, rule in rules.items()
        if rule.get("REQUIRED", True)
    ]
    
    logger.info("Checking for optional Columns")
    optional_source_cols = [
        source_col
        for source_col, config in rules.items()
        if config.get("REQUIRED", True) is False
    ]
    
    missing_required = [
        source_col
        for source_col in required_source_cols
        if source_col not in df.columns
    ]
    
    if missing_required:
        logger.error("Missing required Columns")
        raise ValueError(f"Missing required source Columns: {missing_required}")
    
    logger.info("Adding in optional Columns")
    for source_col in optional_source_cols:
        if source_col not in df.columns:
            df[source_col] = pd.NA

    # Log ignored columns for observability
    ignored_cols = [
        col for col in df.columns
        if col not in source_cols
    ]

    if ignored_cols:
        logger.warning(f"Ignored unmapped columns: {ignored_cols}")

    # Keep only configured source columns, in config order
    logger.info("Restricting Columns")
    df = df[source_cols]

    # Build the rename mapping
    rename_map = {
        old_col: name['NAME']
        for old_col, name in rules.items()
    }
    
    # Rename columns to align
    logger.info("Renaming Columns")
    df = df.rename(columns=rename_map)
    
    # Convert types
    logger.info("Converting Column Types")
    for old_col, config in rules.items():
        new_col = config["NAME"]
        col_type = config["TYPE"]

        logger.info(f"Converting {new_col} to {col_type}")    
        if col_type == "DATE":
            df[new_col] = df[new_col].apply(parse_date_value)
            df[new_col] = pd.to_datetime(df[new_col], errors="coerce").dt.date
        elif col_type in ("FLOAT", "DOUBLE", "NUMBER", "NUMERIC", "INT", "INTEGER"):
            df[new_col] = parse_numeric_series(df[new_col], col_type)
        elif col_type == "VARCHAR":
            df[new_col] = df[new_col].astype("string").fillna("")
        else:
            raise ValueError(f"ERROR - Unsupported column type for {new_col}: {col_type}")
    return df


# =============================================================================
# Snowflake connection and loading
# =============================================================================

def get_private_key_der(sec):
    private_key_text = sec.get("Private_Key")
    private_key_passphrase = sec.get("Private_Key_Passphrase")

    if not private_key_text:
        raise ValueError("Missing Private_Key in secret.")

    if not private_key_passphrase:
        raise ValueError("Missing Private_Key_Passphrase in secret.")

    private_key = serialization.load_pem_private_key(
        private_key_text.encode("utf-8"),
        password=private_key_passphrase.encode("utf-8"),
    )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return private_key_der

def connect_sf(sec, rules):
    USER = sec.get("User")
    ACCT = sec.get("Acct")
    WH = sec.get("WH")
    logger.info("Connecting to SF")
    conn = connector.connect(
        user=USER,
        #password=PASS,
        private_key=get_private_key_der(sec),
        authenticator="SNOWFLAKE_JWT",
        account=ACCT,
        warehouse=WH,
        database=rules['DB'],
        schema=rules['SCHEMA'],
        quote_identifiers=True
        )
    return conn


# =============================================================================
# Pipeline orchestration helpers
# =============================================================================

def build_sheet_dataframe(client, sheet_config):
    logger.info("Opening Google Sheet")
    worksheet = client.open_by_url(sheet_config["LINK"]).worksheet(sheet_config["SHEET"])

    values = get_sheet_values_with_retry(worksheet)

    header_row = int(sheet_config["HEADER_ROW"]) - 1
    column_start = int(sheet_config.get("COLUMN_START", 1)) - 1

    df = sheet_values_to_dataframe(header_row, column_start, values)
    df = align_to_target_schema(df, sheet_config["COLS"])

    df["DATASOURCE"] = sheet_config["LINK"]
    df["DATASOURCE_SHEET"] = sheet_config["SHEET"]
    df["LOAD_DATE"] = datetime.now(timezone.utc).date()
    df["LOAD_DATE"] = pd.to_datetime(df["LOAD_DATE"], errors="coerce").dt.date

    return df

def build_table_dataframe(client, table_config):
    dfs = [
        build_sheet_dataframe(client, sheet_config)
        for sheet_config in table_config["SHEETS"]
    ]

    logger.info("Creating combined table DataFrame")

    all_columns = []
    for df in dfs:
        for col in df.columns:
            if col not in all_columns:
                all_columns.append(col)

    aligned_dfs = [
        df.reindex(columns=all_columns, fill_value=pd.NA)
        for df in dfs
    ]

    main_df = pd.concat(aligned_dfs, ignore_index=True)
    main_df = main_df.reindex(columns=table_config["T_COLS"])

    return main_df

def load_table_via_staging(conn, df, table_name, table_config):
    cursor = None
    transaction_started = False

    try:
        cursor = conn.cursor()

        stage_name = f"{table_name}_stage_{uuid.uuid4().hex[:8]}"
        staging = f'"{table_config["DB"]}"."{table_config["SCHEMA"]}"."{stage_name}"'
        target = f'"{table_config["DB"]}"."{table_config["SCHEMA"]}"."{table_name}"'

        logger.info(f"Creating temp table {stage_name}")
        cursor.execute(f"CREATE TEMP TABLE {staging} LIKE {target}")

        logger.info(f"Writing DataFrame to temp table {stage_name}")
        success, num_chunks, num_rows, output = write_pandas(
            conn,
            df,
            table_name=stage_name,
            database=table_config["DB"],
            schema=table_config["SCHEMA"],
        )

        if not success or num_rows != len(df):
            raise RuntimeError(
                f"Snowflake write validation failed for {table_name}: "
                f"success={success}, num_rows={num_rows}, expected={len(df)}"
            )

        logger.info("Beginning Snowflake transaction")
        cursor.execute("BEGIN")
        transaction_started = True

        if table_config.get("SKIP_DELETE", False):
            logger.info(f"Skipping delete for table {table_name}")
        else:
            logger.info(f"Deleting from {table_name}")
            cursor.execute(f"DELETE FROM {target}")

        cols_order = ", ".join(f'"{col}"' for col in table_config["T_COLS"])

        logger.info(f"Inserting into {table_name} from {stage_name}")
        cursor.execute(
            f"INSERT INTO {target} ({cols_order}) "
            f"SELECT {cols_order} FROM {staging}"
        )

        logger.info("Committing Snowflake transaction")
        cursor.execute("COMMIT")
        transaction_started = False

    except Exception:
        logger.exception(f"ERROR - Failed to load table {table_name} to Snowflake")

        if cursor is not None and transaction_started:
            logger.warning("Attempting rollback")
            try:
                cursor.execute("ROLLBACK")
                logger.warning("Rollback complete")
            except Exception:
                logger.exception("ERROR - Rollback failed")

        raise

    finally:
        if cursor is not None:
            cursor.close()

def process_table(table_name, table_config, gsheet_client, snowflake_secret):
    logger.info(f"Processing table {table_name}")

    df = build_table_dataframe(gsheet_client, table_config)
    if df.empty:
        raise ValueError(f"Empty dataset for {table_name}")
    conn = None
    try:
        conn = connect_sf(snowflake_secret, table_config)
        load_table_via_staging(conn, df, table_name, table_config)
    finally:
        if conn is not None:
            conn.close()


# =============================================================================
# Lambda entrypoint
# =============================================================================

def lambda_handler(event, context):
    transform_rules = load_transform_rules_from_s3(event["File"])

    try:
        validate_transform_rules(transform_rules)
    except Exception as e:
        logger.exception(f"ERROR - Invalid transform rules config - {e}")
        raise RuntimeError("ERROR - Invalid transform rules config") from e

    secrets = get_secrets()
    gsheet_client = connect_gsheet(secrets["GSheet"])

    failed_tables = []

    for table_name, table_config in transform_rules.items():
        try:
            process_table(
                table_name=table_name,
                table_config=table_config,
                gsheet_client=gsheet_client,
                snowflake_secret=secrets["SF"],
            )
        except Exception as e:
            failed_tables.append(table_name)
            logger.exception(f"ERROR - Table failed to load: {table_name} - {e}")

    if failed_tables:
        raise RuntimeError(f"ERROR - Tables failed to load: {failed_tables}")

    return {
        "statusCode": 200,
        "body": {
            "message": "Pipeline completed successfully",
            "tables_loaded": list(transform_rules.keys()),
        },
    }


    
    
    
    
    
    
    
    
    
    
    
    