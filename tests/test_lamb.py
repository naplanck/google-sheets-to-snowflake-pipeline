# -*- coding: utf-8 -*-
import base64
import copy
import io
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

import GS_Uploader as app


# =============================================================================
# Test helpers
# =============================================================================


def valid_transform_rules():
    return {
        "LEADS_RAW": {
            "DB": "PORTFOLIO_DB",
            "SCHEMA": "RAW",
            "T_COLS": [
                "LAUNCH_DATE",
                "SPEND",
                "CLICKS",
                "DATASOURCE",
                "DATASOURCE_SHEET",
                "LOAD_DATE",
            ],
            "SHEETS": [
                {
                    "LINK": "https://docs.google.com/spreadsheets/d/example",
                    "SHEET": "Campaign Tracker",
                    "HEADER_ROW": "2",
                    "COLS": {
                        "Launch Date": {"NAME": "LAUNCH_DATE", "TYPE": "DATE"},
                        "Spend": {"NAME": "SPEND", "TYPE": "FLOAT"},
                        "Clicks": {"NAME": "CLICKS", "TYPE": "INT"},
                    },
                }
            ],
        }
    }


class FakeBody:
    def __init__(self, text):
        self._text = text

    def read(self):
        return self._text.encode("utf-8")


class FakeS3Client:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error
        self.calls = []

    def get_object(self, Bucket, Key):
        self.calls.append({"Bucket": Bucket, "Key": Key})
        if self.error:
            raise self.error
        return {"Body": FakeBody(self.payload)}


class FakeSecretsClient:
    def __init__(self, responses=None, error=None):
        self.responses = responses or {}
        self.error = error
        self.calls = []

    def get_secret_value(self, SecretId):
        self.calls.append(SecretId)
        if self.error:
            raise self.error
        return self.responses[SecretId]


class FakeCursor:
    def __init__(self, fail_on_sql_start=None):
        self.statements = []
        self.closed = False
        self.fail_on_sql_start = fail_on_sql_start

    def execute(self, sql):
        self.statements.append(sql)
        if self.fail_on_sql_start and sql.startswith(self.fail_on_sql_start):
            raise RuntimeError(f"forced SQL failure: {sql}")
        return None

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class FakeWorksheet:
    def __init__(self, values=None, side_effects=None):
        self.values = values
        self.side_effects = list(side_effects or [])
        self.calls = 0

    def get(self):
        self.calls += 1
        if self.side_effects:
            next_result = self.side_effects.pop(0)
            if isinstance(next_result, Exception):
                raise next_result
            return next_result
        return self.values


class FakeSpreadsheet:
    def __init__(self, worksheet):
        self._worksheet = worksheet
        self.requested_sheets = []

    def worksheet(self, sheet_name):
        self.requested_sheets.append(sheet_name)
        return self._worksheet


class FakeGSheetClient:
    def __init__(self, worksheet):
        self._spreadsheet = FakeSpreadsheet(worksheet)
        self.opened_urls = []

    def open_by_url(self, url):
        self.opened_urls.append(url)
        return self._spreadsheet


class FakeAPIError(Exception):
    def __init__(self, status_code):
        super().__init__(f"API error {status_code}")
        self.response = SimpleNamespace(status_code=status_code)


# =============================================================================
# load_transform_rules_from_s3
# =============================================================================


def test_load_transform_rules_from_s3_reads_json_from_s3(monkeypatch):
    payload = '{"TABLE_ONE": {"DB": "DB", "SCHEMA": "RAW", "SHEETS": [], "T_COLS": []}}'
    fake_s3 = FakeS3Client(payload=payload)

    monkeypatch.setenv("TRANSFORM_RULES_BUCKET", "rules-bucket")
    monkeypatch.setattr(app.boto3, "client", lambda service_name: fake_s3)

    result = app.load_transform_rules_from_s3("rules.json")

    assert result["TABLE_ONE"]["DB"] == "DB"
    assert fake_s3.calls == [{"Bucket": "rules-bucket", "Key": "rules.json"}]



def test_load_transform_rules_from_s3_wraps_s3_errors(monkeypatch):
    fake_s3 = FakeS3Client(error=RuntimeError("s3 down"))

    monkeypatch.setenv("TRANSFORM_RULES_BUCKET", "rules-bucket")
    monkeypatch.setattr(app.boto3, "client", lambda service_name: fake_s3)

    with pytest.raises(RuntimeError, match="Failed to load transformation file from s3"):
        app.load_transform_rules_from_s3("rules.json")


# =============================================================================
# read_secret_json / get_secrets
# =============================================================================


def test_read_secret_json_reads_secret_string():
    sm = FakeSecretsClient(
        responses={
            "my-secret": {
                "SecretString": '{"User": "svc_user", "Pass": "secret"}'
            }
        }
    )

    result = app.read_secret_json(sm, "my-secret")

    assert result == {"User": "svc_user", "Pass": "secret"}
    assert sm.calls == ["my-secret"]



def test_read_secret_json_reads_secret_binary():
    encoded = base64.b64encode(b'{"key": "value"}')
    sm = FakeSecretsClient(
        responses={
            "binary-secret": {
                "SecretBinary": encoded
            }
        }
    )

    result = app.read_secret_json(sm, "binary-secret")

    assert result == {"key": "value"}



def test_read_secret_json_wraps_errors():
    sm = FakeSecretsClient(error=RuntimeError("secret manager unavailable"))

    with pytest.raises(RuntimeError, match="Failed to get Secret"):
        app.read_secret_json(sm, "missing-secret")



def test_get_secrets_fetches_google_and_snowflake_secrets(monkeypatch):
    fake_sm = FakeSecretsClient(
        responses={
            "gsheet-secret": {"SecretString": '{"client_email": "svc@example.com"}'},
            "sf-secret": {"SecretString": '{"User": "SNOWFLAKE_USER"}'},
        }
    )

    def fake_boto_client(service_name, region_name=None):
        assert service_name == "secretsmanager"
        assert region_name == "us-east-2"
        return fake_sm

    monkeypatch.setenv("AWS_REGION_VAR", "us-east-2")
    monkeypatch.setenv("GSHEET_SECRET_ID", "gsheet-secret")
    monkeypatch.setenv("SNOWFLAKE_SECRET_ID", "sf-secret")
    monkeypatch.setattr(app.boto3, "client", fake_boto_client)

    result = app.get_secrets()

    assert result == {
        "GSheet": {"client_email": "svc@example.com"},
        "SF": {"User": "SNOWFLAKE_USER"},
    }
    assert fake_sm.calls == ["gsheet-secret", "sf-secret"]


# =============================================================================
# connect_gsheet
# =============================================================================


def test_connect_gsheet_authorizes_with_service_account(monkeypatch):
    fake_creds = object()
    fake_client = object()
    from_service_account_info = Mock(return_value=fake_creds)
    authorize = Mock(return_value=fake_client)

    monkeypatch.setattr(
        app.GoogleCredentials,
        "from_service_account_info",
        from_service_account_info,
    )
    monkeypatch.setattr(app.gspread, "authorize", authorize)

    secret = {"client_email": "svc@example.com"}
    result = app.connect_gsheet(secret)

    assert result is fake_client
    from_service_account_info.assert_called_once()
    authorize.assert_called_once_with(fake_creds)

    _, kwargs = from_service_account_info.call_args
    assert kwargs["scopes"] == [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]


# =============================================================================
# get_sheet_values_with_retry
# =============================================================================


def test_get_sheet_values_with_retry_returns_values_on_first_try():
    worksheet = FakeWorksheet(values=[["Name"], ["Alice"]])

    result = app.get_sheet_values_with_retry(worksheet)

    assert result == [["Name"], ["Alice"]]
    assert worksheet.calls == 1



def test_get_sheet_values_with_retry_retries_transient_errors(monkeypatch):
    monkeypatch.setattr(app, "APIError", FakeAPIError)
    monkeypatch.setattr(app.time, "sleep", Mock())
    monkeypatch.setattr(app.random, "uniform", lambda low, high: 0)

    worksheet = FakeWorksheet(
        side_effects=[
            FakeAPIError(429),
            FakeAPIError(503),
            [["Name"], ["Alice"]],
        ]
    )

    result = app.get_sheet_values_with_retry(
        worksheet,
        max_attempts=3,
        base_sleep=1,
        max_sleep=30,
    )

    assert result == [["Name"], ["Alice"]]
    assert worksheet.calls == 3
    assert app.time.sleep.call_count == 2
    app.time.sleep.assert_any_call(1)
    app.time.sleep.assert_any_call(2)



def test_get_sheet_values_with_retry_does_not_retry_non_transient_api_errors(monkeypatch):
    monkeypatch.setattr(app, "APIError", FakeAPIError)
    monkeypatch.setattr(app.time, "sleep", Mock())

    worksheet = FakeWorksheet(side_effects=[FakeAPIError(400)])

    with pytest.raises(FakeAPIError):
        app.get_sheet_values_with_retry(worksheet, max_attempts=3)

    assert worksheet.calls == 1
    app.time.sleep.assert_not_called()



def test_get_sheet_values_with_retry_raises_after_max_attempts(monkeypatch):
    monkeypatch.setattr(app, "APIError", FakeAPIError)
    monkeypatch.setattr(app.time, "sleep", Mock())
    monkeypatch.setattr(app.random, "uniform", lambda low, high: 0)

    worksheet = FakeWorksheet(
        side_effects=[
            FakeAPIError(500),
            FakeAPIError(500),
            FakeAPIError(500),
        ]
    )

    with pytest.raises(FakeAPIError):
        app.get_sheet_values_with_retry(worksheet, max_attempts=3)

    assert worksheet.calls == 3
    assert app.time.sleep.call_count == 2



def test_get_sheet_values_with_retry_reraises_non_api_errors():
    worksheet = FakeWorksheet(side_effects=[RuntimeError("unexpected failure")])

    with pytest.raises(RuntimeError, match="unexpected failure"):
        app.get_sheet_values_with_retry(worksheet)


# =============================================================================
# sheet_values_to_dataframe
# =============================================================================


def test_sheet_values_to_dataframe_uses_configured_header_and_removes_empty_rows_and_blank_columns():
    values = [
        ["junk title row"],
        ["Name", "Amount", ""],
        ["Alice", "10", ""],
        ["", "", ""],
        ["Bob", "20", ""],
    ]

    df = app.sheet_values_to_dataframe(header=1, column=0, values=values)

    assert list(df.columns) == ["Name", "Amount"]
    assert len(df) == 2
    assert df.iloc[0]["Name"] == "Alice"
    assert df.iloc[1]["Amount"] == "20"



def test_sheet_values_to_dataframe_respects_column_start():
    values = [
        ["junk"],
        ["Ignore Me", "Name", "Amount"],
        ["x", "Alice", "10"],
    ]

    df = app.sheet_values_to_dataframe(header=1, column=1, values=values)

    assert list(df.columns) == ["Name", "Amount"]
    assert df.iloc[0]["Name"] == "Alice"
    assert df.iloc[0]["Amount"] == "10"



def test_sheet_values_to_dataframe_pads_short_rows_to_widest_row():
    values = [
        ["junk"],
        ["Name", "Amount", "Status"],
        ["Alice", "10"],
        ["Bob", "20", "Live"],
    ]

    df = app.sheet_values_to_dataframe(header=1, column=0, values=values)

    assert list(df.columns) == ["Name", "Amount", "Status"]
    assert df.iloc[0]["Status"] == ""
    assert df.iloc[1]["Status"] == "Live"



def test_sheet_values_to_dataframe_header_past_data_raises_error():
    values = [["Only one row"]]

    with pytest.raises(ValueError, match="No data found beyond header row"):
        app.sheet_values_to_dataframe(header=5, column=0, values=values)


# =============================================================================
# parse_date_value
# =============================================================================


def test_parse_date_value_valid_mdy_date():
    assert app.parse_date_value("01/15/2026") == date(2026, 1, 15)


@pytest.mark.parametrize("value", ["", "   ", pd.NA, None])
def test_parse_date_value_blank_or_na_returns_none(value):
    assert app.parse_date_value(value) is None



def test_parse_date_value_invalid_returns_none():
    assert app.parse_date_value("not a real date") is None


# =============================================================================
# parse_numeric_series
# =============================================================================


def test_parse_numeric_series_float_handles_currency_commas_percent_and_negatives():
    series = pd.Series([
        "$1,234.50",
        "(500)",
        "-75",
        "42-",
        "25%",
        "n/a",
        "TBD",
        "",
    ])

    result = app.parse_numeric_series(series, "FLOAT")

    assert str(result.dtype) == "Float64"
    assert result.iloc[0] == 1234.50
    assert result.iloc[1] == -500.0
    assert result.iloc[2] == -75.0
    assert result.iloc[3] == -42.0
    assert result.iloc[4] == 25.0
    assert pd.isna(result.iloc[5])
    assert pd.isna(result.iloc[6])
    assert pd.isna(result.iloc[7])



def test_parse_numeric_series_int_uses_nullable_integer_dtype():
    series = pd.Series(["1", "2", "", "(3)"])

    result = app.parse_numeric_series(series, "INT")

    assert str(result.dtype) == "Int64"
    assert result.tolist()[:2] == [1, 2]
    assert pd.isna(result.iloc[2])
    assert result.iloc[3] == -3



def test_parse_numeric_series_invalid_numbers_become_missing():
    result = app.parse_numeric_series(pd.Series(["abc", "$not-a-number"]), "FLOAT")

    assert pd.isna(result.iloc[0])
    assert pd.isna(result.iloc[1])



def test_parse_numeric_series_unsupported_type_raises_error():
    with pytest.raises(ValueError, match="Unsupported numeric type"):
        app.parse_numeric_series(pd.Series(["1"]), "BOOLEAN")


# =============================================================================
# align_df
# =============================================================================


def test_align_df_renames_filters_optional_columns_and_converts_types():
    df = pd.DataFrame({
        "Launch Date": ["01/15/2026"],
        "Spend": ["($1,234.50)"],
        "Clicks": ["100"],
        "Status": ["Live"],
        "Ignored Column": ["should not survive"],
    })

    rules = {
        "Launch Date": {"NAME": "LAUNCH_DATE", "TYPE": "DATE"},
        "Spend": {"NAME": "SPEND", "TYPE": "FLOAT"},
        "Clicks": {"NAME": "CLICKS", "TYPE": "INT"},
        "Notes": {"NAME": "NOTES", "TYPE": "VARCHAR", "REQUIRED": False},
        "Status": {"NAME": "STATUS", "TYPE": "VARCHAR"},
    }

    result = app.align_to_target_schema(df, rules)

    assert list(result.columns) == [
        "LAUNCH_DATE",
        "SPEND",
        "CLICKS",
        "NOTES",
        "STATUS",
    ]
    assert result.iloc[0]["LAUNCH_DATE"] == date(2026, 1, 15)
    assert result.iloc[0]["SPEND"] == -1234.50
    assert result.iloc[0]["CLICKS"] == 100
    assert result.iloc[0]["NOTES"] == ""
    assert result.iloc[0]["STATUS"] == "Live"



def test_align_df_missing_required_column_raises_error():
    df = pd.DataFrame({"Spend": ["100"]})
    rules = {
        "Launch Date": {"NAME": "LAUNCH_DATE", "TYPE": "DATE"},
        "Spend": {"NAME": "SPEND", "TYPE": "FLOAT"},
    }

    with pytest.raises(ValueError, match="Missing required source Columns"):
        app.align_to_target_schema(df, rules)



def test_align_df_duplicate_source_columns_raises_error():
    df = pd.DataFrame(
        [["01/15/2026", "01/16/2026"]],
        columns=["Launch Date", "Launch Date"],
    )
    rules = {"Launch Date": {"NAME": "LAUNCH_DATE", "TYPE": "DATE"}}

    with pytest.raises(ValueError, match="Duplicate columns found"):
        app.align_to_target_schema(df, rules)



def test_align_df_unsupported_column_type_raises_error():
    df = pd.DataFrame({"Active": ["true"]})
    rules = {"Active": {"NAME": "ACTIVE", "TYPE": "BOOLEAN"}}

    with pytest.raises(ValueError, match="Unsupported column type"):
        app.align_to_target_schema(df, rules)


# =============================================================================
# get_private_key_der / connect_sf
# =============================================================================


def test_get_private_key_der_requires_private_key():
    with pytest.raises(ValueError, match="Missing Private_Key"):
        app.get_private_key_der({"Private_Key_Passphrase": "password"})



def test_get_private_key_der_requires_passphrase():
    with pytest.raises(ValueError, match="Missing Private_Key_Passphrase"):
        app.get_private_key_der({"Private_Key": "fake-key"})



def test_get_private_key_der_returns_der_for_encrypted_pem():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    password = b"test-passphrase"
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(password),
    ).decode("utf-8")

    result = app.get_private_key_der({
        "Private_Key": encrypted_pem,
        "Private_Key_Passphrase": password.decode("utf-8"),
    })

    assert isinstance(result, bytes)
    assert len(result) > 0



def test_connect_sf_uses_key_pair_auth(monkeypatch):
    fake_connect = Mock(return_value="fake-connection")
    monkeypatch.setattr(app.connector, "connect", fake_connect)
    monkeypatch.setattr(app, "get_private_key_der", Mock(return_value=b"fake-der"))

    sec = {
        "User": "SERVICE_USER",
        "Acct": "abc123.us-east-2.aws",
        "WH": "COMPUTE_WH",
        "Private_Key": "fake",
        "Private_Key_Passphrase": "fake-pass",
    }
    rules = {"DB": "PORTFOLIO_DB", "SCHEMA": "RAW"}

    result = app.connect_sf(sec, rules)

    assert result == "fake-connection"
    fake_connect.assert_called_once_with(
        user="SERVICE_USER",
        private_key=b"fake-der",
        authenticator="SNOWFLAKE_JWT",
        account="abc123.us-east-2.aws",
        warehouse="COMPUTE_WH",
        database="PORTFOLIO_DB",
        schema="RAW",
        quote_identifiers=True,
    )


# =============================================================================
# validate_sql_identifier
# =============================================================================


def test_validate_sql_identifier_accepts_valid_identifier():
    assert app.validate_sql_identifier("VALID_NAME_123") == "VALID_NAME_123"


@pytest.mark.parametrize("identifier", ["1BAD", "bad-name", "bad name", "bad.name", "", "bad;DROP"])
def test_validate_sql_identifier_rejects_invalid_identifier(identifier):
    with pytest.raises(ValueError):
        app.validate_sql_identifier(identifier)



def test_validate_sql_identifier_rejects_non_string():
    with pytest.raises(TypeError):
        app.validate_sql_identifier(123)


# =============================================================================
# validate_transform_rules
# =============================================================================


def test_validate_transform_rules_accepts_valid_config():
    app.validate_transform_rules(valid_transform_rules())



def test_validate_transform_rules_rejects_non_dict_config():
    with pytest.raises(TypeError, match="Transformation rules must be a dictionary"):
        app.validate_transform_rules([])



def test_validate_transform_rules_rejects_invalid_table_name():
    rules = valid_transform_rules()
    rules["BAD-TABLE"] = rules.pop("LEADS_RAW")

    with pytest.raises(ValueError, match="Invalid target table"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_invalid_db_name():
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["DB"] = "BAD-DB"

    with pytest.raises(ValueError, match="Invalid LEADS_RAW.DB"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_invalid_schema_name():
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["SCHEMA"] = "BAD SCHEMA"

    with pytest.raises(ValueError, match="Invalid LEADS_RAW.SCHEMA"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_invalid_t_cols_name():
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["T_COLS"].append("BAD-COL")

    with pytest.raises(ValueError, match="Invalid LEADS_RAW.T_COLS column"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_invalid_mapped_target_column_name():
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["SHEETS"][0]["COLS"]["Spend"]["NAME"] = "BAD-COL"

    with pytest.raises(ValueError, match="Invalid LEADS_RAW.Spend.NAME"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_duplicate_target_columns():
    rules = valid_transform_rules()
    sheet_cols = rules["LEADS_RAW"]["SHEETS"][0]["COLS"]
    sheet_cols["Other Spend"] = {"NAME": "SPEND", "TYPE": "FLOAT"}

    with pytest.raises(ValueError, match="Duplicate target column names"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_mapped_column_missing_from_t_cols():
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["T_COLS"].remove("SPEND")

    with pytest.raises(ValueError, match="Mapped columns missing"):
        app.validate_transform_rules(rules)



def test_validate_transform_rules_rejects_empty_sheets():
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["SHEETS"] = []

    with pytest.raises(ValueError, match="No sheets configured"):
        app.validate_transform_rules(rules)


# =============================================================================
# lambda_handler
# =============================================================================


def test_lambda_handler_happy_path_replaces_target_table(monkeypatch):
    rules = valid_transform_rules()
    worksheet = FakeWorksheet(values=[
        ["junk title row"],
        ["Launch Date", "Spend", "Clicks"],
        ["01/15/2026", "$1,234.50", "100"],
        ["01/16/2026", "(500)", "25"],
    ])
    fake_gsheet_client = FakeGSheetClient(worksheet)
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection(fake_cursor)

    monkeypatch.setattr(app, "load_transform_rules_from_s3", Mock(return_value=copy.deepcopy(rules)))
    monkeypatch.setattr(app, "get_secrets", Mock(return_value={"GSheet": {"g": "s"}, "SF": {"s": "f"}}))
    monkeypatch.setattr(app, "connect_gsheet", Mock(return_value=fake_gsheet_client))
    monkeypatch.setattr(app, "connect_sf", Mock(return_value=fake_conn))
    monkeypatch.setattr(app.uuid, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))

    captured = {}

    def fake_write_pandas(conn, df, table_name, database, schema):
        captured["conn"] = conn
        captured["df"] = df.copy()
        captured["table_name"] = table_name
        captured["database"] = database
        captured["schema"] = schema
        return True, 1, len(df), []

    monkeypatch.setattr(app, "write_pandas", fake_write_pandas)

    result = app.lambda_handler({"File": "rules.json"}, context=None)

    assert result["statusCode"] == 200
    assert result["body"]["message"] == "Pipeline completed successfully"
    assert result["body"]["tables_loaded"] == ["LEADS_RAW"]
    app.load_transform_rules_from_s3.assert_called_once_with("rules.json")
    app.get_secrets.assert_called_once()
    app.connect_gsheet.assert_called_once_with({"g": "s"})
    app.connect_sf.assert_called_once_with({"s": "f"}, rules["LEADS_RAW"])

    assert fake_gsheet_client.opened_urls == ["https://docs.google.com/spreadsheets/d/example"]
    assert fake_gsheet_client._spreadsheet.requested_sheets == ["Campaign Tracker"]

    assert captured["conn"] is fake_conn
    assert captured["table_name"] == "LEADS_RAW_stage_abcdef12"
    assert captured["database"] == "PORTFOLIO_DB"
    assert captured["schema"] == "RAW"
    assert list(captured["df"].columns) == rules["LEADS_RAW"]["T_COLS"]
    assert len(captured["df"]) == 2
    assert captured["df"].iloc[0]["SPEND"] == 1234.50
    assert captured["df"].iloc[1]["SPEND"] == -500.0

    assert any(sql.startswith("CREATE TEMP TABLE") for sql in fake_cursor.statements)
    assert "BEGIN" in fake_cursor.statements
    assert any(sql.startswith("DELETE FROM") for sql in fake_cursor.statements)
    assert any(sql.startswith("INSERT INTO") for sql in fake_cursor.statements)
    assert "COMMIT" in fake_cursor.statements
    assert fake_cursor.closed is True
    assert fake_conn.closed is True



def test_lambda_handler_respects_skip_delete(monkeypatch):
    rules = valid_transform_rules()
    rules["LEADS_RAW"]["SKIP_DELETE"] = True

    worksheet = FakeWorksheet(values=[
        ["junk title row"],
        ["Launch Date", "Spend", "Clicks"],
        ["01/15/2026", "100", "10"],
    ])
    fake_gsheet_client = FakeGSheetClient(worksheet)
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection(fake_cursor)

    monkeypatch.setattr(app, "load_transform_rules_from_s3", Mock(return_value=copy.deepcopy(rules)))
    monkeypatch.setattr(app, "get_secrets", Mock(return_value={"GSheet": {}, "SF": {}}))
    monkeypatch.setattr(app, "connect_gsheet", Mock(return_value=fake_gsheet_client))
    monkeypatch.setattr(app, "connect_sf", Mock(return_value=fake_conn))
    monkeypatch.setattr(app.uuid, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))
    monkeypatch.setattr(app, "write_pandas", lambda conn, df, table_name, database, schema: (True, 1, len(df), []))

    app.lambda_handler({"File": "rules.json"}, context=None)

    assert not any(sql.startswith("DELETE FROM") for sql in fake_cursor.statements)
    assert any(sql.startswith("INSERT INTO") for sql in fake_cursor.statements)



def test_lambda_handler_wraps_invalid_transform_rules(monkeypatch):
    monkeypatch.setattr(app, "load_transform_rules_from_s3", Mock(return_value=[]))

    with pytest.raises(RuntimeError, match="Invalid transform rules config"):
        app.lambda_handler({"File": "rules.json"}, context=None)



def test_lambda_handler_raises_when_sheet_fetch_or_transform_fails(monkeypatch):
    rules = valid_transform_rules()
    worksheet = FakeWorksheet(values=[
        ["junk title row"],
        ["Wrong Column"],
        ["some value"],
    ])
    fake_gsheet_client = FakeGSheetClient(worksheet)

    monkeypatch.setattr(app, "load_transform_rules_from_s3", Mock(return_value=copy.deepcopy(rules)))
    monkeypatch.setattr(app, "get_secrets", Mock(return_value={"GSheet": {}, "SF": {}}))
    monkeypatch.setattr(app, "connect_gsheet", Mock(return_value=fake_gsheet_client))

    with pytest.raises(RuntimeError, match="LEADS_RAW"):
        app.lambda_handler({"File": "rules.json"}, context=None)



def test_lambda_handler_raises_when_write_pandas_row_count_mismatch(monkeypatch):
    rules = valid_transform_rules()
    worksheet = FakeWorksheet(values=[
        ["junk title row"],
        ["Launch Date", "Spend", "Clicks"],
        ["01/15/2026", "100", "10"],
    ])
    fake_gsheet_client = FakeGSheetClient(worksheet)
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection(fake_cursor)

    monkeypatch.setattr(app, "load_transform_rules_from_s3", Mock(return_value=copy.deepcopy(rules)))
    monkeypatch.setattr(app, "get_secrets", Mock(return_value={"GSheet": {}, "SF": {}}))
    monkeypatch.setattr(app, "connect_gsheet", Mock(return_value=fake_gsheet_client))
    monkeypatch.setattr(app, "connect_sf", Mock(return_value=fake_conn))
    monkeypatch.setattr(app.uuid, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))
    monkeypatch.setattr(app, "write_pandas", lambda conn, df, table_name, database, schema: (True, 1, 0, []))

    with pytest.raises(RuntimeError, match="Tables failed to load"):
        app.lambda_handler({"File": "rules.json"}, context=None)

    assert "BEGIN" not in fake_cursor.statements
    assert "ROLLBACK" not in fake_cursor.statements
    assert fake_cursor.closed is True
    assert fake_conn.closed is True



def test_lambda_handler_rolls_back_when_insert_fails_after_transaction_starts(monkeypatch):
    rules = valid_transform_rules()
    worksheet = FakeWorksheet(values=[
        ["junk title row"],
        ["Launch Date", "Spend", "Clicks"],
        ["01/15/2026", "100", "10"],
    ])
    fake_gsheet_client = FakeGSheetClient(worksheet)
    fake_cursor = FakeCursor(fail_on_sql_start="INSERT INTO")
    fake_conn = FakeConnection(fake_cursor)

    monkeypatch.setattr(app, "load_transform_rules_from_s3", Mock(return_value=copy.deepcopy(rules)))
    monkeypatch.setattr(app, "get_secrets", Mock(return_value={"GSheet": {}, "SF": {}}))
    monkeypatch.setattr(app, "connect_gsheet", Mock(return_value=fake_gsheet_client))
    monkeypatch.setattr(app, "connect_sf", Mock(return_value=fake_conn))
    monkeypatch.setattr(app.uuid, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))
    monkeypatch.setattr(app, "write_pandas", lambda conn, df, table_name, database, schema: (True, 1, len(df), []))

    with pytest.raises(RuntimeError, match="Tables failed to load"):
        app.lambda_handler({"File": "rules.json"}, context=None)

    assert "BEGIN" in fake_cursor.statements
    assert any(sql.startswith("INSERT INTO") for sql in fake_cursor.statements)
    assert "ROLLBACK" in fake_cursor.statements
    assert "COMMIT" not in fake_cursor.statements
    assert fake_cursor.closed is True
    assert fake_conn.closed is True
