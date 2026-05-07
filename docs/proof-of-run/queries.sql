-- Proof-of-run queries for google-sheets-to-snowflake-pipeline
-- Each section includes the recommended CSV filename for the query output.
-- Database/schema references assume the sample Snowflake layout used in the repo:
-- EXAMPLE.RAW, EXAMPLE.STG, and EXAMPLE.ANALYTICS.


-- ============================================================
-- 01_raw_layer_row_counts.csv
-- Purpose: Confirm that all four RAW tables were loaded.
-- ============================================================

SELECT
    'CAMPAIGN_TRACKER_RAW' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MIN(LOAD_DATE) AS MIN_LOAD_DATE,
    MAX(LOAD_DATE) AS MAX_LOAD_DATE
FROM EXAMPLE.RAW.CAMPAIGN_TRACKER_RAW

UNION ALL

SELECT
    'LEADS_RAW' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MIN(LOAD_DATE) AS MIN_LOAD_DATE,
    MAX(LOAD_DATE) AS MAX_LOAD_DATE
FROM EXAMPLE.RAW.LEADS_RAW

UNION ALL

SELECT
    'PAID_MEDIA_RAW' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MIN(LOAD_DATE) AS MIN_LOAD_DATE,
    MAX(LOAD_DATE) AS MAX_LOAD_DATE
FROM EXAMPLE.RAW.PAID_MEDIA_RAW

UNION ALL

SELECT
    'MONTHLY_BUDGET_RAW' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MIN(LOAD_DATE) AS MIN_LOAD_DATE,
    MAX(LOAD_DATE) AS MAX_LOAD_DATE
FROM EXAMPLE.RAW.MONTHLY_BUDGET_RAW
ORDER BY TABLE_NAME;


-- ============================================================
-- 02_source_sheet_lineage.csv
-- Purpose: Confirm that source workbook/tab lineage was preserved.
-- ============================================================

SELECT
    'CAMPAIGN_TRACKER_RAW' AS TABLE_NAME,
    DATASOURCE,
    DATASOURCE_SHEET,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.RAW.CAMPAIGN_TRACKER_RAW
GROUP BY DATASOURCE, DATASOURCE_SHEET

UNION ALL

SELECT
    'LEADS_RAW' AS TABLE_NAME,
    DATASOURCE,
    DATASOURCE_SHEET,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.RAW.LEADS_RAW
GROUP BY DATASOURCE, DATASOURCE_SHEET

UNION ALL

SELECT
    'PAID_MEDIA_RAW' AS TABLE_NAME,
    DATASOURCE,
    DATASOURCE_SHEET,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.RAW.PAID_MEDIA_RAW
GROUP BY DATASOURCE, DATASOURCE_SHEET

UNION ALL

SELECT
    'MONTHLY_BUDGET_RAW' AS TABLE_NAME,
    DATASOURCE,
    DATASOURCE_SHEET,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.RAW.MONTHLY_BUDGET_RAW
GROUP BY DATASOURCE, DATASOURCE_SHEET
ORDER BY TABLE_NAME, DATASOURCE_SHEET;


-- ============================================================
-- 03_raw_to_stg_row_count_comparison.csv
-- Purpose: Compare RAW row counts with STG row counts.
-- ============================================================

SELECT
    'CAMPAIGN_TRACKER' AS DATASET,
    (SELECT COUNT(*) FROM EXAMPLE.RAW.CAMPAIGN_TRACKER_RAW) AS RAW_ROWS,
    (SELECT COUNT(*) FROM EXAMPLE.STG.STG_CAMPAIGN_TRACKER) AS STG_ROWS

UNION ALL

SELECT
    'LEADS' AS DATASET,
    (SELECT COUNT(*) FROM EXAMPLE.RAW.LEADS_RAW) AS RAW_ROWS,
    (SELECT COUNT(*) FROM EXAMPLE.STG.STG_LEADS) AS STG_ROWS

UNION ALL

SELECT
    'PAID_MEDIA' AS DATASET,
    (SELECT COUNT(*) FROM EXAMPLE.RAW.PAID_MEDIA_RAW) AS RAW_ROWS,
    (SELECT COUNT(*) FROM EXAMPLE.STG.STG_PAID_MEDIA) AS STG_ROWS

UNION ALL

SELECT
    'MONTHLY_BUDGET_WIDE' AS DATASET,
    (SELECT COUNT(*) FROM EXAMPLE.RAW.MONTHLY_BUDGET_RAW) AS RAW_ROWS,
    (SELECT COUNT(*) FROM EXAMPLE.STG.STG_MONTHLY_BUDGET_WIDE) AS STG_ROWS

UNION ALL

SELECT
    'MONTHLY_BUDGET_UNPIVOTED' AS DATASET,
    (SELECT COUNT(*) FROM EXAMPLE.RAW.MONTHLY_BUDGET_RAW) AS RAW_ROWS,
    (SELECT COUNT(*) FROM EXAMPLE.STG.STG_MONTHLY_BUDGET) AS STG_ROWS;


-- ============================================================
-- 04_stg_paid_media_cleanup_sample.csv
-- Purpose: Show cleaned paid-media fields and calculated metrics.
-- ============================================================

SELECT
    "DATE",
    CLIENT,
    CAMPAIGN_NAME,
    PLATFORM,
    SPEND,
    IMPRESSIONS,
    CLICKS,
    CTR_REPORTED,
    CTR_CALCULATED,
    STATUS,
    DATASOURCE_SHEET
FROM EXAMPLE.STG.STG_PAID_MEDIA
ORDER BY "DATE", CLIENT, CAMPAIGN_NAME
LIMIT 25;


-- ============================================================
-- 05_stg_leads_aggregate.csv
-- Purpose: Show lead counts and revenue estimates after staging.
-- ============================================================

SELECT
    LEAD_DATE,
    CLIENT,
    SOURCE,
    SOURCE2,
    IS_QUALIFIED,
    COUNT(*) AS LEAD_COUNT,
    SUM(REVENUE_ESTIMATE) AS TOTAL_REVENUE_ESTIMATE,
    AVG(REVENUE_ESTIMATE) AS AVG_REVENUE_ESTIMATE
FROM EXAMPLE.STG.STG_LEADS
GROUP BY
    LEAD_DATE,
    CLIENT,
    SOURCE,
    SOURCE2,
    IS_QUALIFIED
ORDER BY LEAD_DATE, CLIENT, SOURCE
LIMIT 25;


-- ============================================================
-- 06_monthly_budget_unpivot_sample.csv
-- Purpose: Show monthly budget data transformed from wide to long format.
-- ============================================================

SELECT
    CLIENT,
    CAMPAIGN,
    FISCAL_YEAR,
    BUDGET_MONTH,
    MONTH_NAME,
    MONTH_NUM,
    MONTHLY_BUDGET,
    GROSS_BUDGET,
    DATASOURCE_SHEET
FROM EXAMPLE.STG.STG_MONTHLY_BUDGET
ORDER BY CLIENT, CAMPAIGN, BUDGET_MONTH
LIMIT 36;


-- ============================================================
-- 07_paid_media_quality_issues.csv
-- Purpose: Summarize paid-media data-quality issues found during staging.
-- ============================================================

SELECT
    ISSUE_REASON,
    COUNT(*) AS ISSUE_COUNT
FROM EXAMPLE.STG.STG_PAID_MEDIA_ISSUES
GROUP BY ISSUE_REASON
ORDER BY ISSUE_COUNT DESC;


-- ============================================================
-- 08_monthly_budget_quality_issues.csv
-- Purpose: Summarize monthly-budget data-quality issues found during staging.
-- ============================================================

SELECT
    ISSUE_REASON,
    COUNT(*) AS ISSUE_COUNT
FROM EXAMPLE.STG.STG_MONTHLY_BUDGET_ISSUES
GROUP BY ISSUE_REASON
ORDER BY ISSUE_COUNT DESC;


-- ============================================================
-- 09_analytics_object_row_counts.csv
-- Purpose: Confirm that analytics-layer objects are populated/queryable.
-- ============================================================

SELECT
    'DIM_CAMPAIGN_METADATA' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.DIM_CAMPAIGN_METADATA

UNION ALL

SELECT
    'FCT_PAID_MEDIA_DAILY' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.FCT_PAID_MEDIA_DAILY

UNION ALL

SELECT
    'FCT_LEADS_DAILY' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.FCT_LEADS_DAILY

UNION ALL

SELECT
    'FCT_MONTHLY_BUDGET' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.FCT_MONTHLY_BUDGET

UNION ALL

SELECT
    'MART_CLIENT_MONTHLY_FUNNEL' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.MART_CLIENT_MONTHLY_FUNNEL

UNION ALL

SELECT
    'MART_CLIENT_MONTHLY_SPEND_VS_BUDGET' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.MART_CLIENT_MONTHLY_SPEND_VS_BUDGET

UNION ALL

SELECT
    'UNMAPPED_PAID_MEDIA_CAMPAIGNS' AS OBJECT_NAME,
    COUNT(*) AS ROW_COUNT
FROM EXAMPLE.ANALYTICS.UNMAPPED_PAID_MEDIA_CAMPAIGNS
ORDER BY OBJECT_NAME;


-- ============================================================
-- 10_client_monthly_funnel_mart.csv
-- Purpose: Show the business-facing monthly funnel mart.
-- ============================================================

SELECT
    REPORT_MONTH,
    CLIENT,
    TOTAL_SPEND,
    TOTAL_IMPRESSIONS,
    TOTAL_CLICKS,
    TOTAL_LEADS,
    QUALIFIED_LEADS,
    ESTIMATED_REVENUE,
    CTR,
    CPC,
    CPM,
    COST_PER_LEAD,
    COST_PER_QUALIFIED_LEAD,
    ESTIMATED_ROAS,
    MATCH_STATUS
FROM EXAMPLE.ANALYTICS.MART_CLIENT_MONTHLY_FUNNEL
ORDER BY REPORT_MONTH, CLIENT
LIMIT 25;


-- ============================================================
-- 11_spend_vs_budget_mart.csv
-- Purpose: Show spend-vs-budget reporting output.
-- ============================================================

SELECT
    REPORT_MONTH,
    CLIENT,
    CAMPAIGN,
    CAMPAIGN_NAME,
    TOTAL_SPEND,
    MONTHLY_BUDGET,
    BUDGET_REMAINING,
    BUDGET_UTILIZATION_RATE,
    TOTAL_IMPRESSIONS,
    TOTAL_CLICKS,
    CTR,
    CPC,
    CPM,
    BUDGET_STATUS
FROM EXAMPLE.ANALYTICS.MART_CLIENT_MONTHLY_SPEND_VS_BUDGET
ORDER BY REPORT_MONTH, CLIENT, CAMPAIGN
LIMIT 25;


-- ============================================================
-- 12_campaign_mapping_sample.csv
-- Purpose: Show the campaign mapping table used by the analytics layer.
-- ============================================================

SELECT
    CLIENT,
    CAMPAIGN,
    CAMPAIGN_NAME,
    PLATFORM,
    IS_ACTIVE,
    CREATED_AT
FROM EXAMPLE.ANALYTICS.DIM_CAMPAIGN_MAP
ORDER BY CLIENT, CAMPAIGN, CAMPAIGN_NAME;

