"""
Named Account OD Dashboard — data pipeline agent.

Runs the master query against BigQuery and writes docs/data/od_dashboard.json
so the GitHub Pages frontend can render it without any server.

Auth: expects BIGQUERY_CREDENTIALS env var containing the full contents of a
GCP service-account JSON key file (set this as a GitHub Actions secret).
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

# ── BigQuery query ────────────────────────────────────────────────────────────

QUERY = """
WITH

excluded_personal AS (
  SELECT domain FROM UNNEST([
    'gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com',
    'protonmail.com','me.com','live.com','msn.com','googlemail.com',
    'pm.me','fastmail.com','proton.me','aol.com','mail.com',
    'gmx.com','yandex.com','tutanota.com','zoho.com','rocketmail.com',
    'att.net','sbcglobal.net','yahoo.co.uk','yahoo.co.in'
  ]) AS domain
),

seller_accounts AS (
  SELECT
    da.account_id,
    da.account_name,
    da.account_pod,
    da.account_last_activity_date,
    da.account_owner_user_id,
    da.account_owner_name,
    domain
  FROM `lambda-data-warehouse.general.dim_salesforce_accounts` da
  CROSS JOIN UNNEST(da.account_email_domain_array) AS domain
  WHERE da.account_owner_user_id IN (
    '005Pe00000Bc5MvIAJ',
    '0055a0000087gHHAAY',
    '0055a000009jhEYAAY',
    '005Pe000002Vy5BIAS',
    '005Pe00000CcSkLIAV',
    '005Pe00000Bkb9VIAR',
    '0055a00000AGSrOAAX',
    '005Pe00000CYA9JIAX'
  )
    AND da.is_internal_account = FALSE
    AND da.account_name NOT IN (
      'Lambda Internal Account [for testing]',
      'On Demand Catcher'
    )
    AND da.account_email_domain_array IS NOT NULL
    AND ARRAY_LENGTH(da.account_email_domain_array) > 0
),

acct_totals AS (
  SELECT
    REGEXP_EXTRACT(LOWER(c.chargebee_customer_email), r'@(.+)$') AS domain,
    ROUND(SUM(CAST(a.line_item_subtotal AS FLOAT64)), 0)         AS account_od_spend
  FROM `lambda-data-warehouse.general.fct_chargebee_component_ledger` a
  LEFT JOIN `lambda-data-warehouse.general.dim_chargebee_customers` c
    ON a.chargebee_customer_id = c.chargebee_customer_id
  WHERE c.chargebee_customer_email IS NOT NULL
    AND CAST(a.line_item_subtotal AS FLOAT64) > 0
    AND a.standard_rate_ll_instance_type NOT LIKE '%cpu%'
    AND a.standard_rate_ll_instance_type NOT LIKE '%storage%'
    AND c.chargebee_customer_email NOT LIKE '%@lambda%'
    AND c.chargebee_customer_email NOT LIKE '%@lambdal.com%'
    AND REGEXP_EXTRACT(LOWER(c.chargebee_customer_email), r'@(.+)$')
        NOT IN (SELECT domain FROM excluded_personal)
  GROUP BY 1
),

user_activity AS (
  SELECT
    LOWER(c.chargebee_customer_email)                             AS user_email,
    REGEXP_EXTRACT(LOWER(c.chargebee_customer_email), r'@(.+)$') AS domain,
    MIN(a.line_item_date)                                         AS first_od_date,
    MAX(a.line_item_date)                                         AS last_od_date,
    ROUND(SUM(CAST(a.line_item_subtotal AS FLOAT64)), 0)          AS user_od_spend,
    COUNT(DISTINCT DATE_TRUNC(a.line_item_date, MONTH))           AS months_active
  FROM `lambda-data-warehouse.general.fct_chargebee_component_ledger` a
  LEFT JOIN `lambda-data-warehouse.general.dim_chargebee_customers` c
    ON a.chargebee_customer_id = c.chargebee_customer_id
  WHERE c.chargebee_customer_email IS NOT NULL
    AND CAST(a.line_item_subtotal AS FLOAT64) > 0
    AND a.standard_rate_ll_instance_type NOT LIKE '%cpu%'
    AND a.standard_rate_ll_instance_type NOT LIKE '%storage%'
    AND c.chargebee_customer_email NOT LIKE '%@lambda%'
    AND c.chargebee_customer_email NOT LIKE '%@lambdal.com%'
    AND REGEXP_EXTRACT(LOWER(c.chargebee_customer_email), r'@(.+)$')
        NOT IN (SELECT domain FROM excluded_personal)
  GROUP BY 1, 2
),

gpu_spend_by_type AS (
  SELECT
    LOWER(c.chargebee_customer_email)          AS user_email,
    a.standard_rate_ll_instance_type           AS gpu_type,
    SUM(CAST(a.line_item_subtotal AS FLOAT64)) AS gpu_spend
  FROM `lambda-data-warehouse.general.fct_chargebee_component_ledger` a
  LEFT JOIN `lambda-data-warehouse.general.dim_chargebee_customers` c
    ON a.chargebee_customer_id = c.chargebee_customer_id
  WHERE c.chargebee_customer_email IS NOT NULL
    AND CAST(a.line_item_subtotal AS FLOAT64) > 0
    AND a.standard_rate_ll_instance_type NOT LIKE '%cpu%'
    AND a.standard_rate_ll_instance_type NOT LIKE '%storage%'
  GROUP BY 1, 2
),

primary_gpu AS (
  SELECT user_email, gpu_type AS primary_gpu_type
  FROM (
    SELECT
      user_email,
      gpu_type,
      ROW_NUMBER() OVER (
        PARTITION BY user_email
        ORDER BY gpu_spend DESC
      ) AS rn
    FROM gpu_spend_by_type
  )
  WHERE rn = 1
),

raw_tags AS (
  SELECT
    REGEXP_EXTRACT(LOWER(lambda_account__email), r'@(.+)$') AS domain,
    CASE
      WHEN mom_spend_change > 0.80
        AND average_monthly_spend_dollars > 500
      THEN '1_High_Growth'

      WHEN billing_months_active >= 6
        AND monthly_spend_cv < 0.5
        AND average_monthly_spend_dollars > 200
      THEN '2_1CC_Candidate'

      WHEN mom_spend_change < -0.50
        AND average_monthly_spend_dollars > 500
      THEN '3_Declining'

      WHEN has_used_a100 = TRUE
        AND has_used_h100 = FALSE
        AND COALESCE(days_since_last_active_instance, 999) <= 90
      THEN '4_A100_Migration'

      WHEN COALESCE(days_since_last_active_instance, 999) >= 60
      THEN '5_Abandoned'

      ELSE 'No_Tag'
    END AS behavioral_tag

  FROM `lambda-data-warehouse.general.dim_public_cloud_accounts`
  WHERE NOT is_internal_email
    AND lambda_account__email IS NOT NULL
    AND REGEXP_EXTRACT(LOWER(lambda_account__email), r'@(.+)$')
        NOT IN (SELECT domain FROM excluded_personal)
),

best_tag AS (
  SELECT domain, behavioral_tag
  FROM (
    SELECT
      domain,
      behavioral_tag,
      ROW_NUMBER() OVER (
        PARTITION BY domain
        ORDER BY behavioral_tag ASC
      ) AS rn
    FROM raw_tags
  )
  WHERE rn = 1
),

sfdc_contacts AS (
  SELECT norm_email, first_name, last_name, title
  FROM (
    SELECT
      LOWER(TRIM(lead_or_contact_email))  AS norm_email,
      lead_or_contact_first_name          AS first_name,
      lead_or_contact_last_name           AS last_name,
      lead_or_contact_title               AS title,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(lead_or_contact_email))
        ORDER BY
          CASE WHEN lead_or_contact_title IS NOT NULL    THEN 0 ELSE 1 END,
          CASE WHEN lead_or_contact_object_type = 'Contact' THEN 0 ELSE 1 END
      ) AS rn
    FROM `lambda-data-warehouse.general.dim_salesforce_leads_and_contacts`
    WHERE lead_or_contact_email IS NOT NULL
  )
  WHERE rn = 1
),

joined AS (
  SELECT
    sa.account_owner_name                                          AS seller_name,
    sa.account_pod,
    sa.account_name,
    tot.account_od_spend,
    COALESCE(bt.behavioral_tag, 'No_Tag')                         AS behavioral_tag,
    sa.account_last_activity_date,
    DATE_DIFF(CURRENT_DATE(), sa.account_last_activity_date, DAY) AS days_since_seller_activity,
    NULLIF(TRIM(CONCAT(
      COALESCE(sc.first_name, ''), ' ',
      COALESCE(sc.last_name,  '')
    )), '')                                                        AS contact_name,
    sc.title                                                       AS contact_title,
    ua.user_email,
    pg.primary_gpu_type,
    ua.first_od_date,
    ua.last_od_date,
    ua.months_active,
    ua.user_od_spend,
    CONCAT('https://lambda.lightning.force.com/lightning/r/Account/',
           sa.account_id, '/view')                                 AS sfdc_account_link,

    ROW_NUMBER() OVER (
      PARTITION BY sa.account_id, ua.user_email
      ORDER BY tot.account_od_spend DESC
    ) AS rn

  FROM seller_accounts     sa
  JOIN  acct_totals        tot ON sa.domain     = tot.domain
  JOIN  user_activity      ua  ON sa.domain     = ua.domain
  LEFT JOIN best_tag       bt  ON sa.domain     = bt.domain
  LEFT JOIN primary_gpu    pg  ON ua.user_email = pg.user_email
  LEFT JOIN sfdc_contacts  sc  ON ua.user_email = sc.norm_email
)

SELECT
  seller_name,
  account_pod,
  account_name,
  account_od_spend,
  behavioral_tag,
  CAST(account_last_activity_date AS STRING)  AS account_last_activity_date,
  days_since_seller_activity,
  contact_name,
  contact_title,
  user_email,
  primary_gpu_type,
  CAST(first_od_date AS STRING)               AS first_od_date,
  CAST(last_od_date  AS STRING)               AS last_od_date,
  months_active,
  user_od_spend,
  sfdc_account_link

FROM joined
WHERE rn = 1
  AND account_od_spend > 0

ORDER BY
  seller_name,
  behavioral_tag    ASC,
  account_od_spend  DESC,
  user_od_spend     DESC
"""

# ── Auth ──────────────────────────────────────────────────────────────────────

def build_client() -> bigquery.Client:
    creds_json = os.environ.get("BIGQUERY_CREDENTIALS")
    if not creds_json:
        # Fall back to application default credentials (local dev)
        print("BIGQUERY_CREDENTIALS not set — using application default credentials")
        return bigquery.Client()

    creds_data = json.loads(creds_json)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(creds_data, f)
        key_path = f.name

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
    )
    project = creds_data.get("project_id")
    return bigquery.Client(credentials=credentials, project=project)


# ── Run ───────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to BigQuery…")
    client = build_client()

    print("Running OD dashboard query…")
    rows = list(client.query(QUERY).result())
    print(f"  → {len(rows)} rows returned")

    records = []
    for row in rows:
        records.append({
            "seller_name":               row.seller_name,
            "account_pod":               row.account_pod,
            "account_name":              row.account_name,
            "account_od_spend":          float(row.account_od_spend or 0),
            "behavioral_tag":            row.behavioral_tag,
            "account_last_activity_date":row.account_last_activity_date,
            "days_since_seller_activity":row.days_since_seller_activity,
            "contact_name":              row.contact_name,
            "contact_title":             row.contact_title,
            "user_email":                row.user_email,
            "primary_gpu_type":          row.primary_gpu_type,
            "first_od_date":             row.first_od_date,
            "last_od_date":              row.last_od_date,
            "months_active":             row.months_active,
            "user_od_spend":             float(row.user_od_spend or 0),
            "sfdc_account_link":         row.sfdc_account_link,
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_date":      datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "row_count":     len(records),
        "rows":          records,
    }

    out_dir = os.path.join(os.path.dirname(__file__), "docs", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "od_dashboard.json")

    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    print(f"Written → {out_path}  ({len(records)} rows)")


if __name__ == "__main__":
    main()
