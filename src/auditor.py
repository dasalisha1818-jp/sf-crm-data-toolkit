"""
auditor.py
----------
Queries Salesforce and audits records for:
  - Missing required fields
  - Invalid field formats (email, phone)
  - Null/blank values in key fields

WHY: Before any cleanse or migrate, you need a full picture of data health.
     This module produces an audit report without touching any records.
"""

import re
import pandas as pd
from simple_salesforce import Salesforce
from src.config import config


EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w{2,}$")
PHONE_REGEX = re.compile(r"^\+?[\d\s\-\(\)]{7,15}$")

REQUIRED_FIELDS = {
    "Contact": ["FirstName", "LastName", "Email", "Phone", "AccountId"],
    "Account": ["Name", "Industry", "BillingCity"],
    "Lead":    ["FirstName", "LastName", "Email", "Company"],
}

FORMAT_FIELDS = {
    "Email": EMAIL_REGEX,
    "Phone": PHONE_REGEX,
}


def get_salesforce_client() -> Salesforce:
    return Salesforce(
        username=config.SF_USERNAME,
        password=config.SF_PASSWORD,
        security_token=config.SF_SECURITY_TOKEN,
        domain=config.SF_DOMAIN,
    )


def fetch_records(sf: Salesforce, obj: str, fields: list[str]) -> pd.DataFrame:
    """Run a SOQL query and return results as a DataFrame."""
    field_str = ", ".join(fields)
    soql = f"SELECT Id, {field_str} FROM {obj} LIMIT 50000"
    result = getattr(sf.bulk, obj).query(soql)
    return pd.DataFrame(result)


def audit_object(obj: str) -> dict:
    """
    Audit a Salesforce object and return a report dict.

    Returns:
        {
          "object": "Contact",
          "total_records": 1243,
          "missing_fields": {"Email": 87, "Phone": 34},
          "invalid_formats": {"Email": 12, "Phone": 9},
          "audit_rows": [...]   # list of row-level issues
        }
    """
    sf = get_salesforce_client()
    required = REQUIRED_FIELDS.get(obj, [])
    all_fields = list(set(required + list(FORMAT_FIELDS.keys())))

    print(f"[AUDIT] Fetching {obj} records from Salesforce...")
    df = fetch_records(sf, obj, all_fields)
    total = len(df)
    print(f"[AUDIT] {total} records retrieved.")

    missing_counts: dict[str, int] = {}
    invalid_counts: dict[str, int] = {}
    audit_rows = []

    for field in required:
        if field in df.columns:
            missing = df[field].isna() | (df[field].astype(str).str.strip() == "")
            count = int(missing.sum())
            if count:
                missing_counts[field] = count

    for field, regex in FORMAT_FIELDS.items():
        if field in df.columns:
            valid_mask = df[field].dropna().astype(str).str.match(regex)
            invalid_count = int((~valid_mask).sum())
            if invalid_count:
                invalid_counts[field] = invalid_count

    # Row-level detail for first 100 issues
    for _, row in df.iterrows():
        issues = []
        for field in required:
            if field in row and (pd.isna(row[field]) or str(row[field]).strip() == ""):
                issues.append(f"Missing {field}")
        for field, regex in FORMAT_FIELDS.items():
            if field in row and not pd.isna(row[field]):
                if not regex.match(str(row[field])):
                    issues.append(f"Invalid {field}")
        if issues:
            audit_rows.append({"Id": row.get("Id", "N/A"), "issues": issues})
        if len(audit_rows) >= 100:
            break

    report = {
        "object": obj,
        "total_records": total,
        "missing_fields": missing_counts,
        "invalid_formats": invalid_counts,
        "audit_rows": audit_rows,
    }

    _print_report(report)
    return report


def _print_report(report: dict):
    obj = report["object"]
    total = report["total_records"]
    print(f"\n{'='*50}")
    print(f"AUDIT REPORT — {obj}")
    print(f"{'='*50}")
    print(f"Total Records Scanned: {total}")
    print("\nMissing Required Fields:")
    for f, c in report["missing_fields"].items():
        pct = c / total * 100 if total else 0
        print(f"  {f}: {c} ({pct:.1f}%)")
    print("\nInvalid Field Formats:")
    for f, c in report["invalid_formats"].items():
        pct = c / total * 100 if total else 0
        print(f"  {f}: {c} ({pct:.1f}%)")
    print(f"{'='*50}\n")
