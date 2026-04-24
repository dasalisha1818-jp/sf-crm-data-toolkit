"""
cleaner.py
----------
Normalizes Salesforce record data:
  - Title-cases name fields
  - Strips leading/trailing whitespace
  - Normalizes phone numbers to E.164-like format
  - Lowercases email addresses

WHY: Raw CRM data from multiple sources is inconsistently formatted.
     Normalization ensures uniform data for downstream reports and integrations.
HOW: Loads records → transforms in-memory via pandas → pushes updates via Bulk API.
"""

import re
import pandas as pd
from simple_salesforce import Salesforce
from src.config import config
from src.auditor import get_salesforce_client, fetch_records


NAME_FIELDS = ["FirstName", "LastName", "Name"]
EMAIL_FIELDS = ["Email"]
PHONE_FIELDS = ["Phone", "MobilePhone"]

CLEANSE_FIELDS = {
    "Contact": ["FirstName", "LastName", "Email", "Phone"],
    "Account": ["Name", "BillingCity"],
    "Lead": ["FirstName", "LastName", "Email", "Phone", "Company"],
}


def normalize_phone(phone: str) -> str:
    """Strip all non-numeric characters except leading +."""
    if not phone:
        return phone
    digits = re.sub(r"[^\d+]", "", phone.strip())
    return digits


def clean_dataframe(df: pd.DataFrame, obj: str) -> tuple[pd.DataFrame, int]:
    """
    Apply cleansing transformations to DataFrame.
    Returns (cleaned_df, change_count).
    """
    fields = CLEANSE_FIELDS.get(obj, [])
    changed = 0
    df = df.copy()

    for field in fields:
        if field not in df.columns:
            continue

        original = df[field].copy()

        # Strip whitespace
        df[field] = df[field].astype(str).str.strip()
        df[field] = df[field].replace("nan", None)

        # Title case for name fields
        if field in NAME_FIELDS:
            df[field] = df[field].str.title()

        # Lowercase for email fields
        if field in EMAIL_FIELDS:
            df[field] = df[field].str.lower()

        # Normalize phone
        if field in PHONE_FIELDS:
            df[field] = df[field].apply(
                lambda x: normalize_phone(x) if isinstance(x, str) else x
            )

        changed += (df[field] != original).sum()

    return df, int(changed)


def push_updates(sf: Salesforce, obj: str, df: pd.DataFrame):
    """
    Push cleaned records back to Salesforce via Bulk API.
    Skips if DRY_RUN is enabled.
    """
    records = df.to_dict(orient="records")
    if config.DRY_RUN:
        print(f"[DRY RUN] Would update {len(records)} {obj} records. Skipping.")
        return

    sf_obj = getattr(sf.bulk, obj)
    result = sf_obj.update(records, batch_size=config.BULK_BATCH_SIZE)
    success = sum(1 for r in result if r.get("success"))
    failed = len(result) - success
    print(f"[CLEAN] Updated {success} records | Failed: {failed}")


def cleanse_object(obj: str) -> dict:
    """
    Full cleanse pipeline for a Salesforce object.
    Returns summary dict.
    """
    sf = get_salesforce_client()
    fields = CLEANSE_FIELDS.get(obj, [])

    print(f"[CLEAN] Fetching {obj} records...")
    df = fetch_records(sf, obj, fields + ["Id"])
    original_count = len(df)

    cleaned_df, change_count = clean_dataframe(df, obj)

    print(f"[CLEAN] {original_count} records processed, {change_count} field values updated.")
    push_updates(sf, obj, cleaned_df[["Id"] + fields])

    return {
        "object": obj,
        "total_records": original_count,
        "fields_updated": change_count,
        "dry_run": config.DRY_RUN,
    }
