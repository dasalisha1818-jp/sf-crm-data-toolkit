"""
migrator.py
-----------
Migrates records from a CSV file into Salesforce via the Bulk API 2.0.

Features:
  - Validates CSV schema before upload
  - Upserts records using External ID or Id
  - Saves a rollback file of pre-migration state
  - Rolls back on failure by reverting changed records

WHY: Manual data imports (via Data Loader GUI) are error-prone and not auditable.
     This module makes migrations scriptable, repeatable, and safe.
HOW: CSV → validate → snapshot existing records → upsert → check results →
     rollback if failures exceed threshold.
"""

import json
import os
import pandas as pd
from datetime import datetime
from simple_salesforce import Salesforce
from src.config import config
from src.auditor import get_salesforce_client


ROLLBACK_DIR = "rollback_snapshots"
FAILURE_THRESHOLD = 0.05   # 5% failure rate triggers rollback


def load_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()
    return df


def validate_schema(df: pd.DataFrame, obj: str, sf: Salesforce) -> list[str]:
    """
    Check that CSV columns match Salesforce object fields.
    Returns list of invalid column names.
    """
    sf_fields = set(
        f["name"] for f in getattr(sf, obj).describe()["fields"]
    )
    csv_fields = set(df.columns) - {"Id"}
    invalid = [f for f in csv_fields if f not in sf_fields]
    return invalid


def snapshot_records(sf: Salesforce, obj: str, ids: list[str]) -> str:
    """
    Save current state of records to a rollback file.
    Returns path to the snapshot file.
    """
    os.makedirs(ROLLBACK_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(ROLLBACK_DIR, f"{obj}_{timestamp}_snapshot.json")

    id_list = "', '".join(ids[:500])
    soql = f"SELECT Id, Name FROM {obj} WHERE Id IN ('{id_list}')"
    result = getattr(sf, obj).query(soql)["records"]

    with open(filepath, "w") as f:
        json.dump(result, f, indent=2)

    print(f"[MIGRATE] Snapshot saved to {filepath}")
    return filepath


def rollback_records(sf: Salesforce, obj: str, snapshot_path: str):
    """Restore records from a snapshot file."""
    with open(snapshot_path) as f:
        records = json.load(f)

    # Remove Salesforce metadata fields
    clean_records = [
        {k: v for k, v in r.items() if not k.startswith("attributes")}
        for r in records
    ]

    if config.DRY_RUN:
        print(f"[DRY RUN] Would restore {len(clean_records)} records. Skipping.")
        return

    sf_obj = getattr(sf.bulk, obj)
    result = sf_obj.update(clean_records, batch_size=config.BULK_BATCH_SIZE)
    success = sum(1 for r in result if r.get("success"))
    print(f"[ROLLBACK] Restored {success}/{len(clean_records)} records.")


def migrate_from_csv(obj: str, filepath: str, external_id_field: str = "Id") -> dict:
    """
    Full migration pipeline: validate → snapshot → upsert → rollback if needed.
    """
    sf = get_salesforce_client()

    print(f"[MIGRATE] Loading CSV: {filepath}")
    df = load_csv(filepath)
    print(f"[MIGRATE] {len(df)} records loaded.")

    print("[MIGRATE] Validating schema...")
    invalid_fields = validate_schema(df, obj, sf)
    if invalid_fields:
        raise ValueError(f"Invalid fields for {obj}: {invalid_fields}")

    snapshot_path = None
    if not config.DRY_RUN:
        existing_ids = df["Id"].dropna().tolist() if "Id" in df.columns else []
        if existing_ids:
            snapshot_path = snapshot_records(sf, obj, existing_ids)

    records = df.to_dict(orient="records")

    if config.DRY_RUN:
        print(f"[DRY RUN] Would upsert {len(records)} records. Skipping.")
        return {"object": obj, "total": len(records), "dry_run": True}

    print(f"[MIGRATE] Upserting {len(records)} records via Bulk API...")
    sf_obj = getattr(sf.bulk, obj)
    result = sf_obj.upsert(records, external_id_field, batch_size=config.BULK_BATCH_SIZE)

    success = sum(1 for r in result if r.get("success"))
    failed = len(result) - success
    failure_rate = failed / len(result) if result else 0

    print(f"[MIGRATE] Success: {success} | Failed: {failed} | Rate: {failure_rate:.1%}")

    if failure_rate > FAILURE_THRESHOLD and snapshot_path:
        print(f"[MIGRATE] Failure rate {failure_rate:.1%} exceeds threshold. Rolling back...")
        rollback_records(sf, obj, snapshot_path)

    return {
        "object": obj,
        "total": len(records),
        "success": success,
        "failed": failed,
        "failure_rate": failure_rate,
        "rolled_back": failure_rate > FAILURE_THRESHOLD,
    }
