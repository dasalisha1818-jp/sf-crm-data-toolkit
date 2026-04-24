"""
deduplicator.py
---------------
Detects and flags (or merges) duplicate Salesforce records using fuzzy string
matching on configurable key fields (default: Name + Email).

WHY: Duplicate CRM records inflate pipeline metrics, corrupt reporting,
     and cause sync conflicts with downstream systems like SAP/ERP.
HOW: Fetch records → compute pairwise fuzzy similarity → group duplicates
     → flag in Salesforce or merge via REST API.
"""

import pandas as pd
from rapidfuzz import fuzz, process
from simple_salesforce import Salesforce
from src.config import config
from src.auditor import get_salesforce_client, fetch_records


def build_composite_key(row: pd.Series, fields: list[str]) -> str:
    """Combine fields into a single string for fuzzy comparison."""
    parts = [str(row.get(f, "")).strip().lower() for f in fields if f in row]
    return " ".join(parts)


def find_duplicates(
    df: pd.DataFrame,
    match_fields: list[str],
    threshold: int = 85,
) -> list[tuple[str, str, float]]:
    """
    Find pairs of records where composite key similarity >= threshold.

    Returns:
        List of (id_a, id_b, score) tuples.
    """
    df = df.copy()
    df["_key"] = df.apply(lambda row: build_composite_key(row, match_fields), axis=1)

    keys = df["_key"].tolist()
    ids = df["Id"].tolist()

    duplicates: list[tuple[str, str, float]] = []
    visited = set()

    for i, (key_a, id_a) in enumerate(zip(keys, ids)):
        if i in visited:
            continue
        # Compare against all records after current index
        remaining_keys = keys[i + 1:]
        remaining_ids = ids[i + 1:]

        for j, (key_b, id_b) in enumerate(zip(remaining_keys, remaining_ids), start=i + 1):
            if j in visited:
                continue
            score = fuzz.token_sort_ratio(key_a, key_b)
            if score >= threshold:
                duplicates.append((id_a, id_b, score))
                visited.add(j)

    return duplicates


def flag_duplicates_in_sf(
    sf: Salesforce,
    obj: str,
    duplicate_pairs: list[tuple[str, str, float]],
):
    """
    Add a custom tag/note to duplicate records in Salesforce.
    In production, this would use a custom field like IsDuplicate__c.
    """
    if config.DRY_RUN:
        print(f"[DRY RUN] Would flag {len(duplicate_pairs)} duplicate pairs. Skipping.")
        return

    # Flag the second record in each pair as duplicate
    records_to_flag = [
        {"Id": id_b, "Description": f"[DUPLICATE] Matched with {id_a} (score: {score:.0f})"}
        for id_a, id_b, score in duplicate_pairs
    ]

    if records_to_flag:
        sf_obj = getattr(sf.bulk, obj)
        result = sf_obj.update(records_to_flag, batch_size=config.BULK_BATCH_SIZE)
        success = sum(1 for r in result if r.get("success"))
        print(f"[DEDUPE] Flagged {success} records as duplicates.")


def deduplicate_object(
    obj: str,
    match_fields: list[str] | None = None,
    threshold: int | None = None,
) -> dict:
    """
    Full deduplication pipeline for a Salesforce object.
    """
    if match_fields is None:
        match_fields = config.DEDUP_MATCH_FIELDS
    if threshold is None:
        threshold = config.DEDUP_THRESHOLD

    sf = get_salesforce_client()

    print(f"[DEDUPE] Fetching {obj} records for deduplication...")
    df = fetch_records(sf, obj, match_fields + ["Id"])

    print(f"[DEDUPE] Running fuzzy match (threshold={threshold})...")
    pairs = find_duplicates(df, match_fields, threshold)

    print(f"[DEDUPE] Found {len(pairs)} duplicate pairs.")
    for id_a, id_b, score in pairs[:10]:
        print(f"  {id_a} ↔ {id_b} (score: {score:.0f})")

    flag_duplicates_in_sf(sf, obj, pairs)

    return {
        "object": obj,
        "total_records": len(df),
        "duplicate_pairs_found": len(pairs),
        "threshold": threshold,
        "dry_run": config.DRY_RUN,
    }
