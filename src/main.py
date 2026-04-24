"""
main.py
-------
CLI entry point for the Salesforce CRM Data Quality & Migration Toolkit.

Usage:
    python src/main.py --mode audit      --object Contact
    python src/main.py --mode cleanse    --object Contact
    python src/main.py --mode deduplicate --object Contact --threshold 85
    python src/main.py --mode migrate    --object Contact --file sample_data/sample_contacts.csv
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Salesforce CRM Data Quality & Migration Toolkit"
    )
    parser.add_argument(
        "--mode",
        choices=["audit", "cleanse", "deduplicate", "migrate"],
        required=True,
        help="Operation mode",
    )
    parser.add_argument(
        "--object",
        default="Contact",
        help="Salesforce object name (e.g. Contact, Account, Lead)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=85,
        help="Fuzzy match threshold for deduplication (0-100)",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Path to CSV file for migration mode",
    )

    args = parser.parse_args()

    if args.mode == "audit":
        from src.auditor import audit_object
        report = audit_object(args.object)
        print(f"\nAudit complete. {report['total_records']} records scanned.")

    elif args.mode == "cleanse":
        from src.cleaner import cleanse_object
        result = cleanse_object(args.object)
        print(f"\nCleanse complete. {result['fields_updated']} field values updated.")

    elif args.mode == "deduplicate":
        from src.deduplicator import deduplicate_object
        result = deduplicate_object(args.object, threshold=args.threshold)
        print(f"\nDedup complete. {result['duplicate_pairs_found']} pairs found.")

    elif args.mode == "migrate":
        if not args.file:
            print("ERROR: --file is required for migrate mode.")
            sys.exit(1)
        from src.migrator import migrate_from_csv
        result = migrate_from_csv(args.object, args.file)
        print(f"\nMigration complete: {result}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
