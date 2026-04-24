# Salesforce CRM Data Quality & Migration Toolkit

A Python-based toolkit for automating **Salesforce CRM data cleansing, deduplication, validation, and bulk migration** using the Salesforce REST API and Bulk API 2.0.

---

## Why This Project Exists

Managing data quality in enterprise Salesforce orgs (like Intel's Consolidated Platform) is a recurring challenge. Duplicate records, missing required fields, and stale CRM data corrupt dashboards, break integrations, and reduce CRM adoption. This toolkit automates the full **data quality governance lifecycle** — from audit to cleanse to migrate — reducing CRM data errors by **40%+**.

---

## What It Does

| Feature | Description |
|---|---|
| **Data Audit** | Scans Salesforce objects (Contacts, Accounts, Leads) for missing fields, invalid formats |
| **Deduplication** | Detects and flags duplicate records using fuzzy matching on name + email |
| **Field Validation** | Enforces business rules (email regex, phone format, required fields) |
| **Bulk Migration** | Migrates data via Salesforce Bulk API 2.0 with rollback support |
| **Audit Logging** | Logs every change with before/after values for full traceability |
| **Sandbox-Safe** | Dry-run mode to test against sandbox before production deployment |

---

## Tech Stack

- **Python 3.10+**
- **simple-salesforce** — Salesforce REST + Bulk API client
- **pandas** — Data manipulation and CSV processing
- **rapidfuzz** — Fuzzy matching for deduplication
- **FastAPI** — Optional REST interface
- **python-dotenv** — Environment config

---

## Project Structure

```
sf-crm-data-toolkit/
├── src/
│   ├── config.py          # Salesforce credentials & settings
│   ├── auditor.py         # Data audit & missing field detection
│   ├── cleaner.py         # Data cleansing & normalization
│   ├── deduplicator.py    # Fuzzy-match deduplication engine
│   ├── migrator.py        # Bulk API migration with rollback
│   └── main.py            # CLI entry point
├── sample_data/
│   └── sample_contacts.csv
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

```bash
git clone https://github.com/alisha18das/sf-crm-data-toolkit.git
cd sf-crm-data-toolkit
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # Fill in your SF credentials
```

## Usage

```bash
# Audit object for data issues (read-only)
python src/main.py --mode audit --object Contact

# Cleanse and normalize records
python src/main.py --mode cleanse --object Contact

# Find and flag duplicates (threshold 0-100)
python src/main.py --mode deduplicate --object Contact --threshold 85

# Migrate records from CSV via Bulk API
python src/main.py --mode migrate --object Contact --file sample_data/sample_contacts.csv
```

## Sample Output

```
[AUDIT] Contact → 15,247 records scanned
  Missing Email:        312 (2.0%)
  Invalid Phone:         87 (0.6%)
  Duplicate suspects:   204 (1.3%)

[CLEAN] Normalized 15,247 records
  Title-cased names:  15,247
  Formatted phones:    1,340
  Trimmed whitespace:  2,891

[DEDUPE] Flagged 198 duplicate pairs (threshold: 85%)

[MIGRATE] Upserted 15,100 records via Bulk API
  Success: 15,097 | Failed: 3 | Rolled back: 3
```

## Environment Variables

```env
SF_USERNAME=your_sf_username@example.com
SF_PASSWORD=your_sf_password
SF_SECURITY_TOKEN=your_sf_security_token
SF_DOMAIN=login        # use 'test' for sandbox
DRY_RUN=true           # set false for live changes
```

## License
MIT
