"""
config.py
---------
Loads Salesforce credentials and app settings from environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SF_USERNAME: str = os.getenv("SF_USERNAME", "")
    SF_PASSWORD: str = os.getenv("SF_PASSWORD", "")
    SF_SECURITY_TOKEN: str = os.getenv("SF_SECURITY_TOKEN", "")
    SF_DOMAIN: str = os.getenv("SF_DOMAIN", "login")   # 'test' for sandbox
    DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() == "true"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Deduplication defaults
    DEDUP_THRESHOLD: int = 85          # fuzzy match score 0-100
    DEDUP_MATCH_FIELDS: list = ["Name", "Email"]

    # Bulk API batch size
    BULK_BATCH_SIZE: int = 10_000


config = Config()
