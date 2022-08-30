"""Settings.py, general settings for gdrive-insights."""

import os
from pathlib import Path

REPO_PATH = os.environ.get("GDRIVE_INSIGHTS_REPO", "/home/paul/repos/gdrive-insights")

FEATHER_SFX = ".feather"
JSON_SFX = ".json"

REPO_DIR = Path(REPO_PATH) / "gdrive_insights"
DATA_DIR = REPO_DIR / "data"

CHANGES_FILE = (DATA_DIR / "changes").with_suffix(FEATHER_SFX)
FILES_FILE = (DATA_DIR / "files").with_suffix(FEATHER_SFX)
REVISIONS_FILE = (DATA_DIR / "revisions").with_suffix(FEATHER_SFX)
BOOK_FILE = (DATA_DIR / "df_book").with_suffix(FEATHER_SFX)

STORAGE_JSON_FILE = (REPO_DIR / "storage").with_suffix(JSON_SFX)
CLIENT_ID_JSON_FILE = (REPO_DIR / "client_id").with_suffix(JSON_SFX)

GOOGLE_DOCUMENT_FILETYPE = "application/vnd.google-apps.document"
PDF_FILETYPE = "application/pdf"
