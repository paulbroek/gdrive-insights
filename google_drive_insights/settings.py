"""Settings.py, general settings for google-drive-insights."""

import os
from pathlib import Path

# todo: how to  load .env file from site-packages dir
# REPO_PATH = os.environ.get("GOOGLE_DRIVE_INSIGHTS_REPO")
# assert REPO_PATH is not None

REPO_PATH = "/home/paul/repos/google-drive-insights"

REPO_DIR = Path(REPO_PATH) / "google_drive_insights"
DATA_DIR = REPO_DIR / "data"

CHANGES_FILE = DATA_DIR / "changes.feather"
REVISIONS_FILE = DATA_DIR / "revisions.feather"
