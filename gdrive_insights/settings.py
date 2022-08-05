"""Settings.py, general settings for gdrive-insights."""

import os
from pathlib import Path

# todo: how to  load .env file from site-packages dir
# REPO_PATH = os.environ.get("GDRIVE_INSIGHTS_REPO")
# assert REPO_PATH is not None

REPO_PATH = "/home/paul/repos/gdrive-insights"

REPO_DIR = Path(REPO_PATH) / "gdrive_insights"
DATA_DIR = REPO_DIR / "data"

CHANGES_FILE = DATA_DIR / "changes.feather"
REVISIONS_FILE = DATA_DIR / "revisions.feather"
