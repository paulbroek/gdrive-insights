"""Settings.py, general settings for gdrive-insights."""

from pathlib import Path

# todo: how to  load .env file from site-packages dir
# REPO_PATH = os.environ.get("GDRIVE_INSIGHTS_REPO")
# assert REPO_PATH is not None

REPO_PATH = "/home/paul/repos/gdrive-insights"

REPO_DIR = Path(REPO_PATH) / "gdrive_insights"
DATA_DIR = REPO_DIR / "data"

CHANGES_FILE = DATA_DIR / "changes.feather"
FILES_FILE = DATA_DIR / "files.feather"
REVISIONS_FILE = DATA_DIR / "revisions.feather"
BOOK_FILE = DATA_DIR / "df_book.feather"

STORAGE_JSON_FILE = REPO_DIR / "storage.json"
CLIENT_ID_JSON_FILE = REPO_DIR / "client_id.json"
