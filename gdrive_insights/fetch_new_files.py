"""fetch_new_files.py.

fetch new files in google drive
uses pageToken saved in db, and starts fetching from lastPageToken[-1] till latest new item

Usage:
    ipy fetch_new_files.py -i -- -t 2080713
    # or without -t, uses second last page_token from db
    ipy fetch_new_files.py
"""

import argparse
import asyncio

import psycopg2  # type: ignore[import]
from gdrive_insights import config as config_dir
from gdrive_insights.db.helpers import get_page_tokens
from gdrive_insights.db.methods import methods as dm
from gdrive_insights.db.models import psql
from gdrive_insights.display_changes import changes_to_pandas, fetch_changes
from rarc_utils.sqlalchemy_base import get_async_session, load_config

psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)

async_session = get_async_session(psql)

# connect to postgresql
con = psycopg2.connect(
    database=psql.db, user=psql.user, password=psql.passwd, host=psql.host, port="5432"
)

parser = argparse.ArgumentParser(description="fetch_new_files.py cli parameters")
parser.add_argument(
    "-t",
    "--start_page_token",
    type=int,
    default=None,
    help="start_page_token to start polling from (low number will always start from first change in time)",
)

if __name__ == "__main__":
    args = parser.parse_args()
    loop = asyncio.get_event_loop()

    start_page_token = (
        args.start_page_token or get_page_tokens(con, n=2).iloc[0].val_int
    )
    start_page_token = str(start_page_token)

    changes = fetch_changes(saved_start_page_token=start_page_token)
    df = changes_to_pandas(changes)

    res_files = loop.run_until_complete(dm.push_files(df, async_session))
