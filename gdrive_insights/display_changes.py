"""display_google_drive_changes.py.

Based on:
    https://developers.google.com/drive/api/guides/manage-changes#python

Steps I did:
    - I enabled `Google Drive API` for my GCP project `G Suite Testing`
    - added OAuth 2, and added test user
    - now you can run this file, login with your account and all changes will be fetched

See Google Drive API endpoints:
    https://developers.google.com/drive/api/v3/reference?apix=true

Todo:
    - Now that you know file changes, you can find all pdf files in the Books folder
    - Whenever you add a highlight, it can be detected by the API

How to run:
    conda activate py39
    cd ~/repos/gdrive-insights
    pip install --upgrade .

    # run:
    ipy display_changes.py -i -- -s -n 5
    ipy display_changes.py -i -- -s -n 5 --start_page_token 99106 --push

"""

from __future__ import print_function

import asyncio
import logging
import sys

import pandas as pd
import psycopg2  # type: ignore[import]
from gdrive_insights import config as config_dir
from gdrive_insights.args import ArgParser
from gdrive_insights.data_methods import data_methods as dm
from gdrive_insights.db.methods import methods as db_methods
from gdrive_insights.db.models import psql
from gdrive_insights.settings import CHANGES_FILE, FILES_FILE, REVISIONS_FILE
from rarc_utils.log import setup_logger
from rarc_utils.sqlalchemy_base import (get_async_session, get_session,
                                        load_config)

psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)

async_session = get_async_session(psql)
psession = get_session(psql)()

log_fmt = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-16s - %(levelname)-7s - %(message)s"  # name
logger = setup_logger(
    cmdLevel=logging.INFO, saveFile=0, savePandas=0, jsonLogger=0, color=1, fmt=log_fmt
)

UNNAMED = "Naamloos document"
GOOGLE_DOCUMENT_FILETYPE = "application/vnd.google-apps.document"
PDF_FILETYPE = "application/pdf"
FILE_ID = "id"

# connect to postgresql
con = psycopg2.connect(
    database=psql.db, user=psql.user, password=psql.passwd, host=psql.host, port="5432"
)


if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    parser = ArgParser.get_parser()
    args = parser.parse_args()

    start_page_token = int(args.start_page_token)

    if args.use_cache:
        # df = pd.read_feather(CHANGES_FILE)
        df = pd.read_feather(FILES_FILE)
        rv = pd.read_feather(REVISIONS_FILE)

        # start_page_token = df.page_token.max()

        # logger.info(f"start polling from {start_page_token=:,}")

    elif args.use_cache_sql:
        df = dm.files_from_sql()
        # df = changes_from_sql()
        rv = dm.revisions_from_sql()
        # start_page_token = df.page_token.max()
        # start_page_token = df.page_token.iloc[-1]

    if not args.dryrun:
        changes = dm.fetch_changes(
            saved_start_page_token=start_page_token, max_fetch=args.nfetch
        )

    # append lines to existing dataset
    # if args.use_cache:
    #     df = df.append(changes_to_pandas(changes)).reset_index(drop=True)

    # else:
    #     df = changes_to_pandas(changes)

    # df_pdf = df[df.file_mimeType.str.endswith("pdf")].copy()
    view = dm.filter_files(df, keep=None)

    # todo: how to fetch only new revisions, or new files?
    # maybe changes includes this data, when fetching the last list using pageToken?

    if args.dryrun:
        sys.exit()

    rv, fids = dm.revisions_pipeline(view, use_sql_cache=False)

    if args.save:
        df.to_feather(CHANGES_FILE)
        rv.to_feather(REVISIONS_FILE)

    if args.push:
        res_files = loop.run_until_complete(db_methods.push_files(view, async_session))
        res_revs = loop.run_until_complete(db_methods.push_revisions(rv, async_session))

    # fetch new revisions
    # view, forbidden_ids = revisions_pipeline(
    #     df[~df.is_forbidden], progress=1, keep=None, use_sql_cache=False
    # )
    # rv = revisions_from_feather()
    # revisions_data_analysis(df, rv).tail(25)

    # todo: make async, as soon as files / revisions come in, push them in batches to postgres.
    # gdrive api does not have async support, yet
