"""data_methods.py.

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
    - Whenever you add a highlight, it will be recorded by google, so you can reconstruct
        which books you read most

How to run:
    conda activate py39
    cd ~/repos/gdrive-insights
    pip install --upgrade .
    ipy display_changes -i -- -s -n 5
"""

from __future__ import print_function

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2  # type: ignore[import]
from googleapiclient.errors import HttpError  # type: ignore[import]
from rarc_utils.log import setup_logger
from tqdm import tqdm  # type: ignore[import]

from .core.utils import create_gdrive, unnest_col
from .db.helpers import get_or_update_page_token, update_is_forbidden
from .db.models import psql
from .settings import REVISIONS_FILE

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


#### google drive api
DRIVE = create_gdrive()
####


class data_methods:
    """Implements methods related to data transactions / dataframes."""

    @staticmethod
    def changes_to_pandas(items: List[Dict[str, Any]]) -> pd.DataFrame:

        df = pd.DataFrame(items)
        df = df.dropna(subset="file").reset_index()
        df["page_token"] = df["page_token"].astype(int)
        df = df.pipe(unnest_col, pfxCol="file")
        df["id"] = df["fileId"]

        return df

    @staticmethod
    def changes_to_sql(df: pd.DataFrame, table="change") -> None:

        df.to_sql(table, con, if_exists="append", index=False, index_label=False)

    @staticmethod
    def revisions_to_sql(df: pd.DataFrame, table="revision") -> None:

        df.to_sql(table, con, if_exists="append", index=False, index_label=False)

    @staticmethod
    def files_from_sql(n: Optional[int] = None, dropForbiddenRows=True) -> pd.DataFrame:
        q = "SELECT * FROM file"

        if dropForbiddenRows:
            q += " WHERE NOT is_forbidden"

        if n is not None:
            q += " LIMIT {}".format(n)

        logger.debug(q)
        df: pd.DataFrame = pd.read_sql_query(q, con)

        return df

    @staticmethod
    def changes_from_sql(
        n: Optional[int] = None, dropForbiddenRows=True
    ) -> pd.DataFrame:
        q = "SELECT * FROM change"

        if dropForbiddenRows:
            q += " WHERE NOT is_forbidden"

        if n is not None:
            q += " LIMIT {}".format(n)

        logger.debug(q)
        df: pd.DataFrame = pd.read_sql_query(q, con)

        return df

    @staticmethod
    def revisions_to_pandas(
        items: List[Dict[str, Any]], localize=False
    ) -> pd.DataFrame:
        df = pd.DataFrame(items)
        df["modifiedTime_iso"] = df["modifiedTime"]

        df["modifiedTime"] = pd.to_datetime(df.modifiedTime)

        if not localize:
            df["modifiedTime"] = df.modifiedTime.dt.tz_localize(None)

        return df

    @staticmethod
    def revisions_from_sql(n: Optional[int] = None) -> pd.DataFrame:
        q = "SELECT * FROM revision"
        if n is not None:
            q += " LIMIT {}".format(n)

        logger.debug(q)
        df: pd.DataFrame = pd.read_sql_query(q, con)

        return df

    @staticmethod
    def set_file_is_forbidden_df(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
        df = df.copy()
        assert isinstance(file_id, str)
        ixs = df[df[FILE_ID].isin([file_id])].index
        assert ixs.shape[0] == 1
        ix = ixs[0]
        df.loc[ix, "is_forbidden"] = True

        return df

    @staticmethod
    def filter_google_documents(
        df: pd.DataFrame, keep: Optional[int] = None
    ) -> pd.DataFrame:
        """Filter google documents from dataset.

        - Get all google docs files that do not have name=Unnamed, and count the number revisions
           These represent the actual number of changes to the document
        """
        view = df.query(
            "mimeType == '{}' & name != '{}'".format(GOOGLE_DOCUMENT_FILETYPE, UNNAMED)
        ).copy()

        if keep is not None:
            return view.tail(keep)

        return view

    @staticmethod
    def filter_pdf_files(df: pd.DataFrame, keep: Optional[int] = None) -> pd.DataFrame:
        """Filter pdf files from dataset."""
        view = df.query("mimeType == '{}'".format(PDF_FILETYPE))

        if keep is not None:
            return view.tail(keep)

        return view

    @classmethod
    def filter_files(df, keep=None):
        view = pd.concat(
            [
                df.pipe(cls.filter_google_documents),
                df.pipe(cls.filter_pdf_files),
            ],
            ignore_index=True,
        )

        if keep is not None:
            return view.tail(keep)

        return view

    @staticmethod
    def fetch_revisions(file_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Retrieve the list of revisions for file_id."""
        assert file_id is not None
        logger.debug(f"{file_id=}")
        response = DRIVE.revisions().list(fileId=file_id).execute()
        revisions: List[Dict[str, Any]] = response["revisions"]

        return revisions

    @classmethod
    def fetch_revisions_over_files(
        cls, df: pd.DataFrame, use_sql_cache=True, progress=True
    ) -> pd.DataFrame:

        logger.info(f"fetching revisions")
        # if use_sql_cache:
        #     existing_ids: pd.DataFrame = pd.read_sql_query(
        #         "SELECT id FROM revision; ", con
        #     )

        forbidden_ids = set()
        file_id_to_revisions = {}
        for file_id in tqdm(df[FILE_ID].values, disable=not progress):
            try:
                rev = cls.fetch_revisions(file_id)

            except Exception as e:
                df = cls.set_file_is_forbidden_df(df, file_id)
                logger.warning(f"should set {file_id=} to is_forbidden")
                forbidden_ids.add(file_id)

                update_is_forbidden(file_id)
                logger.info(f"{forbidden_ids=}")

                continue

            file_id_to_revisions[file_id] = rev

        # add fileId to records
        aa = [
            [{**{"fileId": k}, **a} for a in v] for k, v in file_id_to_revisions.items()
        ]
        recs: List[dict] = sum(aa, [])

        rev_df = cls.revisions_to_pandas(recs)

        # save intermediary results to sqlite
        # bit ugly for now, but fetch unique (id, fileId) tuples, and only insert if tuple is new
        if use_sql_cache:
            # # first drop existing ids
            # new_rows = rev_df[~rev_df.id.isin(existing_ids.id)]
            # # save to sql
            # if not new_rows.empty:
            #     new_rows.to_sql(
            #         "revision", con, if_exists="append", index=False, index_label=False
            #     )
            #     logger.info(f"added {new_rows.shape[0]:,} rows to sqlite")
            # else:
            #     logger.warning(f"no new rows to add")
            raise NotImplementedError

        # df["nrevision"] = df["file_id"].map(file_id_to_revisions)

        # return df.sort_values("nrevision", ascending=False)
        return rev_df, forbidden_ids

    @classmethod
    def revisions_pipeline(
        cls, df: pd.DataFrame, progress=True, use_sql_cache=True
    ) -> pd.DataFrame:
        """Complete revisions pipeline.

        Usage:
            view = pipeline(df, keep=10, progress=True)

        Todo:
            - implement cache. save all revisions to sqlite
        """
        view, forbidden_ids = df.pipe(
            cls.fetch_revisions_over_files,
            progress=progress,
            use_sql_cache=use_sql_cache,
        )

        return view, forbidden_ids

    @staticmethod
    def fetch_files(saved_start_page_token, max_fetch=None) -> List[dict]:
        """Retrieve the list of files for the currently authenticated user.

        Args:
            saved_start_page_token : StartPageToken for the current state of the
            account.
        Returns:
            saved start page token.
        """
        files = []

        try:

            # Begin with our last saved start token for this user or the
            page_token = saved_start_page_token

            nfetch = 0
            while page_token is not None:
                response = (
                    DRIVE.files().list(pageToken=page_token, spaces="drive").execute()
                )
                for file in response.get("files"):
                    # print(F'Change found for file: {change.get("fileId")}')
                    file["page_token"] = page_token

                files += response.get("files")
                print(f"{page_token=} {files[-1]['name']=}")
                if "newStartPageToken" in response:
                    # Last page, save this token for the next polling interval
                    saved_start_page_token = response.get("newStartPageToken")
                page_token = response.get("nextPageToken")

                if max_fetch is not None and nfetch >= max_fetch:
                    break

                nfetch += 1

        except HttpError as error:
            print(f"An error occurred: {error}")
            saved_start_page_token = None

        return files

    @staticmethod
    def fetch_changes(saved_start_page_token, max_fetch=None) -> List[dict]:
        """Retrieve the list of changes for the currently authenticated user.

            prints changed file's ID
        Args:
            saved_start_page_token : StartPageToken for the current state of the
            account.
        Returns:
            saved start page token.

        Load pre-authorized user credentials from the environment.
        TODO(developer) - See https://developers.google.com/identity
        for guides on implementing OAuth2 for the application.
        """
        changes = []

        try:

            # Begin with our last saved start token for this user or the
            # current token from getStartPageToken()
            page_token = saved_start_page_token
            # pylint: disable=maybe-no-member

            nfetch = 0
            while page_token is not None:
                response = (
                    DRIVE.changes().list(pageToken=page_token, spaces="drive").execute()
                )
                for change in response.get("changes"):
                    # print(F'Change found for file: {change.get("fileId")}')
                    change["page_token"] = page_token

                changes += response.get("changes")
                print(f"{page_token=} {changes[-1]['time']=}")
                if "newStartPageToken" in response:
                    # Last page, save this token for the next polling interval
                    saved_start_page_token = response.get("newStartPageToken")

                page_token = response.get("nextPageToken")

                # submit page_token to postgres
                if page_token is not None:
                    get_or_update_page_token("change", page_token)

                if max_fetch is not None and nfetch >= max_fetch:
                    break

                nfetch += 1

        except HttpError as error:
            print(f"An error occurred: {error}")
            saved_start_page_token = None

        return changes

    @staticmethod
    def revisions_from_feather() -> pd.DataFrame:
        df: pd.DataFrame = pd.read_feather(REVISIONS_FILE)
        return df

    @staticmethod
    def revisions_data_analysis(
        changes_df: pd.DataFrame, rev_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Analyse revisions.

        changes_df  changes dataset
        rev_df      revisions dataset
        """
        gb = (
            rev_df.groupby("file_id")
            .agg(
                count=("file_id", "count"),
                first_modified=("modifiedTime", "first"),
                last_modified=("modifiedTime", "last"),
            )
            .sort_values("count")
        ).reset_index()
        gb["last_min_first"] = gb["last_modified"] - gb["first_modified"]
        merged = pd.merge(
            gb,
            changes_df[["id", "name", "mimeType"]],
            how="left",
            left_on="file_id",
            right_on="id",
        )
        # now you see that fileId with more than 1 revision are max 1 month old. so google only saves revisions for a month,
        # you cannot fetch them from longer ago, should fetch them daily to gather this data
        # only recent pdf files show a lot of revisions? why? is revision data deleted over time?

        return merged
