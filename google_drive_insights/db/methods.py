from typing import Dict, List, Tuple

import pandas as pd

from ..core.types import FileId, FileRec, TableTypes
from .helpers import create_many_items
from .models import Change, File, Revision


class methods:
    """Db methods: methods for syncing data with db."""

    @classmethod
    async def push_files(
        cls, df: pd.DataFrame, async_session, autobulk=True, returnExisting=False
    ) -> Dict[str, Dict[str, TableTypes]]:
        """Push files to db."""
        df = df.copy()
        records_dict = {}

        recs = cls._make_file_recs(df)
        records_dict["channel"] = await create_many_items(
            async_session,
            File,
            recs,
            nameAttr="id",
            # returnExisting=returnExisting,
            mergeExisting=True,
            autobulk=autobulk,
            commit=True,
        )

        return records_dict

    @classmethod
    async def push_revisions(
        cls, df: pd.DataFrame, async_session, autobulk=True, returnExisting=False
    ) -> Dict[str, Dict[str, TableTypes]]:
        """Push revisions to db."""
        df = df.copy()
        records_dict = {}

        recs = cls._make_revision_recs(df)
        records_dict["channel"] = await create_many_items(
            async_session,
            Revision,
            recs,
            nameAttr="id",
            # returnExisting=returnExisting,
            mergeExisting=True,
            autobulk=autobulk,
            commit=True,
        )

        return records_dict

    @staticmethod
    def _make_file_recs(
        df: pd.DataFrame, columns=("id", "mimeType", "name")
    ) -> Dict[FileId, FileRec]:
        """Make File records from dataframe."""
        recs = (
            df.rename(
                columns={
                    "file_id": "id",
                    "file_mimeType": "mimeType",
                    "file_name": "name",
                }
            )[list(columns)]
            .assign(index=df["file_id"])
            .set_index("index")
            .drop_duplicates("id")
            .to_dict("index")
        )

        return recs

    @staticmethod
    def _make_revision_recs(
        df: pd.DataFrame, columns=("id", "file_id", "mimeType", "modifiedTime")
    ) -> Dict[FileId, FileRec]:
        """Make Revision records from dataframe."""
        recs = (
            df.rename(columns={"fileId": "file_id"})[list(columns)]
            .assign(index=df["id"])
            .set_index("index")
            .drop_duplicates("id")
            .to_dict("index")
        )

        return recs
