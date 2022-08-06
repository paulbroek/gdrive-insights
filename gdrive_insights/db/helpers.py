"""helpers.py, helper methods for SQLAlchemy models, listed in models.py."""

import logging
from typing import Any, Dict, Optional

import gdrive_insights.config as config_dir
import pandas as pd
from rarc_utils.sqlalchemy_base import create_many, get_session, load_config
from sqlalchemy.future import select  # type: ignore[import]

from ..core.types import FileId, FileRec
from .models import Change, File, Revision

psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)
psession = get_session(psql)()

logger = logging.getLogger(__name__)


async def create_many_items(asession, *args, **kwargs):
    """Create many SQLAlchemy model items in db."""
    # asession = args[0]
    async with asession() as session:
        items = await create_many(session, *args, **kwargs)

    return items


def update_is_forbidden(file_id: str):
    """Update File.is_forbidden.

    Remedy to deal with `Encountered 403 Forbidden with reason "insufficientFilePermissions"`
    messages from Google Drive API
    """
    file = psession.query(File).filter(File.id == file_id).one_or_none()
    assert file is not None, f"create file first"
    file.is_forbidden = True
    return psession.commit()


def construct_file_path(fileId: str, fullPath="") -> Optional[str]:
    """Construct file path.

    Repeatedly calls
        GET https://www.googleapis.com/drive/v3/files/[FileId]?fields=parents
    till root node is found.

    Reconstructs a file path
    """
    parent = DRIVE.files().get(fileId=fileId, fields="parents").execute()
    name = DRIVE.files().get(fileId=fileId, fields="name").execute().get("name", None)
    parent_id = parent.get("parents", None)
    parent_id = parent_id[0] if parent_id is not None else None

    if parent_id is not None:
        print(f"{parent_id=:<40} {name=:<50} ")
        fullPath = "/" + name + fullPath
        return construct_file_path(parent_id, fullPath=fullPath)

    print(f"{fullPath=}")

    return fullPath


def map_files_to_path(df):
    """Call `construct_file_path` on all `id` rows."""
    df = df.copy()
    df["path"] = df["id"].map(construct_file_path)
    return df


def update_file_paths(df):
    """Update file paths in db for a dataframe of files."""
    assert "id" in df.columns
    assert "path" in df.columns

    # fetch files
    ids = df.id.unique()
    files_by_id = dict(
        zip(
            ids,
            psession.execute(select(File).filter(File.id.in_(ids)))
            .scalars()
            .fetchall(),
        )
    )
    df["file"] = df["id"].map(files_by_id)

    nmissing = df["file"].isna().sum()
    logger.info(f"{nmissing=:,}")

    # update file_paths
    file_and_path = df[["file", "path"]].to_records(index=False)
    for file, path in file_and_path:
        file.path = path

    psession.commit()

    return df
