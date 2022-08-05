"""helpers.py, helper methods for SQLAlchemy models, listed in models.py."""

import logging

import gdrive_insights.config as config_dir
from rarc_utils.sqlalchemy_base import create_many, get_session, load_config

from ..core.types import FileId, FileRec
from .models import Change, File, Revision

# from sqlalchemy.future import select  # type: ignore[import]


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
