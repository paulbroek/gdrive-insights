"""sqlite.py.

Legacy code. Projects uses PostgreSQL now
"""

import logging
import sqlite3

SQLITE_DB = "data/db.sqlite"

logger = logging.getLogger(__name__)

##### sqlite
con = sqlite3.connect(SQLITE_DB)
cur = con.cursor()

# pandas + sqlite can also create default schema, but I do need the unique constraint
CHANGE_SCHEMA = """
    shape_id INTEGER PRIMARY KEY,
    background_color TEXT,
    foreground_color TEXT,
    UNIQUE(background_color,foreground_color)
"""

REVISION_SCHEMA = """
    shape_id INTEGER PRIMARY KEY,
    background_color TEXT,
    foreground_color TEXT,
    UNIQUE(background_color,foreground_color)
"""

# add_revision_index = "CREATE UNIQUE INDEX ux_id_fileId ON revision(id, fileId);"
add_revision_index = "CREATE UNIQUE INDEX ux_id ON revision(id);"
#####


def set_file_is_forbidden(file_id: str) -> None:
    """Set file.is_forbidden to True.

    Some files cannot be fetched from Google Drive API.
    Todo:
        - fix that
    """
    try:
        q = "ALTER TABLE change \
         ADD COLUMN is_forbidden; \
         "
        cur.execute(q)

        q = "UPDATE change \
         SET is_forbidden=0; \
         "
        cur.execute(q)
        logger.info("added col `is_forbidden`")

    except Exception as e:
        pass

    q = "UPDATE change  \
    SET is_forbidden='t' \
    WHERE file_id='{}';".format(
        file_id
    )

    cur.execute(q)
