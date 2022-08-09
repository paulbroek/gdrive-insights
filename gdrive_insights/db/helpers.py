"""helpers.py, helper methods for SQLAlchemy models, listed in models.py."""

import logging
from subprocess import Popen

import gdrive_insights.config as config_dir
import pandas as pd
from googleapiclient import discovery  # type: ignore[import]
from rarc_utils.sqlalchemy_base import create_many, get_session, load_config
from sqlalchemy.future import select  # type: ignore[import]

from .models import File

psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)
psession = get_session(psql)()

logger = logging.getLogger(__name__)


async def create_many_items(asession, *args, **kwargs):
    """Create many SQLAlchemy model items in db."""
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


def construct_file_path(
    fileId: str, drive: discovery.Resource, fullPath="", fileName=None, debug=False
) -> str:
    """Construct file path.

    Repeatedly calls
        GET https://www.googleapis.com/drive/v3/files/[FileId]?fields=parents
    till root node is found.

    fileName:   optionally pass fileName to check if path name has fileName in it

    Reconstructs a file path
    """
    parent = drive.files().get(fileId=fileId, fields="parents").execute()
    name = drive.files().get(fileId=fileId, fields="name").execute().get("name", None)
    parent_id = parent.get("parents", None)
    parent_id = parent_id[0] if parent_id is not None else None

    if parent_id is not None:
        if debug:
            print(f"{parent_id=:<40} {name=:<50} ")
        fullPath = "/" + name + fullPath
        return construct_file_path(
            parent_id, drive, fileName=fileName, fullPath=fullPath
        )

    if fileName is not None:
        print(f"{fileName=:<40} {fullPath=}")
    else:
        print(f"{fullPath=}")

    fullPathOut: str = fullPath

    return fullPathOut


def map_files_to_path(df: pd.DataFrame, drive: discovery.Resource) -> pd.DataFrame:
    """Call `construct_file_path` on all `id` rows."""
    df = df.copy()
    df["path"] = df["id"].map(lambda x: construct_file_path(x, drive=drive))
    # df["path"] = df[["id", "name"]].apply(
    #     lambda row: construct_file_path(row["id"], fileName=row["name"], drive=drive),
    #     axis=1,
    # )
    return df


def update_file_paths(df: pd.DataFrame) -> pd.DataFrame:
    """Update file paths in db for a dataframe of files."""
    assert "id" in df.columns
    assert "path" in df.columns

    # fetch files
    ids = df.id.unique()
    files = psession.execute(select(File).filter(File.id.in_(ids))).scalars().fetchall()
    files_by_id = dict(zip((f.id for f in files), files))
    df["file"] = df["id"].map(files_by_id)

    nmissing = df["file"].isna().sum()
    logger.info(f"{nmissing=:,}")

    # update file_paths
    file_and_path = df[["file", "path"]].to_records(index=False)
    for file, path in file_and_path:
        file.path = path

    nupdated = len(file_and_path)
    logger.info(f"{nupdated=:,}")

    psession.commit()

    return df


def get_pdfs(con, n=5) -> pd.DataFrame:
    """Open frequently opened pdf files."""
    q = """
    REFRESH MATERIALIZED VIEW revisions_by_file;
    select * from revisions_by_file where file_type = 'application/pdf' limit {};
    """.format(
        n
    )

    df: pd.DataFrame = pd.read_sql(q, con)

    return df


def open_pdf(cmd_args):

    # todo: opening with contextmanager makes the files harder to close by pressing Ctr+C
    with Popen(cmd_args) as p:
        print(f"run: {cmd_args}")
        # output = p.stdout.read() 
        p.wait()
        # return output


def open_pdfs(df: pd.DataFrame, pfx=None, ctxmgr=False) -> None:
    """Open pdf files with pdf viewer.

    Usage:
        open_pdfs(get_pdfs(con, n=5), pfx='/home/paul/gdrive')
    """
    assert pfx is not None
    df["file_path"] = pfx + df["file_path"]
    print(f"{df.shape=}")

    # base_command = "atril "
    base_command = "atril"
    # base_command = ""
    # commands = (base_command + df["file_path"].apply(lambda x: "'" + x + "'")).to_list()
    # paths = df["file_path"].apply(lambda x: "'" + x + "'").to_list()
    paths = df["file_path"].to_list()
    print(f"{paths[:2]=}")

    # procs = [Popen(i) for i in commands]

    if ctxmgr:
        for c in paths:
            open_pdf([base_command, c])

    else:
        procs = [Popen([base_command, i]) for i in paths]
        for p in procs:
            p.wait()
