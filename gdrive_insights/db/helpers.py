"""helpers.py, helper methods for SQLAlchemy models, listed in models.py."""

import logging
import sys
from datetime import datetime
from multiprocessing import Pool
from subprocess import Popen
from typing import Any, Dict, List, Optional, Tuple

import gdrive_insights.config as config_dir
import pandas as pd
from googleapiclient import discovery  # type: ignore[import]
from rarc_utils.decorators import items_per_sec
from rarc_utils.sqlalchemy_base import create_many, get_session, load_config
from sqlalchemy import and_
from sqlalchemy.future import select  # type: ignore[import]
from tqdm import tqdm  # type: ignore[import]

from ..core.utils import create_gdrive, is_not_none
from .models import File, fileSession, pageToken

psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)
psession = get_session(psql)()

logger = logging.getLogger(__name__)

# use tqdm with df.progress_map()
tqdm.pandas()


async def create_many_items(asession, *args, **kwargs):
    """Create many SQLAlchemy model items in db."""
    async with asession() as session:
        items = await create_many(session, *args, **kwargs)

    return items


def update_is_forbidden(file_id: str) -> None:
    """Update File.is_forbidden.

    Remedy to deal with `Encountered 403 Forbidden with reason "insufficientFilePermissions"`
    messages from Google Drive API
    """
    file = psession.query(File).filter(File.id == file_id).one_or_none()
    assert file is not None, "create file first"
    file.is_forbidden = True
    psession.commit()


def construct_file_path(
    fileId: str,
    drive: Optional[discovery.Resource] = None,
    fullPath="",
    fileName: Optional[str] = None,
) -> str:
    """Construct file path.

    Repeatedly calls
        GET https://www.googleapis.com/drive/v3/files/[FileId]?fields=parents
    till root node is found.

    fileName:   optionally pass fileName to check if path name has fileName in it
    """
    if drive is None:
        drive = create_gdrive()

    parent = drive.files().get(fileId=fileId, fields="parents").execute()
    name = drive.files().get(fileId=fileId, fields="name").execute().get("name", None)
    parent_id: Optional[str] = parent.get("parents", None)
    parent_id = parent_id[0] if parent_id is not None else None

    if parent_id is not None:
        logger.debug(f"{parent_id=:<40} {name=:<50} ")
        fullPath = "/" + name + fullPath
        return construct_file_path(
            parent_id, drive, fileName=fileName, fullPath=fullPath
        )

    if fileName is not None:
        logger.info(f"{fileName=:<40} {fullPath=}")
    else:
        logger.info(f"{fullPath=}")

    fullPathOut: str = fullPath

    return fullPathOut


@items_per_sec
def construct_file_path_in_parallel(
    nprocess: int, drive: discovery.Resource, fileIds: List[str]
) -> List[Dict[str, Any]]:
    """Construct file path in parallel."""
    pool = Pool(processes=nprocess)

    # todo: google throws `dailyLimitExceededUnreg` error
    # more help: https://cloud.google.com/docs/quota#capping_usage
    lres = []
    total: int = 0
    logger.info(f"{nprocess=}")
    # can this be rewritten using with ... ?
    # pconstruct_file_path = partial(construct_file_path, drive=drive)
    pconstruct_file_path = construct_file_path
    for i, x in enumerate(pool.imap_unordered(pconstruct_file_path, fileIds)):
        total += len(x)
        sys.stdout.write(f"Processed {i:,} line(s). Total processed: {total:,}\r")
        sys.stdout.flush()

        lres.append(x)

    return sum(lres, [])


def map_files_to_path(
    df: pd.DataFrame, drive: discovery.Resource, onlyMissing=False, nproc=4
) -> pd.DataFrame:
    """Call `construct_file_path` on all `id` rows."""
    # todo: implement `onlyMissing`
    df = df.copy()
    df["path"] = df["id"].progress_map(lambda x: construct_file_path(x, drive=drive))
    # fileIds = df["id"].to_list()
    # res = construct_file_path_in_parallel(nproc, drive, fileIds)

    # return res

    df["path"] = df[["id", "name"]].progress_apply(
        lambda row: construct_file_path(row["id"], fileName=row["name"], drive=drive),
        axis=1,
    )
    return df


def fetch_files_over_df(df: pd.DataFrame, idCol="id") -> pd.DataFrame:
    """Fetch files over all dataframe rows.

    Adds `file` col to df
    """
    assert idCol in df.columns, f"{idCol=} not in {df.columns=}"

    ids: List[str] = df[idCol].unique().tolist()
    files: List[File] = (
        psession.execute(select(File).filter(File.id.in_(ids))).scalars().fetchall()
    )
    files_by_id: Dict[str, File] = dict(zip((f.id for f in files), files))
    df["file"] = df[idCol].map(files_by_id)

    return df


def update_file_paths(df: pd.DataFrame) -> pd.DataFrame:
    """Update file paths in db for a dataframe of files."""
    assert "id" in df.columns
    assert "path" in df.columns

    # fetch files
    df = fetch_files_over_df(df)
    nmissing: int = df["file"].isna().sum()
    logger.info(f"{nmissing=:,}")

    # update file_paths
    file_and_path: List[Tuple[File, str]] = (
        df[["file", "path"]].to_records(index=False).tolist()
    )
    for file, path in file_and_path:
        file.path = path

    nupdated: int = len(file_and_path)
    logger.info(f"{nupdated=:,}")

    psession.commit()

    return df


def get_page_tokens(con, n=2) -> pd.DataFrame:
    """Get page_token from db."""
    query = """
    SELECT id, "table", value::int AS val_int, created, updated FROM page_token ORDER BY val_int DESC LIMIT {};
    """.format(
        n
    )
    df: pd.DataFrame = pd.read_sql(query, con)

    return df


def get_or_update_page_token(table: str, value: str) -> None:
    """Get or update pageToken from db."""
    assert table is not None
    assert value is not None
    pt = (
        psession.execute(
            select(pageToken).where(
                and_(pageToken.table == table, pageToken.value == value)
            )
        )
        .scalars()
        .one_or_none()
    )
    if pt is None:
        pt = pageToken(table=table, value=value)
        psession.add(pt)

    pt.updated = datetime.utcnow()
    psession.commit()


def get_sessions(con, n=8) -> pd.DataFrame:
    """Get fileSessions from db."""
    query = """
    SELECT * FROM file_session LIMIT {};
    """.format(
        n
    )
    df: pd.DataFrame = pd.read_sql(query, con)

    return df


def get_session_by_input(n=20) -> Optional[fileSession]:
    """Get file_session by user input."""
    psession.execute("REFRESH MATERIALIZED VIEW vw_file_sessions;")
    query = """SELECT * FROM vw_file_sessions LIMIT {};""".format(n)
    res = psession.execute(query).fetchall()
    df = pd.DataFrame(res)

    fs: Optional[fileSession] = None

    if not df.empty:
        df = df.set_index("sid")
        input_ = input(f"{df.to_string()}\n\nselect index of session to use: ")

        fs_ix = int(input_)
        fs = (
            psession.execute(select(fileSession).filter(fileSession.id == fs_ix))
            .scalars()
            .one_or_none()
        )
        if fs is not None:
            logger.info(f"found a session match, {fs=}")

    return fs


def get_session_by_file_ids(file_ids: List[str]) -> Optional[fileSession]:
    """Find session match.

    Query association table on union of session_id and file_ids,
    if non-empty, return the match
    """
    fmt_ids: str = "'{0}'".format("', '".join(file_ids))
    query = """SELECT * FROM file_session_association WHERE file_id IN ({});""".format(
        fmt_ids
    )
    res = psession.execute(query).fetchall()
    df = pd.DataFrame(res)

    fs: Optional[fileSession] = None
    view: pd.DataFrame = pd.DataFrame()

    if not df.empty:
        sdf: pd.DataFrame = (
            df.groupby("file_session_id")["file_id"].count().to_frame("nfile")
        )
        view = sdf[sdf.nfile == len(file_ids)]

    if not view.empty:
        fs_ix: int = int(view.index[0])
        fs = (
            psession.execute(select(fileSession).filter(fileSession.id == fs_ix))
            .scalars()
            .one_or_none()
        )
        if fs is not None:
            logger.info(f"found a session match, {fs=}")

    return fs


def get_pdfs(con, file_ids: Optional[List[str]] = None, n=5) -> pd.DataFrame:
    """Open frequently opened pdf files."""
    con.cursor().execute("REFRESH MATERIALIZED VIEW revisions_by_file;")
    query = """
    SELECT * from revisions_by_file where file_type = 'application/pdf'
    """
    if file_ids is not None:
        fmt_ids: str = "'{0}'".format("', '".join(file_ids))
        query += """ AND file_id IN ({}) """.format(fmt_ids)

    query += """LIMIT {}""".format(n)
    logger.debug(f"{query=}")

    df: pd.DataFrame = pd.read_sql(query, con)

    return df


def get_file_ids_of_session(fs_id: int) -> List[str]:
    """Get pdfs by file_session."""
    query = """SELECT file_id FROM file_session_association WHERE file_session_id = {};""".format(
        fs_id
    )
    res = psession.execute(query).scalars().fetchall()

    return list(res)


def get_pdfs_manual(con, n=30) -> pd.DataFrame:
    """Get pdfs by manually selecting which items to keep."""
    df = get_pdfs(con, n=n)

    view = df[["file_name", "last_update", "nrevision", "file_id"]].copy()
    input_ = input(
        f"{view.to_string()}\n\nselect indices of rows to keep, separated by spaces: "
    )

    rows: List[Optional[int]] = list(
        map(lambda x: int(x) if len(x) > 0 else None, input_.split(" "))
    )
    ixs: List[int] = list(filter(is_not_none, rows))

    res: pd.DataFrame = df.iloc[ixs].sort_index()
    return res


def popen_file(cmd_args):
    """Open file using context manager."""
    # todo: opening with contextmanager makes the files harder to close by pressing Ctr+C
    with Popen(cmd_args) as p:
        print(f"run: {cmd_args}")
        # output = p.stdout.read()
        p.wait()
        # return output


def open_pdfs(df: pd.DataFrame, fs=None, pfx=None, ctxmgr=False) -> None:
    """Open pdf files with pdf viewer.

    Usage:
        open_pdfs(get_pdfs(con, n=5), pfx='/home/paul/gdrive')

    Also save default settings in Evince, by clicking:
        Save Current Settings as Default

    Manually through command line is also available:
        gsettings set org.mate.Atril.Default continuous false
    """
    assert pfx is not None
    df["file_path"] = pfx + df["file_path"]
    print(f"{df.shape=}")

    # base_command = "atril"
    base_command = "evince"

    paths = df["file_path"].to_list()
    # print(f"{paths[:2]=}")

    # fetch file metadata
    df = fetch_files_over_df(df, idCol="file_id")

    # find matching file_session
    if fs is None:
        fs = get_session_by_file_ids(df.file_id.to_list())

    # create new file_session
    if fs is None:
        logger.info(f"creating new fileSession")
        fs = fileSession(files=df.file.to_list())
        psession.add(fs)
        psession.commit()

    fs.nused += 1
    psession.commit()
    psession.close()

    if ctxmgr:
        for c in paths:
            popen_file([base_command, c])

    else:
        procs = [Popen([base_command, i]) for i in paths]
        for p in procs:
            p.wait()
