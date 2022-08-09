"""open_files.py.

Open last session of files
"""
import argparse
import logging
import sys
from enum import Enum

import psycopg2  # type: ignore[import]
from gdrive_insights import config as config_dir
from gdrive_insights.db.helpers import (get_pdfs, get_pdfs_manual,
                                        get_sessions, open_pdfs)
from gdrive_insights.db.models import fileSession
from rarc_utils.log import setup_logger
from rarc_utils.sqlalchemy_base import load_config

psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)

log_fmt = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-16s - %(levelname)-7s - %(message)s"  # name
logger = setup_logger(
    cmdLevel=logging.INFO, saveFile=0, savePandas=0, jsonLogger=0, color=1, fmt=log_fmt
)
con = psycopg2.connect(
    database=psql.db, user=psql.user, password=psql.passwd, host=psql.host, port="5432"
)


class programMode(Enum):
    FILE = 0
    SESSION = 1


# construct file path if missing

# interactively select files, by typing indices

# save last session to db, if exists, update existing one

# or use Typer?


CLI = argparse.ArgumentParser()
CLI.add_argument(
    "-m",
    "--mode",
    type=str,
    default=programMode.FILE.name,
    help="mode to use, file or session mode \nfile mode (0, default) allows to select individual files, \nsession mode (1) allows to select recent sessions",
)
CLI.add_argument(
    "-n",
    type=int,
    default=0,
    help="open max n files",
)
CLI.add_argument(
    "-d",
    "--dryrun",
    action="store_true",
    default=False,
    help="import modules, do not open files",
)

if __name__ == "__main__":

    args = CLI.parse_args()

    mode_input = args.mode.upper()
    msg = f"{mode_input=}, not in {list(programMode.__members__.keys())}"
    assert mode_input in programMode.__members__, msg

    mode = programMode[mode_input]

    if args.dryrun:
        sys.exit()

    if mode == programMode.FILE:
        # pdfs = get_pdfs(con, n=args.n)
        pdfs = get_pdfs_manual(con, n=25)

    elif mode == programMode.SESSION:
        sessions = get_sessions(con, n=10)

        # let user select session

    else:
        raise Exception(f"Invalid programMode: {msg}")

    open_pdfs(pdfs, pfx="/home/paul/gdrive", ctxmgr=False)
