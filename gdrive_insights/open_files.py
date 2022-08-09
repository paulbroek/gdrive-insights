"""open_files.py.

Open last session of files
"""
import logging

import psycopg2  # type: ignore[import]
from gdrive_insights import config as config_dir
from gdrive_insights.db.helpers import get_pdfs, open_pdfs
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


# construct file path if missing

# interactively select files, by typing indices

# save last session to db


if __name__ == "__main__":
    open_pdfs(get_pdfs(con, n=3), pfx='/home/paul/gdrive', ctxmgr=False)
