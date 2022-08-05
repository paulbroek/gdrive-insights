"""models.py.

using:
    https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html 
"""

import argparse
import asyncio
import logging
import uuid
from datetime import datetime

import timeago  # type: ignore[import]
from gdrive_insights import config as config_dir
from rarc_utils.log import loggingLevelNames, set_log_level, setup_logger
from rarc_utils.sqlalchemy_base import (async_main, get_async_session,
                                        get_session, load_config)
from sqlalchemy import (Boolean, Column, DateTime, ForeignKey, Integer, String,
                        UniqueConstraint, func)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

LOG_FMT = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-16s - %(levelname)-7s - %(message)s"  # title

Base = declarative_base()


psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)
psession = get_session(psql)()


class Revision(Base):
    """Represent a revision for a user or shared drive.

    See:
        https://developers.google.com/drive/api/v3/reference/revisions
    """

    __tablename__ = "revision"
    id = Column(String, primary_key=True)
    modifiedTime = Column(DateTime, nullable=False)
    mimeType = Column(String, nullable=False)

    file_id = Column(String, ForeignKey("file.id"), nullable=False)
    file = relationship("File", uselist=False, lazy="selectin")

    created = Column(DateTime, server_default=func.now())  # current_timestamp()
    updated = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # add this so that it can be accessed
    __mapper_args__ = {"eager_defaults": True}

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "Revision(id={}, mimeType={}, modifiedAgo={})".format(
            self.id, self.mimeType, self.modifiedAgo()
        )

    def modifiedAgo(self) -> str:
        res: str = ""
        if self.last_scrape is not None:
            res = timeago.format(self.last_scrape, datetime.utcnow())

        return res


class File(Base):
    """Represent a change for a user or shared drive.

    See:
        https://developers.google.com/drive/api/v3/reference/files
    """

    __tablename__ = "file"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    mimeType = Column(String, nullable=False)
    # resourceKey = Column(String)

    created = Column(DateTime, server_default=func.now())  # current_timestamp()
    updated = Column(DateTime, server_default=func.now(), onupdate=func.now())
    path = Column(String)

    is_forbidden = Column(Boolean, default=False, nullable=False)

    # add this so that it can be accessed
    __mapper_args__ = {"eager_defaults": True}

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "File(id={}, mimeType={}, name={}, created={})".format(
            self.id,
            self.mimeType,
            self.name,
            self.created,
        )


class Change(Base):
    """Represent a change for a user or shared drive.

    See:
        https://developers.google.com/drive/api/v3/reference/changes/list
    """

    __tablename__ = "change"
    # id = Column(Integer, primary_key=True)
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        unique=True,
        nullable=False,
        default=uuid.uuid4,
    )

    removed = Column(Boolean)
    time = Column(DateTime)
    type = Column(String)
    changeType = Column(String)
    page_token = Column(Integer)

    file_id = Column(String, ForeignKey("file.id"), nullable=False)
    file = relationship("File", uselist=False, lazy="selectin")

    is_forbidden = Column(Boolean)

    created = Column(DateTime, server_default=func.now())  # current_timestamp()
    updated = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # add this so that it can be accessed
    __mapper_args__ = {"eager_defaults": True}

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return (
            "Change(removed={}, time={}, type={}, page_token={}, file_name={})".format(
                self.removed,
                self.time.name,
                self.type,
                self.page_token,
                self.file.name,
            )
        )


CLI = argparse.ArgumentParser()
CLI.add_argument(
    "-v",
    "--verbosity",
    type=str,
    default="info",
    help=f"choose debug log level: {', '.join(loggingLevelNames())}",
)
CLI.add_argument(
    "--create",
    type=int,
    default=0,
    help="create new models (1), or use existing ones (0)",
)
CLI.add_argument(
    "-f",
    "--force",
    action="store_true",
    default=False,
    help="don't ask for model creation confirmation. \
        caution: deletes all existing models",
)

if __name__ == "__main__":

    args = CLI.parse_args()

    async_session = get_async_session(psql)
    # async_db = get_async_db(psql)()

    loop = asyncio.new_event_loop()

    log_level = args.verbosity.upper()

    logger = setup_logger(
        cmdLevel=logging.DEBUG, saveFile=0, savePandas=0, color=1, fmt=LOG_FMT
    )
    set_log_level(logger, level=log_level, fmt=LOG_FMT)

    if args.create:
        print("create models")
        loop.run_until_complete(
            async_main(psql, base=Base, force=args.force, dropFirst=True)
        )

        # print('create data')
        # items = loop.run_until_complete(create_initial_items(async_session))
        # loop.run_until_complete(create_initial_items(async_session))
    else:
        print("get data")
        # data = loop.run_until_complete(get_data2(psql))

    # strMappings = loop.run_until_complete(aget_str_mappings(psql))
