"""models.py.

using:
    https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html 

frequently used queries:
    select * from revisions_by_file limit 15;
    select 
        left(file_name, 40) AS tr_file_name,
        left(file_path, 40) AS tr_file_path,
        nrevision,
        file_id
    from revisions_by_file where file_type = 'application/pdf' limit 15;


"""

import argparse
import asyncio
import logging
import subprocess
import uuid
from datetime import datetime
from typing import Optional

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
from sqlalchemy.schema import Table

LOG_FMT = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-16s - %(levelname)-7s - %(message)s"  # title

Base = declarative_base()


psql = load_config(db_name="gdrive", cfg_file="postgres.cfg", config_dir=config_dir)
psession = get_session(psql)()


file_session_association = Table(
    "file_session_association",
    Base.metadata,
    Column("file_session_id", Integer, ForeignKey("file_session.id")),
    Column("file_id", String, ForeignKey("file.id")),
    UniqueConstraint("file_session_id", "file_id"),
)


class fileSession(Base):
    """Save metadata of recently opened files."""

    __tablename__ = "file_session"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)

    files = relationship(
        "File", uselist=True, secondary=file_session_association, lazy="selectin"
    )

    nused = Column(Integer, nullable=False, default=0)

    created = Column(DateTime, server_default=func.now())  # current_timestamp()
    updated = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # add this so that it can be accessed
    __mapper_args__ = {"eager_defaults": True}

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "fileSession(id={}, name={}, nused={}, nfile={})".format(
            self.id, self.name, self.nused, len(self.files)
        )


class pageToken(Base):
    """Represent a pageToken, used when calling Google Drive API.

    See:
        https://developers.google.com/drive/api/v3/reference/revisions/list
    """

    __tablename__ = "page_token"
    id = Column(Integer, primary_key=True)
    table = Column(String, nullable=False)
    value = Column(String, nullable=False)

    created = Column(DateTime, server_default=func.now())  # current_timestamp()
    updated = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # add this so that it can be accessed
    __mapper_args__ = {"eager_defaults": True}

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "pageToken(id={}, table={}, value={})".format(
            self.id, self.table, self.value
        )


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
        if self.updated is not None:
            res = timeago.format(self.updated, datetime.utcnow())

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

    did_inspect = Column(Boolean, default=False, nullable=False)
    book_id = Column(Integer, nullable=True)

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

    def path_with(self, sfx: Optional[str] = None) -> str:
        if sfx is None:
            return self.path

        return sfx + self.path

    def open_pdf(self, sfx: Optional[str] = None):
        """Open pdf file using your favourite pdf viewer."""
        full_path = self.path_with(sfx)
        cmd = "atril {}".format(full_path)
        # result = subprocess.run([sys.executable, "-c", "print('ocean')"])
        result = subprocess.run(["atril", cmd])

    @classmethod
    def open_pdfs(cls):
        pass

    # def escape_file_path(self) -> Optional[str]:
    #     """Espace file path.

    #     To open files in linux
    #     """
    #     if self.path is None:
    #         return None

    #     return self.path.replace(" ", "/\ ").replace("(", "/(").replace(")", "/)")


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
