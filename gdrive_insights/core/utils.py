"""utils.py."""
import logging
from typing import Optional

import pandas as pd
from googleapiclient.discovery import Resource, build  # type: ignore[import]
from oauth2client import client, file, tools  # type: ignore[import]
from typing_extensions import TypeGuard

from ..settings import CLIENT_ID_JSON_FILE, STORAGE_JSON_FILE

logger = logging.getLogger(__name__)

SCOPES = "https://www.googleapis.com/auth/drive.readonly.metadata"


def unnest_col(
    df: pd.DataFrame,
    pfxCol: Optional[str] = None,
    renameColAs: Optional[str] = None,
    dropFileCol=True,
) -> pd.DataFrame:
    assert not df.empty
    assert pfxCol is not None
    df = df.copy()
    # find first row with non nane item
    sel: pd.Series = df[pfxCol].dropna()
    if sel.empty:
        logger.warning(f"no none value found for {pfxCol=}")
        return df

    kys = sel.values[0].keys()

    renameAs = renameColAs or pfxCol
    newKeys = [(k, f"{renameAs}_{k}") for k in kys]
    for (k, nk) in newKeys:
        df[nk] = df[pfxCol].map(lambda x: x.get(k) if x is not None else None)

    if dropFileCol:
        del df[pfxCol]

    return df


def is_not_none(x: Optional[int]) -> TypeGuard[int]:
    """Return Int is not None."""
    return x is not None


def create_gdrive() -> Resource:
    """Create Google Drive API connector."""
    store = file.Storage(STORAGE_JSON_FILE)
    creds = store.get()

    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_ID_JSON_FILE, SCOPES)
        creds = tools.run_flow(flow, store)

    DRIVE = build("drive", "v3", credentials=creds)

    return DRIVE
