import logging
from typing import Optional

import pandas as pd
from typing_extensions import TypeGuard

logger = logging.getLogger(__name__)


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
    return x is not None
