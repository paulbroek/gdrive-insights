"""types.py, define types for google-drive-insights."""

from typing import Any, Dict, NewType, Union  # , Type

FileId = NewType("FileId", str)
ChangeId = NewType("ChangeId", str)
RevisionId = NewType("RevisionId", str)

File = NewType("File", dict)
Change = NewType("Change", dict)
Revision = NewType("Revision", dict)

TableTypes = Union[File, Change, Revision]

Record = NewType("Record", Dict[str, Any])

FileRec = NewType("FileRec", Record)
ChangeRec = NewType("ChangeRec", Record)
RevisionRec = NewType("RevisionRec", Record)
