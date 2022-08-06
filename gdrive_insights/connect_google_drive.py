"""connect_google_drive.py.

I use this file to connect to Google Drive API, list some files and run some requests interactively

Based on:
    https://developers.google.com/drive/api/quickstart/python

Steps I did:
    - I enabled `Google Drive API` for my GCP project `G Suite Testing`

See Google Drive API endpoints:
    https://developers.google.com/drive/api/v3/reference?apix=true

"""

from __future__ import print_function

from typing import Any, Dict, Optional

import pandas as pd
from googleapiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools

SCOPES = "https://www.googleapis.com/auth/drive.readonly.metadata"
store = file.Storage("storage.json")
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets("client_id.json", SCOPES)
    creds = tools.run_flow(flow, store)
DRIVE = discovery.build("drive", "v3", http=creds.authorize(Http()))

files = DRIVE.files().list().execute().get("files", [])
for f in files[:10]:
    print(f'{f["mimeType"]:<50} {f["name"]}')

# detailed python help at: https://developers.google.com/drive/api/guides/manage-changes#python
startPageToken = DRIVE.changes().getStartPageToken().execute()
pageToken = startPageToken["startPageToken"]

# for changes see `display_google_drive_changes.py`

# todo: get parents of a file, removed in v3?
# DRIVE.parents().list().execute()
