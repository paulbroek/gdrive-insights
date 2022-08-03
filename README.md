# google-drive-insights

Show your recent Google Drive file revisions in a streamlit dashboard. Which files do you use a lot?

## 1. Install

### 1.1 Oauth

See [this tutorial](https://codelabs.developers.google.com/codelabs/gsuite-apis-intro/#0) to get your OAuth 2.0 tokens

```bash

# Get client Id from Google Drive API
# See tutorial: https://codelabs.developers.google.com/codelabs/gsuite-apis-intro/#0
```

### 1.2 PostgreSQL configuration

Add `postgres.cfg` file to `config` dir, with contents:

```bash
[general]
uvloop      = True

[psql]
host        = 192.168.178.XX
user        = postgres
passwd      = PASSWORD
db          = gdrive
```

And copy the file to anaconda `config` dir after installing

```bash
pip install -U ~/repos/google-drive-insights
cp ~/repos/google-drive-insights/google_drive_insights/config/postgres.cfg /home/paul/anaconda3/envs/py39/lib/python3.9/site-packages/google_drive_insights/config
```

Initialize all models in database:

```bash
cd ~/repos/google-drive-insights/google_drive_insights/db
ipy models.py -i -- --create 1
```

### 2.1 To-do

-   [ ] Open frequently changed files directly from command line
-   [ ] Get full file path with [Parents endpoints](https://developers.google.com/drive/api/v2/reference/parents/get)
