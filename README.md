# gdrive-insights

Show your recent Google Drive file revisions in a streamlit dashboard. Which files do you use a lot?

## 0. Config

### 0.1 
gdrive-insights can run without interacting with the file system, but to open files directly through the package mount Google Drive to the file system using `rclone` first:


## 1. Install

### 1.1 Oauth

See [this tutorial](https://codelabs.developers.google.com/codelabs/gsuite-apis-intro/#0) to get your OAuth 2.0 tokens

```bash

# Get client Id from Google Drive API
# See tutorial: https://codelabs.developers.google.com/codelabs/gsuite-apis-intro/#0

# mv client_id.json to repo dir
mv client_id.json ~/repos/gdrive-insights/gdrive_insights/client_id.json
# initialize oauth
python connect_google_drive.py
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
pip install -U ~/repos/gdrive-insights
cp ~/repos/gdrive-insights/gdrive_insights/config/postgres.cfg /home/paul/anaconda3/envs/py39/lib/python3.9/site-packages/gdrive_insights/config
```

Initialize all models in database:

```bash
cd ~/repos/gdrive-insights/gdrive_insights/db
ipy models.py -i -- --create 1
```

### 2.1 How to run

```bash
# open files by saved session
ipy open_files.py -i -- -m sesssion
# open files by user input, save to session
ipy open_files.py -i -- -m manual
```

### 3.1 To-do

-   [ ] Open frequently changed files directly from command line
-   [ ] Get full file path with [Parents endpoints](https://developers.google.com/drive/api/v2/reference/parents/get)
