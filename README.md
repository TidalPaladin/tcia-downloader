# TCIA Downloader

Basic script to download and unpack files from a [TCIA](https://wiki.cancerimagingarchive.net/display/Public/Collections) manifest.
This script has only been tested with the [BCS-DBT](https://wiki.cancerimagingarchive.net/pages/viewpage.action?pageId=64685580) dataset.

## Usage

1. Run `make venv` to install the script to a virtual environment
2. Run the script with `venv/bin/python -m tcia_downloader MANIFEST.tcia DEST_DIR`

Full list of options:

```
usage: tcia_downloader [-h] [-j JOBS] [-u] collection dest

positional arguments:
  collection            TCIA manifest file or collection name to download
  dest                  Destination filepath

optional arguments:
  -h, --help            show this help message and exit
  -j JOBS, --jobs JOBS  Number of parallel download jobs
  -u, --unpack          Unpack zip files after downloading
```
