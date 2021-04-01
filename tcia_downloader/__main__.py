#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import shutil
from argparse import ArgumentParser, Namespace
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional
from zipfile import ZipFile

import pydicom
from tqdm import tqdm

from .tcia import TCIAClient


tcia_client = TCIAClient(baseUrl="https://services.cancerimagingarchive.net/services/v4", resource="TCIA")


def download_image(series: str, dest: str) -> bool:
    return tcia_client.get_image(series, dest, series + ".zip")


def download_collection(args: Namespace):
    cols = [x["Collection"] for x in json.loads(tcia_client.get_collection_values().read())]
    if args.collection not in cols:
        raise ValueError(f"Invalid collection {cols}. Valid choices: \n{cols}")
    raise NotImplementedError(
        "Downloading from collection by name is not yet supported. " "Please supply a manifest file"
    )


def download_manifest(args: Namespace) -> Dict[str, bool]:
    manifest = Path(args.collection)
    dest = Path(args.dest)
    dest.mkdir(exist_ok=True)

    # copy manifest to dest
    shutil.copy(manifest, Path(dest, "manifest.tcia"))

    # get series to download from manifest
    with open(manifest, "r") as f:
        for line in f:
            if "ListOfSeries" in line:
                break
        else:
            raise RuntimeError(f"Couldn't find series list in {manifest}")
        series_list = [series.strip() for series in f]

    if args.limit:
        series_list = series_list[: args.limit]

    # queue up download jobs and get futures
    bar = tqdm(series_list, desc="Downloading")
    futures = []
    with ThreadPoolExecutor(args.jobs) as tp:
        for series in series_list:
            zipfile = Path(dest, f"{series}.zip")
            if not zipfile.is_file():
                _ = tp.submit(download_image, series, dest)
                _.add_done_callback(lambda x: bar.update(1))
                futures.append(_)
            else:
                bar.update(1)

    # wait for downloads to finish, track failed jobs
    failed_jobs = []
    result = {k: True for k in series_list}
    for i, future in enumerate(futures):
        while not future.done():
            pass
        if not future.result():
            failed_jobs.append(series_list[i])
            result[series_list[i]] = False
    bar.close()

    if failed_jobs:
        print("The following jobs failed to download:")
        for job in failed_jobs:
            print(job)

    return result


def create_case_subdir(dcm_path: Path, dest: Path) -> Optional[Path]:
    with pydicom.dcmread(dcm_path, stop_before_pixels=True) as dcm:
        try:
            study = dcm.StudyInstanceUID
        except Exception:
            return

    if not study:
        return
    subdir = Path(dest, study)
    subdir.mkdir(exist_ok=True)
    new_file = Path(subdir, dcm_path.name)
    dcm_path.rename(new_file)
    return new_file


def rename_to_series(dcm_path: Path) -> Optional[Path]:
    with pydicom.dcmread(dcm_path, stop_before_pixels=True) as dcm:
        try:
            series = dcm.SeriesInstanceUID
        except Exception:
            return

    if not series:
        return
    new_file = Path(dcm_path.parent, f"{series}.dcm")
    dcm_path.rename(new_file)
    return new_file


def unpack(path: Path) -> List[Path]:
    dest = Path(path, "unpacked")
    zipfiles = list(path.glob("*.zip"))
    for f in tqdm(zipfiles, desc="Unpacking"):
        with ZipFile(f, "r") as zipf:
            zipf.extractall(dest)

    # glob without dcm extension in case files are extensionless
    # exception will catch non-dicom files
    unpacked_files = list(dest.glob("*"))
    result = []
    for f in tqdm(unpacked_files, desc="Creating case directories"):
        try:
            _ = create_case_subdir(f, dest)
            _ = rename_to_series(_ if _ else f)
            result.append(_)
        except Exception:
            pass
    return result


def main(args: Namespace):
    if ".tcia" in args.collection.lower():
        download_manifest(args)
    else:
        download_collection(args)

    if args.unpack:
        unpack(Path(args.dest))


def parse_args() -> Namespace:
    parser = ArgumentParser("tcia_downloader")
    parser.add_argument("collection", type=str, help="TCIA manifest file or collection name to download")
    parser.add_argument("dest", type=str, help="Destination filepath")
    parser.add_argument(
        "-l", "--limit", type=int, default=None, help="Only download the first LIMIT files (for testing)"
    )
    parser.add_argument("-j", "--jobs", type=int, default=4, help="Number of parallel download jobs")
    parser.add_argument("-u", "--unpack", default=False, action="store_true", help="Unpack zip files after downloading")
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
