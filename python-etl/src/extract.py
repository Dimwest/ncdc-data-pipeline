import requests
import shutil
from src.log import logger, with_logging
from os import remove
from multiprocessing import Pool
from zipfile import ZipFile
from typing import List


def download_zip_file(url: str) -> str:

    """
    Streams remote ZIP file to disk without using excessive memory, extract its content,
    then deletes it.

    :param url: remote file URL

    :return: extracted file name
    """

    local_filename = f"tmp/{url.split('/')[-1]}"

    # Downloading ZIP file
    logger.info(
        f"Downloading ZIP file from {url} into local file '{local_filename}' ..."
    )
    with requests.get(url, stream=True) as r, open(local_filename, "wb") as f:
        shutil.copyfileobj(r.raw, f)
    logger.info(
        f"Download of ZIP data from {url} into local file '{local_filename}' successfull !"
    )

    # Extracting ZIP file data
    logger.info(f"Extracting ZIP data from {local_filename} ...")
    with ZipFile(local_filename) as zf:
        zf.extractall()
    logger.info(f"Extraction of ZIP data from {local_filename} successfull !")

    # Removing ZIP file after extraction and returning the extracted filename
    remove(local_filename)
    return local_filename.split(".zip")[0]


@with_logging
def download_zip_files(urls: List[str]) -> List[str]:

    """
    Parallelizes download_zip_file function.

    :param urls: list of remote files URLs

    :return: list of extracted files names
    """

    pool = Pool(processes=len(urls))
    names = pool.map(download_zip_file, urls)
    return names
