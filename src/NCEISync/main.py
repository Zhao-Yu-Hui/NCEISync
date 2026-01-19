import os
import logging
from logging.handlers import RotatingFileHandler
import asyncio
import json
import argparse
import sqlite3
import itertools
import base64

import aiosqlite

from .curl import curl
from .ls import ls


async def batched_map(func, batches):
    rtn = []
    for batch in itertools.batched(batches, TASKS_LIMIT):
        rtn.append(await asyncio.gather(*(func(*args) for args in batch)))
    return tuple(itertools.chain(*rtn))


def get_save_filepath(_url):
    _url_relative = _url[len(URL):]
    _filepath = os.path.join(SAVE_DIR, *os.path.split(_url_relative))
    return str(_filepath)


async def valid_file(url: str) -> bool:
    filepath = get_save_filepath(url)
    async with aiosqlite.connect(DB_PATH) as _conn:
        cursor = await _conn.cursor()
        res = await cursor.execute("SELECT done, local_path FROM files WHERE url = ?", (url,))
        res = await res.fetchone()
        LOGGER.debug(f"res: {res}")

        if res is None:
            return False

        elif not res[0]:
            return False

        elif not (os.path.isfile(filepath) and filepath == res[1]):
            LOGGER.info(f"{url}: local file not found")
            await cursor.execute("UPDATE files SET done = ?, local_path = ? WHERE url = ?", (False, filepath, url,))
            await _conn.commit()
            return False

        else:
            return True


async def download_file(url: str) -> None:
    if await valid_file(url):
        LOGGER.info(f"{url}: skipped")
        return

    filepath = get_save_filepath(url)
    async with aiosqlite.connect(DB_PATH) as _conn:
        cursor = await _conn.cursor()
        await cursor.execute(
            "INSERT OR REPLACE INTO files (url, local_path, done) VALUES (?, ?, ?)",
            (url, filepath, False,)
        )
        await _conn.commit()

    errno, _, _ = await curl(url, filepath, TIMEOUT, RETRY_LIMIT, DB_PATH)
    if errno == 0:
        LOGGER.info(f"{url}: download succeed")
        async with aiosqlite.connect(DB_PATH) as _conn:
            cursor = await _conn.cursor()
            await cursor.execute(
                "INSERT OR REPLACE INTO files (url, local_path, done) VALUES (?, ?, ?)",
                (url, filepath, True,)
            )
            await _conn.commit()
    else:
        LOGGER.error(f"{url}: download failed")


async def download_dir(url: str) -> list[str]:
    LOGGER.info(f"{url}: dir download begin")
    os.makedirs(get_save_filepath(url), exist_ok=True)
    html_path = os.path.join(HTML_DIR,  base64.urlsafe_b64encode(url.encode("utf-8")).decode() + '.html')
    await curl(url, html_path, TIMEOUT, RETRY_LIMIT, DB_PATH)

    files, dirs = ls(html_path)
    LOGGER.info(f"{url}: found {len(files)} files in it.")

    await batched_map(download_file, ((url + _file,) for _file in files))

    LOGGER.info(f"{url}: file in this dir download done, found {len(dirs)} dirs in it.")

    return list(url + _dir for _dir in dirs)


async def download():
    dirs = await download_dir(URL)

    while len(dirs) > 0:
        rlt = await batched_map(download_dir, ((_dir,) for _dir in dirs))
        dirs = list(itertools.chain(*rlt))


def main():
    global URL, SAVE_DIR, RETRY_LIMIT, TIMEOUT, HTML_DIR, TASKS_LIMIT, DB_PATH, LOGGER

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="https://www.ncei.noaa.gov/pub/data/igra/")
    parser.add_argument("--save-dir", default="./", type=os.path.abspath)
    parser.add_argument("--log", default="./logs/", type=os.path.abspath)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--retry-limit", type=int, default=30)
    parser.add_argument("--timeout", default=100, type=int)
    args = parser.parse_args()

    if not args.url.endswith("/"):
        args.url += '/'

    URL = args.url
    SAVE_DIR = args.save_dir
    RETRY_LIMIT = args.retry_limit
    TIMEOUT = args.timeout
    LOG_DIR = args.log
    HTML_DIR = os.path.join(LOG_DIR, "html")
    TASKS_LIMIT = 16

    log_filepath = os.path.join(LOG_DIR, f"NCEISync.log")
    DB_PATH = os.path.join(LOG_DIR, f"NCEISync.db")

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(HTML_DIR, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS files
            (
                url TEXT NOT NULL PRIMARY KEY,
                local_path TEXT NOT NULL,
                done BOOLEAN NOT NULL
            )
            """
        )
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_files ON files (url)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS curl
            (
                url TEXT NOT NULL PRIMARY KEY,
                stdout TEXT NOT NULL,
                stderr TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_curl ON curl (url)")
        conn.commit()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = RotatingFileHandler(log_filepath, maxBytes=10 * 1024 * 1024, backupCount=100)
    file_handler.setFormatter(formatter)

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(getattr(logging, args.log_level.upper()))
    LOGGER.addHandler(stream_handler)
    LOGGER.addHandler(file_handler)

    LOGGER.info(f"begin with args: {json.dumps(vars(args), indent=2)}")

    asyncio.run(download())


if __name__ == "__main__":
    main()