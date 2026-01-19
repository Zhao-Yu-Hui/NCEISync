from typing import Optional
import asyncio

import aiosqlite

async def curl(
        url: str,
        save_path: Optional[str] = None,
        timeout: int = 10,
        retry_limit: int = 3,
        db_path: Optional[str] = None,
) -> tuple[int, str, str]:
    cmd = (f"curl"
           f" -m {timeout}"
           f" --retry {retry_limit}"
           f" {'' if save_path is None else ('-o ' + save_path)}"
           f" {url}"
           )

    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if db_path is not None:
        async with aiosqlite.connect(db_path) as conn:
            cur = await conn.cursor()
            await cur.execute(
                "INSERT OR REPLACE INTO curl (url, stdout, stderr) VALUES (?, ?, ?);",
                (url, stdout, stderr,)
            )
            await conn.commit()


    return proc.returncode, stdout.decode(), stderr.decode()

__all__ = ["curl"]