import asyncio
import sqlite3

from .db import prepare_tables
from .doidownloader import DOIDownloader, retrieve_fulltexts


async def store_fulltexts(dois: list[str], con: sqlite3.Connection) -> None:
    async with DOIDownloader() as client:
        await retrieve_fulltexts(dois, con, client)


if __name__ == "__main__":
    with open("dois.txt") as fh:
        dois = [line.strip() for line in fh]
    con = sqlite3.connect("asynciotest.db")
    prepare_tables(con)

    asyncio.run(store_fulltexts(dois, con))
