import asyncio
import sqlite3

from .db import prepare_tables
from .doidownloader import DOIDownloader, retrieve_fulltexts

if __name__ == "__main__":
    with open("dois.txt") as fh:
        dois = [line.strip() for line in fh]
    con = sqlite3.connect("asynciotest.db")
    prepare_tables(con)

    client = DOIDownloader()

    asyncio.run(retrieve_fulltexts(dois, con, client))
