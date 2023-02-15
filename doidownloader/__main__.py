import sqlite3
import asyncio
from .doidownloader import DOIDownloader, retrieve_metadata

if __name__ == "__main__":
    with open("dois.txt") as fh:
        dois = [line.strip() for line in fh]
    con = sqlite3.connect("asynciotest.db")
    client = DOIDownloader()

    asyncio.run(retrieve_metadata(dois, con, client))
    # asyncio.run(save_fulltext(con, client))