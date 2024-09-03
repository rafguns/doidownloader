import asyncio
import sqlite3
import sys
from collections.abc import Iterator, MutableMapping
from pathlib import Path

import click

from .db import prepare_tables
from .doidownloader import DOIDownloader, save_fulltexts_from_dois


class CrawlDelays(MutableMapping):
    """Simple file-based cache for crawl delays"""

    def __init__(self, cache_filename: str) -> None:
        self.cache_filename = cache_filename
        self.delays = {}
        try:
            with open(self.cache_filename) as fh:
                for line in fh:
                    domain, delay = line.strip().split()
                    self.delays[domain] = int(delay)
        except FileNotFoundError:
            Path(cache_filename).touch()

    def __setitem__(self, key: str, value: int) -> None:
        with open(self.cache_filename, "a") as fh:
            fh.write(f"{key}\t{value}\n")
        self.delays[key] = value

    def __getitem__(self, key: str) -> int:
        return self.delays[key]

    def __contains__(self, key: str) -> bool:
        return key in self.delays

    def __delitem__(self, key: str) -> None:
        del self.delays[key]

    def __iter__(self) -> Iterator:
        return iter(self.delays)

    def __len__(self) -> int:
        return len(self.delays)


async def store_fulltexts(dois: list[str], con: sqlite3.Connection) -> None:
    crawl_delays = CrawlDelays("robots.txt")
    async with DOIDownloader(crawl_delays=crawl_delays) as client:
        await save_fulltexts_from_dois(dois, con, client)


@click.command()
@click.argument("dois", nargs=-1)
@click.option(
    "--file",
    "-f",
    "fh",
    type=click.File(),
    help="Plain-text file, where each line contains one DOI",
)
def main(dois: list[str], fh: click.File) -> list[str]:
    """DOIdownloader: You give it DOIs, it gives you the article PDFs.
    Either supply a list of DOIs as arguments, e.g.:

        python -m doidownloader "10.1057/s41599-024-03044-y" "10.1002/asi.24706"

    Or point to a plain-text file in which each line contains one DOI:

        python -m doidownloader -f dois.txt

    """
    if dois and fh:
        print(  # noqa: T201
            "WARNING: Both a list of DOIs and a file were given. File will be ignored.",
            file=sys.stderr,
        )
    dois = dois or [line.strip() for line in fh]
    con = sqlite3.connect("doi-fulltexts.db")
    prepare_tables(con)

    asyncio.run(store_fulltexts(dois, con))


if __name__ == "__main__":
    main()
