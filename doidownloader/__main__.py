import asyncio
import sqlite3
import sys
from pathlib import Path

import click

from . import strategies
from .core import DownloadTask
from .crawl_delays import CrawlDelays
from .db import prepare_tables
from .doidownloader import DOIDownloader


async def store_fulltexts(
    dois: list[str], con: sqlite3.Connection, email: str | None, strat_names: list[str]
) -> None:
    crawl_delays = CrawlDelays("robots.txt")
    strat_names = strat_names or [
        "direct_link",
        "html_meta",
        "url_templates",
        "unpaywall",
    ]
    strats = [getattr(strategies, name) for name in strat_names]
    async with DOIDownloader(crawl_delays=crawl_delays, email_address=email) as client:
        download_task = DownloadTask(dois, con, strats)
        await download_task.save_fulltexts(client)


@click.command()
@click.argument("dois", nargs=-1)
@click.option(
    "--file",
    "-f",
    "fh",
    type=click.File(),
    help="Plain-text file, where each line contains one DOI",
)
@click.option(
    "--database",
    type=click.Path(),
    default=Path("doi-fulltexts.db"),
    help="SQLite database that will store the downloaded PDFs",
)
@click.option(
    "--email", type=str, help="Email address, which can speed up lookups in Unpaywall"
)
@click.option(
    "--strat",
    "-s",
    type=str,
    multiple=True,
    help="Strategies to retrieve the 'best' full-text",
)
def main(
    dois: list[str],
    fh: click.File,
    database: click.Path,
    email: str | None,
    strat: list[str],
) -> None:
    """DOIdownloader: You give it DOIs, it gives you the article PDFs.
    Either supply a list of DOIs as arguments, e.g.:

        python -m doidownloader "10.1057/s41599-024-03044-y" "10.1002/asi.24706"

    Or point to a plain-text file in which each line contains one DOI:

        python -m doidownloader -f dois.txt

    Results are stored in a SQLite database named 'doi-fulltexts.db'.
    To store in another file:

        python -m doidownloader -f dois.txt --database my-database.sqlite

    It is recommended to supply an email address for Unpaywall lookups (see
    https://unpaywall.org/products/api):

        python -m doidownloader -f dois.txt --email youraddress@example.com

    """
    if dois and fh:
        print(  # noqa: T201
            "WARNING: Both a list of DOIs and a file were given. File will be ignored.",
            file=sys.stderr,
        )
    dois = dois or [line.strip() for line in fh]  # pyright: ignore[reportGeneralTypeIssues]
    con = sqlite3.connect(str(database))
    prepare_tables(con)

    asyncio.run(store_fulltexts(dois, con, email, strat))


if __name__ == "__main__":
    main()
