import asyncio
import dataclasses
import datetime as dt
import sqlite3
import typing
from collections.abc import Iterable

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)

from . import db
from .doidownloader import DOIDownloader, logger


@dataclasses.dataclass
class DownloadTask:
    dois: list[str]
    connection: sqlite3.Connection
    strategies: list[typing.Callable]

    async def save_fulltexts(self, client: DOIDownloader) -> None:
        """Retrieve and save metadata for all DOIs."""
        # With this, we can run the script in multiple batches.
        inserted_dois = db.dois_with_fulltext(self.connection)

        tasks = set()

        for doi in self.dois:
            if doi in inserted_dois:
                continue
            task = asyncio.create_task(self.save_fulltext(doi, client))
            tasks.add(task)

        for task in track(
            asyncio.as_completed(tasks),
            description="Looking up DOIs...",
            total=len(tasks),
        ):
            await task

    async def save_fulltext(
        self,
        doi: str,
        client: DOIDownloader,
    ) -> None:
        for strat in self.strategies:
            res = strat(doi, client)
            # We could expand it here to try multiple strategies
            # even if we have found some result
            if res is not None:
                break
        if not res:
            logger.debug("No fulltext for DOI %s", doi)
            return

        logger.debug("Saving fulltext for DOI %s", doi)
        try:
            self.connection.execute(
                """insert into doi_fulltext values (?, ?, ?, ?, ?, ?, ?)""",
                (doi, *res, dt.datetime.now()),
            )
            self.connection.commit()
        except sqlite3.IntegrityError:
            logger.error("SQLite integrity error trying to insert DOI %s", doi)


def track(sequence: Iterable, *args, **kwargs) -> Iterable:
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
    )
    with progress:
        yield from progress.track(sequence, *args, **kwargs)
