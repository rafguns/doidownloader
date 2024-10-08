"""📝⬇️ DOIdownloader: You give it DOIs, it gives you the article PDFs.**

It is surprisingly tricky to reliably obtain the full PDF of a scientific
publication given its DOI. This Python package aims to do just that: you give it a list
of DOIs, and it will download the full-text PDFs (or other formats if no PDF is
available), taking care of much of the complexity. It ensures that lookups to
different domains can happen asynchronously (i.e., one slow website won't stall all
your other downloads).

"""
import asyncio
import json
import logging
import sqlite3
import ssl
import typing
from collections.abc import Iterable, MutableMapping
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from urllib.parse import quote
from urllib.robotparser import RobotFileParser

import httpx
import lxml
import lxml.html
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)

from . import db, html
from .files import determine_filetype

__version__ = "0.0.1"

logger = logging.getLogger("doidownloader")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("log.txt")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

logger.debug("Application start")

url_templates = {
    "link.springer.com": [
        "https://link.springer.com/content/pdf/{doi}.pdf",
        "https://page-one.springer.com/pdf/preview/{doi}",
    ],
    "www.magonlinelibrary.com": ["https://www.magonlinelibrary.com/doi/pdf/{doi}"],
    "onlinelibrary.wiley.com": [
        "https://onlinelibrary.wiley.com/doi/pdf/{doi}",
        "https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}",
    ],
    "www.tandfonline.com": ["https://www.tandfonline.com/doi/pdf/{doi}"],
    "www.worldscientific.com": ["https://www.worldscientific.com/doi/pdf/{doi}"],
    "www.jstor.org": ["https://www.jstor.org/stable/pdf/{doi}.pdf"],
    "www.emerald.com": ["https://www.emerald.com/insight/content/doi/{doi}/full/pdf"],
}


def track(sequence: Iterable, *args, **kwargs) -> Iterable:
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
    )
    with progress:
        yield from progress.track(sequence, *args, **kwargs)


@dataclass(frozen=True)
class LookupResult:
    """Result of an HTTP request.

    This is similar to a slimmed down version of `httpx.Response`.
    However, even if a request does not yield a response, there is still a LookupResult.
    """

    url: httpx.URL
    error: str | None = None
    status_code: int | None = None
    content: bytes | None = None
    filetype: str | None = None

    def as_tuple(self) -> tuple:
        return (
            str(self.url),
            self.error,
            self.status_code,
            self.content,
            self.filetype,
        )


# For type annotation of DOIDownloader.__aenter__()
U = typing.TypeVar("U", bound="DOIDownloader")


class DOIDownloader:
    """Client for downloading full-texts from DOIs.

    Example usage
    -------------

    ```python
    import sqlite3
    import doidownloader

    # SQLite database where results will be stored
    con = sqlite3.connect("somedois.db")
    doidownloader.db.prepare_tables(con)
    # List of DOIs to search for
    dois_to_find = ["10.1108/JCRPP-02-2020-0025", "10.23860/JMLE-2020-12-3-1"]

    async with doidownloader.DOIDownloader() as client:
        await save_fulltexts_from_dois(dois, con, client)
    ```

    """

    def __init__(
        self,
        client: httpx.Client | None = None,
        crawl_delays: MutableMapping[str, int] | None = None,
    ) -> None:
        self.client = client or httpx.AsyncClient(
            timeout=10.0,
            headers={
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
                )
            },
        )
        # We maintain a lock per domain to ensure that the crawl delays are respected.
        self.domain_locks: dict[str, asyncio.Lock] = {}
        self.crawl_delays: MutableMapping[str, int] = crawl_delays or {}

    async def __aenter__(self: U) -> U:
        await self.client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.client.__aexit__(exc_type, exc_value, traceback)

    @staticmethod
    def lookup_result_on_http_error(
        exception: httpx.HTTPError, url: httpx.URL
    ) -> LookupResult:
        status_code = (
            exception.response.status_code
            if isinstance(exception, httpx.HTTPStatusError)
            else None
        )

        return LookupResult(url, f"HTTP error: {exception}", status_code)

    async def get(
        self,
        url: httpx.URL,
        *,
        raise_for_status: bool = True,
        follow_redirects: bool = True,
        **kwargs,
    ) -> httpx.Response:
        try:
            lock = self.domain_locks[url.host]
        except KeyError:
            lock = asyncio.Lock()
            self.domain_locks[url.host] = lock

        async with lock:
            crawl_delay = await self.check_crawl_delay(url)
            await asyncio.sleep(crawl_delay)

            logger.debug("Retrieving url %s", url)
            r = await self.client.get(url, follow_redirects=follow_redirects, **kwargs)
            if raise_for_status:
                r.raise_for_status()

        return r

    async def metadata_from_url(self, url: httpx.URL, **kwargs) -> LookupResult:
        """Retrieve HTML metadata for URL."""
        try:
            r = await self.get(url, **kwargs)
        except (httpx.HTTPError, ssl.SSLCertVerificationError) as exc:
            return self.lookup_result_on_http_error(exc, url)

        # Retrieve metadata
        try:
            meta = html.metadata_from_html(self.response_to_html(r))
        except (AttributeError, lxml.etree.ParserError):
            return LookupResult(
                r.url, "Not HTML page, or empty/unparseable page", r.status_code
            )

        if not meta:
            # Handle HTML-based redirects, used by Elsevier and possibly others
            new_url = html.resolve_html_redirect(html.response_to_html(r))
            if new_url:
                return await self.metadata_from_url(new_url, **kwargs)
            return LookupResult(r.url, "No <meta> on HTML page", r.status_code)

        return LookupResult(
            r.url, None, r.status_code, json.dumps(meta).encode("utf-8"), "json"
        )

    async def check_crawl_delay(self, url: httpx.URL, default_delay: int = 1) -> int:
        domain = url.host

        if domain not in self.crawl_delays:
            robots_url = url.copy_with(path="/robots.txt", query=None, fragment=None)
            logger.debug("Checking robots policy for %s (%s)", domain, robots_url)

            try:
                r = await self.client.get(robots_url, follow_redirects=True)
                r.raise_for_status()

                rp = RobotFileParser()
                rp.parse(r.text.splitlines())
                self.crawl_delays[domain] = int(rp.crawl_delay("*") or default_delay)
            except httpx.HTTPError:  # HTTP error or no robots.txt
                self.crawl_delays[domain] = default_delay

        return self.crawl_delays[domain]

    async def retrieve_fulltext(
        self, url: httpx.URL, expected_filetype: str, **kwargs
    ) -> LookupResult:
        """Retrieve full-text from URL.

        This only returns the full-text if the file type matches what was expected.
        The reason for that is that some servers return web pages saying 'Not found'
        but with status code 200.

        """
        try:
            r = await self.get(url, **kwargs)
            # Follow redirects
            if 300 <= r.status_code <= 399:  # noqa: PLR2004
                new_url = r.next_request
                r = await self.retrieve_fulltext(new_url, **kwargs)
        except (httpx.HTTPError, ssl.SSLCertVerificationError) as exc:
            return self.lookup_result_on_http_error(exc, url)

        filetype = determine_filetype(r.headers.get("content-type"), r.content)
        if filetype == expected_filetype:
            return LookupResult(r.url, None, r.status_code, r.content, filetype)

        # Type is different from what we expected. Typically this is some HTML page
        # being shown instead of the desired content.
        return LookupResult(
            r.url, "Not expected file type", r.status_code, r.content, filetype
        )

    async def best_unpaywall_url(
        self, doi: str, email: str = "raf.guns@uantwerpen.be"
    ) -> httpx.URL | None:
        url = httpx.URL(f"https://api.unpaywall.org/v2/{quote(doi)}?email={email}")
        try:
            r = await self.get(url)
        except httpx.HTTPError:
            return None

        data = r.json()
        if not data["is_oa"]:
            return None

        return httpx.URL(data["best_oa_location"]["url"])


async def save_fulltexts_from_dois(
    dois: list[str], con: sqlite3.Connection, client: DOIDownloader
) -> None:
    """Retrieve and save metadata for all DOIs."""
    # With this, we can run the script in multiple batches.
    inserted_dois = db.dois_with_fulltext(con)

    tasks = set()

    for doi in dois:
        if doi in inserted_dois:
            continue
        task = asyncio.create_task(save_fulltext_from_doi(doi, con, client))
        tasks.add(task)

    for task in track(
        asyncio.as_completed(tasks), description="Looking up DOIs...", total=len(tasks)
    ):
        await task


async def save_fulltext_from_doi(
    doi: str,
    con: sqlite3.Connection,
    client: DOIDownloader,
) -> None:
    res = await retrieve_best_fulltext(doi, client)

    if not res:
        logger.debug("No fulltext for DOI %s", doi)
        return

    logger.debug("Saving fulltext for DOI %s", doi)
    try:
        con.execute(
            """insert into doi_fulltext values (?, ?, ?, ?, ?, ?, ?)""",
            (doi, *res, datetime.now()),
        )
        con.commit()
    except sqlite3.IntegrityError:
        logger.error("SQLite integrity error trying to insert DOI %s", doi)


async def retrieve_best_fulltext(doi: str, client: DOIDownloader) -> tuple | None:
    # Does DOI link directly to PDF?
    logger.debug("Checking for direct link for DOI %s", doi)
    doi_url = httpx.URL("https://doi.org").join(f"/{quote(doi)}")
    res = await client.retrieve_fulltext(doi_url, expected_filetype="pdf")
    if res.error is None:
        return res.as_tuple()

    # Can we use information from HTML <meta> elements?
    logger.debug("Checking for metadata for DOI %s", doi)
    direct_url = res.url
    res = await client.metadata_from_url(direct_url)
    if res.error is None:
        try:
            fulltext_url, filetype = html.fulltext_urls_from_meta(res.content)
            res = await client.retrieve_fulltext(
                fulltext_url, expected_filetype=filetype
            )
            if res.error is None:
                return res.as_tuple()
        except TypeError:
            pass

    # URL templates by hostname
    logger.debug("Checking for URL template for DOI %s", doi)
    for template in url_templates.get(direct_url.host, []):
        tmpl_url = httpx.URL(template.format(doi=quote(doi)))
        res = await client.retrieve_fulltext(tmpl_url, expected_filetype="pdf")
        if res.error is None:
            return res.as_tuple()

    # Unpaywall
    logger.debug("Checking for Unpaywall for DOI %s", doi)
    unpaywall_url = await client.best_unpaywall_url(doi)
    if unpaywall_url:
        res = await client.retrieve_fulltext(unpaywall_url, expected_filetype="pdf")
        if res.error is None:
            return res.as_tuple()

    return None
