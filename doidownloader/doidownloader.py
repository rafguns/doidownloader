import asyncio
import json
import logging
import ssl
import typing
from collections.abc import MutableMapping
from dataclasses import dataclass
from types import TracebackType
from urllib.parse import quote
from urllib.robotparser import RobotFileParser

import httpx
import lxml
import lxml.etree

from . import html
from .files import determine_filetype

__version__ = "0.0.1"

logger = logging.getLogger("doidownloader")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("log.txt")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

logger.debug("Application start")


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
        client: httpx.AsyncClient | None = None,
        crawl_delays: MutableMapping[str, int] | None = None,
        email_address: str | None = None,
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
        self.email_address = email_address

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
        exception: httpx.HTTPError | ssl.SSLCertVerificationError, url: httpx.URL
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
            response = await self.client.get(
                url, follow_redirects=follow_redirects, **kwargs
            )
            if raise_for_status:
                response.raise_for_status()

        return response

    async def metadata_from_url(self, url: httpx.URL, **kwargs) -> LookupResult:
        """Retrieve HTML metadata for URL."""
        try:
            r = await self.get(url, **kwargs)
        except (httpx.HTTPError, ssl.SSLCertVerificationError) as exc:
            return self.lookup_result_on_http_error(exc, url)

        # Retrieve metadata
        try:
            meta = html.metadata_from_html(html.response_to_html(r))
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
            response = await self.get(url, **kwargs)
        except (httpx.HTTPError, ssl.SSLCertVerificationError) as exc:
            return self.lookup_result_on_http_error(exc, url)

        filetype = determine_filetype(
            response.headers.get("content-type"), response.content
        )
        if filetype == expected_filetype:
            return LookupResult(
                response.url, None, response.status_code, response.content, filetype
            )

        # Type is different from what we expected. Typically this is some HTML page
        # being shown instead of the desired content.
        return LookupResult(
            response.url,
            "Not expected file type",
            response.status_code,
            response.content,
            filetype,
        )

    async def best_unpaywall_url(self, doi: str) -> httpx.URL | None:
        if not self.email_address:
            # Email address is required, see http://unpaywall.org/products/api
            return None
        url = httpx.URL(
            f"https://api.unpaywall.org/v2/{quote(doi)}?email={self.email_address}"
        )
        try:
            r = await self.get(url)
        except httpx.HTTPError:
            return None

        data = r.json()
        if not data["is_oa"]:
            return None

        return httpx.URL(data["best_oa_location"]["url"])
