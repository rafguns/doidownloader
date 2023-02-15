"""DOI downloader: legally download full-text documents  from a list of DOIs."""
import asyncio
import json
import re
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional
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

from .files import determine_extension, file_types

__version__ = "0.0.1"

logger = logging.getLogger("doidownloader")

# Prefill a few publishers where we encountered problems due to missing or
# incorrect robots.txt
crawl_delays: dict[str, int] = {}
with open("robots.txt") as fh_robots:
    for line in fh_robots:
        domain, delay = line.strip().split()
        crawl_delays[domain] = int(delay)

    domain_locks: dict[str, asyncio.Lock] = {}


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
    error: Optional[str] = None
    status_code: Optional[int] = None
    content: Optional[bytes] = None

    def as_tuple(self) -> tuple:
        return (str(self.url), self.error, self.status_code, self.content)


class DOIDownloader:
    """Client for downloading full-texts from DOIs.

    In principle, you'll mainly use this for the `save_metadata` and `save_fulltext`
    functions. Example usage::

        import sqlite3
        import doidownloader

        con = sqlite3.connect("somedois.db")
        dois_to_find = ["10.1108/JCRPP-02-2020-0025", "10.23860/JMLE-2020-12-3-1"]

        with DOIDownloader() as client:
            save_metadata(dois_to_find, con, client)
            save_fulltext(con, client)

    """

    def __init__(self, client: Optional[httpx.Client] = None) -> None:
        self.client = client or httpx.AsyncClient(
            timeout=10.0,
            headers={
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 "
                + "Safari/537.36"
            },
        )

    def __enter__(self):
        self.client.__enter__()
        return self

    def __exit__(self, ecx_type, ecx_value, traceback):
        self.client.__exit__(ecx_type, ecx_value, traceback)

    @staticmethod
    def response_to_html(response: httpx.Response) -> lxml.html.HtmlElement:
        html = lxml.html.fromstring(response.text, base_url=str(response.url))
        # So we don't have to worry about relative links further on:
        html.make_links_absolute()

        return html

    @staticmethod
    def lookup_result_on_http_error(exception: httpx.HTTPError) -> LookupResult:
        error = f"HTTP error: {exception}"
        status_code = (
            exception.response.status_code
            if isinstance(exception, httpx.HTTPStatusError)
            else None
        )

        return LookupResult(exception.request.url, error, status_code)

    @staticmethod
    def resolve_html_redirect(html: lxml.html.HtmlElement) -> Optional[httpx.URL]:
        try:
            redirect = html.cssselect(
                'meta[http-equiv="REFRESH"], meta[http-equiv="refresh"]'
            )[0]
        except IndexError:
            return None

        # Parse out the URL from the attribute (e.g. `content="5; url=/foo"`)
        # We use separate regexes for variants with and without quote marks.
        m = re.search(
            r'url\s*=\s*[\'"](.*?)[\'"]', redirect.attrib.get("content"), re.IGNORECASE
        ) or re.search(r"url\s*=\s*(.+)", redirect.attrib.get("content"), re.IGNORECASE)
        if not m:
            return None

        redirect_url = m[1]
        return httpx.URL(html.base_url).join(redirect_url)

    @staticmethod
    def metadata_from_html(html: lxml.html.HtmlElement) -> list[tuple[str, str]]:
        """Return all Google Scholar and Dublin Core meta info."""
        meta_els = html.cssselect(
            'meta[name^="citation_"], meta[name^="dc."], meta[name^="DC."]'
        )
        return [
            (el.attrib["name"], el.attrib["content"])
            for el in meta_els
            if "content" in el.attrib
        ]

    async def get(
        self,
        url: httpx.URL,
        raise_for_status: bool = True,
        follow_redirects: bool = True,
        *args,
        **kwargs,
    ) -> httpx.Response:
        crawl_delay = await self.check_crawl_delay(url)
        try:
            lock = domain_locks[url.host]
        except KeyError:
            lock = asyncio.Lock()
            domain_locks[url.host] = lock

        async with lock:
            await asyncio.sleep(crawl_delay)

            logger.debug("Retrieving url %s", url)
            r = await self.client.get(
                url, follow_redirects=follow_redirects, *args, **kwargs
            )
            if raise_for_status:
                r.raise_for_status()

        return r

    async def metadata_from_url(self, url: httpx.URL, **kwargs) -> LookupResult:
        """Retrieve HTML metadata for URL."""
        try:
            r = await self.get(url, **kwargs)
        except httpx.HTTPError as exc:
            return self.lookup_result_on_http_error(exc)

        # Retrieve metadata
        try:
            meta = self.metadata_from_html(self.response_to_html(r))
        except AttributeError:
            return LookupResult(r.url, "Not HTML page", r.status_code)
        except lxml.etree.ParserError:
            return LookupResult(r.url, "Empty or unparseable page", r.status_code)

        # Handle HTML-based redirects, used by Elsevier and possibly others
        if not meta:
            new_url = self.resolve_html_redirect(self.response_to_html(r))
            if new_url:
                return await self.metadata_from_url(new_url, **kwargs)

        return LookupResult(
            r.url, None, r.status_code, json.dumps(meta).encode("utf-8")
        )

    async def check_crawl_delay(self, url: httpx.URL, default_delay: int = 1) -> int:
        if url.host not in crawl_delays:
            logger.debug(f"Checking robots policy for {url.host}")
            robots_url = url.copy_with(path="/robots.txt")

            try:
                r = await self.client.get(robots_url)
                r.raise_for_status()

                rp = RobotFileParser()
                rp.parse(r.text.splitlines())
                crawl_delays[domain] = int(rp.crawl_delay("*") or default_delay)
            except httpx.HTTPError:  # HTTP error or no robots.txt
                crawl_delays[domain] = default_delay
            with open("robots.txt", "a") as fh:
                fh.write(f"{domain}\t{crawl_delays[domain]}\n")

        return crawl_delays[domain]

    async def retrieve_fulltext(
        self, url: httpx.URL, expected_ftype: str, **kwargs
    ) -> Optional[LookupResult]:
        """Retrieve full-text from URL.

        This only returns the full-text if the file type matches what was expected.
        The reason for that is that some servers return web pages saying 'Not found'
        but with status code 200.

        """
        try:
            r = await self.get(url, **kwargs)
        except httpx.HTTPError as exc:
            return self.lookup_result_on_http_error(exc)

        extension = determine_extension(r.headers.get("content-type"), r.content)
        if extension == expected_ftype:
            return LookupResult(r.url, None, r.status_code, r.content)

        # Type is different from what we expected. Typically this is some HTML page
        # being shown instead of the desired content.

        # ScienceDirect uses *another* interim page here; follow only link, which
        # redirects to the actual PDF
        if "sciencedirect.com" in url.host:
            links = [
                el.attrib["href"]
                for el in self.response_to_html(r).cssselect("a[href]")
            ]
            if len(links) == 1:
                return await self.retrieve_fulltext(
                    links.pop(), expected_ftype, **kwargs
                )

        return None

    async def best_unpaywall_url(
        self, doi: str, email: str = "raf.guns@uantwerpen.be"
    ) -> Optional[httpx.URL]:
        url = httpx.URL(f"https://api.unpaywall.org/v2/{quote(doi)}?email={email}")
        try:
            r = await self.get(url)
        except httpx.HTTPError:
            return None

        data = r.json()
        if not data["is_oa"]:
            return None

        return httpx.URL(data["best_oa_location"]["url"])


async def retrieve_metadata(
    dois: list[str], con: sqlite3.Connection, client: DOIDownloader
) -> None:
    """Retrieve and save metadata for all DOIs."""
    # Field error specifies what kind of error (if any) has occurred
    # (e.g. no content, connection error, HTTP error).
    # Field status_code is for HTTP status code, including HTTP errors.
    con.execute(
        """
        create table if not exists doi_meta
        (
            doi text primary key,
            url text,
            error text,
            status_code integer,
            meta text,
            last_change timestamp
        )
        """
    )
    con.commit()

    # With this, we can run the script in multiple batches.
    inserted_dois = {
        row[0] for row in con.execute("select doi from doi_meta").fetchall()
    }

    tasks = set()

    for doi in dois:
        if doi in inserted_dois:
            continue
        doi_url = httpx.URL("https://doi.org").join(f"/{quote(doi)}")
        task = asyncio.create_task(client.metadata_from_url(doi_url), name=doi)
        tasks.add(task)
        task.add_done_callback(lambda task: save_metadata(task, con))

    for task in track(tasks, description="Looking up DOIs..."):
        await task


def save_metadata(task, con):
    doi = task.get_name()
    res = task.result()

    con.execute(
        """insert into doi_meta values (?, ?, ?, ?, ?, ?)""",
        (doi, *res.as_tuple(), datetime.now()),
    )
    con.commit()


async def retrieve_fulltexts(con: sqlite3.Connection, client: DOIDownloader) -> None:
    """Retrieve and save full-text (where available) of all DOIs in table doi_meta."""
    con.execute(
        """
        create table if not exists doi_fulltext
        (
            doi text,
            url text,
            error text,
            status_code integer,
            content blob,
            content_type text,
            last_change timestamp,
            constraint doi_fulltext_pk primary key (doi, url)
        )
        """
    )
    con.commit()

    doi_meta = con.execute(
        """
        select *
        from doi_meta
        where doi not in (select doi from doi_fulltext)
        """
    ).fetchall()

    tasks = set()

    for doi, url, error, status_code, meta, _ in doi_meta:
        task = asyncio.create_task(
            retrieve_best_fulltexts(
                client, doi, LookupResult(httpx.URL(url), error, status_code, meta)
            ),
            name=doi,
        )

        tasks.add(task)
        task.add_done_callback(lambda task: save_fulltexts(task, con))

    for task in track(
        asyncio.as_completed(tasks), description="Saving fulltexts...", total=len(tasks)
    ):
        await task


def save_fulltexts(task, con):
    doi = task.get_name()
    reslist = task.result()

    if reslist is None:
        # TODO log these somehow
        return

    for res in reslist:
        try:
            con.execute(
                """insert into doi_fulltext values (?, ?, ?, ?, ?, ?, ?)""",
                (doi, *res, datetime.now()),
            )
            con.commit()
        except sqlite3.IntegrityError:
            # Ignore - this may happen if same content is registered under
            # multiple content-types, e.g., application/xml and text/xml
            pass


def _list2dict(list_of_tuples):
    d = defaultdict(set)
    for k, v in list_of_tuples:
        d[k].add(v)
    return d


async def retrieve_best_fulltexts(
    client: DOIDownloader, doi: str, lookup_result: LookupResult
) -> list[tuple]:
    # Direct PDF link
    if lookup_result.error == "Not HTML page" and lookup_result.status_code == 200:
        res = await client.retrieve_fulltext(lookup_result.url, expected_ftype="pdf")
        if res:
            return [(*res.as_tuple(), "application/pdf")]

    ft = []
    # meta citation_ links
    if lookup_result.content:
        meta_info = json.loads(lookup_result.content)
        meta_dict = _list2dict(meta_info)

        found_fulltext = False
        for file_type, content_type, url_type in file_types:
            if url_type not in meta_dict:
                continue
            for fulltext_url in meta_dict[url_type]:
                res = await client.retrieve_fulltext(
                    httpx.URL(fulltext_url), expected_ftype=file_type
                )
                if res:
                    if res.status_code == 200:
                        found_fulltext = True
                    ft.append((*res.as_tuple(), content_type))
        if found_fulltext:
            return ft

    # URL templates by hostname
    hostname = lookup_result.url.host
    templates = url_templates.get(hostname, [])
    for template in templates:
        tmpl_url = httpx.URL(template.format(doi=quote(doi)))
        res = await client.retrieve_fulltext(tmpl_url, expected_ftype="pdf")
        if res and res.status_code == 200:
            return [(*res.as_tuple(), "application/pdf")]

    # Unpaywall
    unpaywall_url = await client.best_unpaywall_url(doi)
    if unpaywall_url:
        res = await client.retrieve_fulltext(unpaywall_url, expected_ftype="pdf")
        if res and res.status_code == 200:
            return [(*res.as_tuple(), "application/pdf")]
