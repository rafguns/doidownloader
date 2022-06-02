import hashlib
import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import httpx
import lxml
import pandas as pd
import requests_html
from tqdm.auto import tqdm

# Prefill a few publishers where we encountered problems due to missing or
# incorrect robots.txt
crawl_delays: Dict[str, int] = {}
with open("robots.txt") as fh_robots:
    for line in fh_robots:
        domain, delay = line.strip().split()
        crawl_delays[domain] = int(delay)


file_types = [
    # extension, MIME type, GS meta field
    ("pdf", "application/pdf", "citation_pdf_url"),
    ("xml", "application/xml", "citation_xml_url"),
    ("xml", "text/xml", "citation_xml_url"),
    ("html", "text/html", "citation_full_html_url"),
    ("txt", "text/plain", None),
    ("epub", "application/epub+zip", None),
    ("json", "application/json", None),
    ("png", "image/png", None),
]

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


@dataclass(frozen=True)
class LookupResult:
    """Result of an HTTP request"""

    url: httpx.URL
    error: Optional[str] = None
    status_code: Optional[int] = None
    content: Optional[bytes] = None

    def as_tuple(self) -> tuple:
        return (str(self.url), self.error, self.status_code, self.content)


def response_to_html(response: httpx.Response) -> requests_html.HTML:
    return requests_html.HTML(url=str(response.url), html=response.content)


def lookup_result_on_http_error(exception: httpx.HTTPError) -> LookupResult:
    error = f"HTTP error: {exception}"
    status_code = (
        exception.response.status_code
        if isinstance(exception, httpx.HTTPStatusError)
        else None
    )

    return LookupResult(exception.request.url, error, status_code)


def check_crawl_delay(url: httpx.URL, client: httpx.Client, default_delay: int = 1) -> int:
    domain = url.netloc.decode("utf-8")

    if domain not in crawl_delays:
        print(f"Checking robots policy for {domain}")

        try:
            r = client.get(url, follow_redirects=True)
            r.raise_for_status()

            rp = RobotFileParser()
            rp.parse((line for line in r.text))
            crawl_delays[domain] = int(rp.crawl_delay("*") or default_delay)
        except (httpx.HTTPError, AttributeError):  # HTTP error or no robots.txt
            crawl_delays[domain] = default_delay
        with open("robots.txt", "a") as fh:
            fh.write(f"{domain}\t{crawl_delays[domain]}\n")

    return crawl_delays[domain]


def resolve_html_redirect(html: requests_html.HTML) -> Optional[str]:
    redirect = html.find(
        'meta[http-equiv="REFRESH"], meta[http-equiv="REFRESH"]', first=True
    )
    if not redirect:
        return None

    m = re.search(
        r'url\s*=\s*[\'"](.*?)[\'"]', redirect.attrs.get("content"), re.IGNORECASE
    )
    if not m:
        return None

    redirect_url = m[1]
    return urljoin(html.base_url, redirect_url)


def metadata_from_url(url: str, client: httpx.Client, **kwargs) -> LookupResult:
    """Retrieve HTML metadata for URL"""

    try:
        r = client.get(url, follow_redirects=True, **kwargs)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        return lookup_result_on_http_error(exc)

    # Retrieve metadata
    try:
        meta = metadata_from_html(response_to_html(r))
    except AttributeError:
        return LookupResult(r.url, "Not HTML page", r.status_code)
    except lxml.etree.ParserError:
        return LookupResult(r.url, "Empty or unparseable page", r.status_code)

    # Handle HTML-based redirects, used by Elsevier and possibly others
    if not meta:
        new_url = resolve_html_redirect(response_to_html(r))
        if new_url:
            return metadata_from_url(new_url, client, **kwargs)

    return LookupResult(r.url, None, r.status_code, json.dumps(meta).encode("utf-8"))


def metadata_from_html(html: requests_html.HTML) -> List[Tuple[str, str]]:
    """Return all Google Scholar and Dublin Core meta info"""
    meta_els = html.find(
        'meta[name^="citation_"], meta[name^="dc."], meta[name^="DC."]'
    )
    return [
        (el.attrs["name"], el.attrs["content"])
        for el in meta_els
        if "content" in el.attrs
    ]


def retrieve_fulltext(
    url: str, client: httpx.Client, expected_ftype: str, **kwargs
) -> Optional[LookupResult]:
    """Retrieve full-text from URL

    This only returns the full-text if the file type matches what was expected.
    The reason for that is that some servers return web pages with 'Not found' on them,
    but with status code 200.

    """
    try:
        r = client.get(url, follow_redirects=True, **kwargs)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        return lookup_result_on_http_error(exc)

    extension = determine_extension(r.headers.get("content-type"), r.content)
    if extension == expected_ftype:
        return LookupResult(r.url, None, r.status_code, r.content)

    # Type is different from what we expected. Typically this is some HTML page being shown
    # instead of the desired content.

    # ScienceDirect uses *another* interim page here; follow only link, which redirects to
    # the actual PDF
    if "sciencedirect.com" in url:
        links = response_to_html(r).links
        if len(links) == 1:
            return retrieve_fulltext(links.pop(), client, expected_ftype, **kwargs)

    return None


def best_unpaywall_url(
    doi: str, client: httpx.Client, email: str = "raf.guns@uantwerpen.be"
) -> Optional[str]:
    url = f"https://api.unpaywall.org/v2/{quote(doi)}?email={email}"
    r = client.get(url)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        return None

    data = r.json()
    if not data["is_oa"]:
        return None

    return data["best_oa_location"]["url"]


def save_metadata(
    dois: List[str], con: sqlite3.Connection, client: httpx.Client
) -> None:
    """Retrieve and save metadata for all DOIs"""
    # Field error specifies what kind of error (if any) has occurred
    # (e.g. no content, connection error, HTTP error).
    # Field status_code is for HTTP status code, including HTTP errors.
    cur = con.cursor()
    cur.execute(
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
        row[0] for row in cur.execute("select doi from doi_meta").fetchall()
    }

    for doi in tqdm(dois):
        if doi in inserted_dois:
            continue
        doi_url = "https://doi.org/" + quote(doi)
        res = metadata_from_url(doi_url, client)

        cur.execute(
            """insert into doi_meta values (?, ?, ?, ?, ?, ?)""",
            (doi, *res.as_tuple(), datetime.now()),
        )
        con.commit()
        time.sleep(check_crawl_delay(res.url, client))


def save_fulltext(con: sqlite3.Connection, client: httpx.Client) -> None:
    """Retrieve and save full-text (where available) of all DOIs in table doi_meta"""
    cur = con.cursor()
    cur.execute(
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

    cur.execute(
        """
        select *
        from doi_meta
        where doi not in (select doi from doi_fulltext)
        """
    )

    # Small utility function used further
    def list2dict(l):
        d = defaultdict(set)
        for k, v in l:
            d[k].add(v)
        return d

    # XXX Decouple this from doi_meta table
    for doi, url, error, status_code, meta, _ in tqdm(cur.fetchall()):
        results: List[tuple] = []

        # Direct PDF link
        if error == "Not HTML page" and status_code == 200:
            res = retrieve_fulltext(url, client, expected_ftype="pdf")
            if res:
                results.append((*res.as_tuple(), "application/pdf"))

        # meta citation_ links
        if not results:
            if meta:
                meta_info = json.loads(meta)
                meta_dict = list2dict(meta_info)
                for file_type, content_type, url_type in file_types:
                    if url_type not in meta_dict:
                        continue
                    # Filter out empty string URLs
                    fulltext_urls = {url for url in meta_dict[url_type] if url}
                    for fulltext_url in fulltext_urls:
                        res = retrieve_fulltext(
                            fulltext_url, client, expected_ftype=file_type
                        )
                        if res:
                            results.append((*res.as_tuple(), content_type))

        # URL templates by hostname
        if not results:
            hostname = urlsplit(url).netloc
            templates = url_templates.get(hostname, [])
            for template in templates:
                tmpl_url = template.format(doi=quote(doi))
                res = retrieve_fulltext(tmpl_url, client, expected_ftype="pdf")
                if res and res.status_code == 200:
                    results.append((*res.as_tuple(), "application/pdf"))
                    break

        # Unpaywall
        if not results:
            unpaywall_url = best_unpaywall_url(doi, client)
            if unpaywall_url:
                res = retrieve_fulltext(unpaywall_url, client, expected_ftype="pdf")
                if res and res.status_code == 200:
                    results.append((*res.as_tuple(), "application/pdf"))

        # Save results
        for result in results:
            try:
                con.execute(
                    """insert into doi_fulltext values (?, ?, ?, ?, ?, ?, ?)""",
                    (doi, *result, datetime.now()),
                )
            except sqlite3.IntegrityError:
                # Ignore - this may happen if same content is registered under
                # multiple content-types, e.g., application/xml and text/xml
                pass
        con.commit()


def determine_extension(content_type: str, content: bytes) -> str:
    extensions = {mime_type: ext for ext, mime_type, _ in file_types}
    try:
        content_type = content_type.split(";")[0]
        return extensions[content_type]
    except (AttributeError, KeyError):
        # Unknown or missing content type; guess by content sniffing
        if content[:4] == b"%PDF":
            return "pdf"
        elif content.startswith(b"<article"):
            return "xml"
        else:
            return "unknown"


def same_contents(fname: str, bytestring: bytes) -> bool:
    """Check if contents of file are same as bytestring"""
    hash_file = hashlib.md5(open(fname, "rb").read()).digest()
    hash_bytestring = hashlib.md5(bytestring).digest()

    return hash_file == hash_bytestring


class FileWithSameContentExists(Exception):
    """Exception: a file with the same contents already exists"""


def determine_filename(
    basename: str, ext: str, content: bytes, extra_letter: str = ""
) -> str:
    fname = f"{basename}{extra_letter}.{ext}"

    if not os.path.exists(fname):
        return fname

    if same_contents(fname, content):
        raise FileWithSameContentExists(f"File {fname} has same contents.")

    # There is already a file with the same name but different contents
    if extra_letter == "":
        extra_letter = "a"
    else:
        extra_letter = chr(ord(extra_letter) + 1)

    return determine_filename(basename, ext, content, extra_letter)


if __name__ == "__main__":
    connection = sqlite3.connect("download_extra_DOIs.db")

    df = pd.read_excel("analysistable.xlsx")
    df = df.query("score_total >= 8")

    # Shut up incorrect warning "Type[Client]" has no attribute "__enter__"
    with httpx.Client as http_client:  # type: ignore
        save_metadata(df.DOI.unique(), connection, http_client)
        # save_fulltext(connection, http_client)
        # save_to_docs(connection)
