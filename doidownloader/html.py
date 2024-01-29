import contextlib
import json
import re
from collections import defaultdict

import httpx
import lxml.html


def _list2dict(list_of_tuples: list[tuple]) -> dict[str, set[str]]:
    d = defaultdict(set)
    for k, v in list_of_tuples:
        d[k].add(v)
    return d


def resolve_html_redirect(html: lxml.html.HtmlElement) -> httpx.URL | None:
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


def response_to_html(response: httpx.Response) -> lxml.html.HtmlElement:
    html = lxml.html.fromstring(
        # This avoids errors when there's an XML declaration with encoding.
        # See issue #18.
        bytes(response.text, encoding="utf-8"),
        base_url=str(response.url),
    )
    # So we don't have to worry about relative links further on:
    with contextlib.suppress(ValueError):
        html.make_links_absolute()

    return html


def fulltext_urls_from_meta(data: bytes) -> tuple[httpx.URL, str] | None:
    meta_info = json.loads(data)
    meta_dict = _list2dict(meta_info)

    # These are the main fields used in HTML <meta> elements, with their file type.
    # Note: We do NOT include "citation_fulltext_html_url". This is used by Springer to
    # refer to landing pages rather than proper full-text documents.
    meta_url_fields = {
        "citation_pdf_url": "pdf",
        "citation_xml_url": "xml",
        "citation_full_html_url": "html",
    }
    for field, filetype in meta_url_fields.items():
        if field not in meta_dict:
            continue
        for fulltext_url in meta_dict[field]:
            # Skip blank URLs
            if fulltext_url.strip() == "":
                continue
            return httpx.URL(fulltext_url), filetype

    # No relevant meta fields found
    return None
