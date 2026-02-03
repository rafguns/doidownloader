from urllib.parse import quote

import httpx

from . import html
from .doidownloader import DOIDownloader, logger


def doi_url(doi: str) -> httpx.URL:
    return httpx.URL("https://doi.org").join(f"/{quote(doi)}")


async def direct_link(doi: str, client: DOIDownloader) -> tuple | None:
    """Does DOI link directly to PDF?"""
    logger.debug("Checking for direct link for DOI %s", doi)
    url = doi_url(doi)
    res = await client.retrieve_fulltext(url, expected_filetype="pdf")
    return res.as_tuple() if res.error is None else None


async def html_meta(doi: str, client: DOIDownloader) -> tuple | None:
    """Can we use information from HTML <meta> elements?"""
    logger.debug("Checking for metadata for DOI %s", doi)
    res_doi = await client.get(doi_url(doi))
    res_landingpage = await client.metadata_from_url(res_doi.url)
    if res_landingpage.content is not None:
        try:
            fulltext_url, filetype = html.fulltext_urls_from_meta(
                res_landingpage.content
            )  # pyright: ignore[reportGeneralTypeIssues]
            res_landingpage = await client.retrieve_fulltext(
                fulltext_url, expected_filetype=filetype
            )
            if res_landingpage.error is None:
                return res_landingpage.as_tuple()
        except TypeError:
            pass
    return None


async def url_templates(doi: str, client: DOIDownloader) -> tuple | None:
    """Try known URL templates by hostname"""
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
        "www.emerald.com": [
            "https://www.emerald.com/insight/content/doi/{doi}/full/pdf"
        ],
    }
    logger.debug("Checking for URL template for DOI %s", doi)
    res_doi = await client.get(doi_url(doi))
    for template in url_templates.get(res_doi.url.host, []):
        tmpl_url = httpx.URL(template.format(doi=quote(doi)))
        res_landingpage = await client.retrieve_fulltext(
            tmpl_url, expected_filetype="pdf"
        )
        if res_landingpage.error is None:
            return res_landingpage.as_tuple()
    return None


async def unpaywall(doi: str, client: DOIDownloader) -> tuple | None:
    """Get best OA version according to Unpaywall"""
    logger.debug("Checking for Unpaywall for DOI %s", doi)
    unpaywall_url = await client.best_unpaywall_url(doi)
    if unpaywall_url:
        res_landingpage = await client.retrieve_fulltext(
            unpaywall_url, expected_filetype="pdf"
        )
        if res_landingpage.error is None:
            return res_landingpage.as_tuple()
    return None
