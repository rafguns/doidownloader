import httpx
import lxml.html
import pytest

from doidownloader import html


@pytest.mark.parametrize(
    "html_string",
    [
        '<meta http-equiv="refresh" content="0; url=https://example.com/foo">',
        '<META HTTP-EQUIV=REFRESH CONTENT="5; URL=https://example.com/foo">',
    ],
)
def test_resolve_html_redirect_no_base_url(html_string):
    html_el = lxml.html.fromstring(html_string)
    assert html.resolve_html_redirect(html_el) == httpx.URL("https://example.com/foo")


@pytest.mark.parametrize(
    "html_string",
    [
        '<meta http-equiv="refresh" content="0; url=https://example.com/foo">',
        '<meta http-equiv="refresh" content="5; url=/foo">',
    ],
)
def test_resolve_html_redirect_base_url(html_string):
    html_el = lxml.html.fromstring(html_string, base_url="https://example.com")
    assert html.resolve_html_redirect(html_el) == httpx.URL("https://example.com/foo")


@pytest.mark.parametrize(
    "html_string",
    [
        '<meta http-equiv="refresh" content="0>',
        '<meta http-equiv="content-type" content="5; url=/foo">',
    ],
)
def test_resolve_html_redirect_no_redirect(html_string):
    html_el = lxml.html.fromstring(html_string, base_url="https://example.com")
    assert html.resolve_html_redirect(html_el) is None


@pytest.mark.parametrize(
    ("html_string", "metadata"),
    [
        (
            """<meta name="description" content="Review of periodical articles">
            <meta name="dc.identifier" content="doi:10.1017/S0963926820000012">
            <meta name="citation_doi" content="10.1017/S0963926820000012">""",
            [
                ("dc.identifier", "doi:10.1017/S0963926820000012"),
                ("citation_doi", "10.1017/S0963926820000012"),
            ],
        )
    ],
)
def test_metadata_from_html(html_string, metadata):
    html_el = lxml.html.fromstring(html_string, base_url="https://example.com")
    assert html.metadata_from_html(html_el) == metadata


def test_response_to_html():
    ...


def test_fulltext_urls_from_meta():
    ...
