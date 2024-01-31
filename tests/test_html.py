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


def test_metadata_from_html():
    ...


def test_response_to_html():
    ...


def test_fulltext_urls_from_meta():
    ...
