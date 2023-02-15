import os
import hashlib

file_types = [
    # extension, MIME type, GS meta field
    ("pdf", "application/pdf", "citation_pdf_url"),
    ("xml", "application/xml", "citation_xml_url"),
    ("xml", "text/xml", "citation_xml_url"),
    ("html", "text/html", "citation_full_html_url"),
    # Note: We do NOT include "citation_fulltext_html_url". This is used by Springer to
    # refer to landing pages rather than proper full-text documents.
    ("txt", "text/plain", None),
    ("epub", "application/epub+zip", None),
    ("json", "application/json", None),
    ("png", "image/png", None),
]


class FileWithSameContentExists(Exception):
    """Exception: a file with the same contents already exists."""


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
    """Check if contents of file are same as bytestring."""
    hash_file = hashlib.md5(open(fname, "rb").read()).digest()
    hash_bytestring = hashlib.md5(bytestring).digest()

    return hash_file == hash_bytestring


def determine_filename(
    basename: str, ext: str, content: bytes, extra_letter: str = ""
) -> str:
    fname = f"{basename}{extra_letter}.{ext}"

    if not os.path.exists(fname):
        return fname

    if same_contents(fname, content):
        raise FileWithSameContentExists(f"File {fname} has same contents.")

    # There is already a file with the same name but different contents
    extra_letter = "a" if extra_letter == "" else chr(ord(extra_letter) + 1)

    return determine_filename(basename, ext, content, extra_letter)
