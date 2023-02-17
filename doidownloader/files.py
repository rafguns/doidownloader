import hashlib
import os

# Mapping of MIME type to basic file type/extension
mime_types = {
    "application/pdf": "pdf",
    "application/xml": "xml",
    "text/xml": "xml",
    "text/html": "html",
    "text/plain": "txt",
    "application/epub+zip": "epub",
    "application/json": "json",
    "image/png": "png",
}


class FileWithSameContentError(Exception):
    """Exception: a file with the same contents already exists."""


def determine_extension(content_type: str, content: bytes) -> str:
    try:
        content_type = content_type.split(";")[0]
        return mime_types[content_type]
    except (AttributeError, KeyError):
        # Unknown or missing content type; guess by content sniffing
        if content[:4] == b"%PDF":
            return "pdf"
        if content.startswith(b"<article"):
            return "xml"

        return "unknown"


def same_contents(fname: str, bytestring: bytes) -> bool:
    """Check if contents of file are same as bytestring."""
    with open(fname, "rb") as fh:
        hash_file = hashlib.md5(fh.read()).digest()
    hash_bytestring = hashlib.md5(bytestring).digest()

    return hash_file == hash_bytestring


def determine_filename(
    basename: str, ext: str, content: bytes, extra_letter: str = ""
) -> str:
    fname = f"{basename}{extra_letter}.{ext}"

    if not os.path.exists(fname):
        return fname

    if same_contents(fname, content):
        err = f"File {fname} has same contents."
        raise FileWithSameContentError(err)

    # There is already a file with the same name but different contents
    extra_letter = "a" if extra_letter == "" else chr(ord(extra_letter) + 1)

    return determine_filename(basename, ext, content, extra_letter)
