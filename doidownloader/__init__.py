"""📝⬇️ DOIdownloader: You give it DOIs, it gives you the article PDFs.

It is surprisingly tricky to reliably obtain the full PDF of a scientific
publication given its DOI. This Python package aims to do just that: you give it a list
of DOIs, and it will download the full-text PDFs (or other formats if no PDF is
available), taking care of much of the complexity. It ensures that lookups to
different domains can happen asynchronously (i.e., one slow website won't stall all
your other downloads).

"""
from .doidownloader import (
    DOIDownloader,
    LookupResult,
    save_fulltexts_from_dois,
)
from .files import FileWithSameContentError, determine_filename, determine_filetype
