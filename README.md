# doidownloader

**doidownloader: You give it DOIs, it gives you the article PDFs.**

It is surprisingly tricky to reliably obtain the full PDF of a scientific
publication given its DOI. This Python package aims to do just that: you give it a list of
DOIs, and it will download the full-text PDFs (or other formats if no PDF is
available), taking care of much of the complexity. It ensures that lookups to
different domains can happen asynchronously (i.e., one slow website won't stall all
your other downloads).

Doidownloader gives precedence to the publisher-formatted version (the so-called ‘Version of Record’),
and will try downloading an open access pre- or postprint if you cannot access the publisher version.
Importantly, doidownloader only tries downloading through routes that are widely
considered to be legal. In more concrete terms, we do *not* download from Sci-Hub or
similar platforms.

## Installation

The package can be installed with `pip`:

```console
pip install git+https://github.com/rafguns/doidownloader.git
```

## Basic usage: command-line

The easiest way to get started is from the command-line.
If you have a plain-text file of DOIs named `dois.txt`, you can download their PDFs as follows:

```console
python -m doidownloader
```

This will download the results to a SQLite database named `doi-fulltexts.db` in the same directory.
(You may notice that this also created a file called `robots.txt`. This is used to keep track of how long
we should wait between calls to the same domain.)


## Advanced usage: Python

Here's an example of how to use this from within Python:

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

See `__main__.py` for an example of how to keep track of crawl delays per domain.