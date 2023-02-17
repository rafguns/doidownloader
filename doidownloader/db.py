import sqlite3


def prepare_tables(con: sqlite3.Connection) -> None:
    # Field error specifies what kind of error (if any) has occurred
    # (e.g. no content, connection error, HTTP error).
    # Field status_code is for HTTP status code, including HTTP errors.
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


def dois_with_fulltext(con: sqlite3.Connection) -> set:
    return {
        row[0]
        for row in con.execute(
            "select doi from doi_fulltext where error is null"
        ).fetchall()
    }
