from collections.abc import Iterator, MutableMapping
from pathlib import Path


class CrawlDelays(MutableMapping):
    """Simple file-based cache for crawl delays"""

    def __init__(self, cache_filename: str) -> None:
        self.cache_filename = cache_filename
        self.delays = {}
        try:
            with open(self.cache_filename) as fh:
                for line in fh:
                    domain, delay = line.strip().split()
                    self.delays[domain] = int(delay)
        except FileNotFoundError:
            Path(cache_filename).touch()

    def __setitem__(self, key: str, value: int) -> None:
        with open(self.cache_filename, "a") as fh:
            fh.write(f"{key}\t{value}\n")
        self.delays[key] = value

    def __getitem__(self, key: str) -> int:
        return self.delays[key]

    def __contains__(self, key: str) -> bool:
        return key in self.delays

    def __delitem__(self, key: str) -> None:
        del self.delays[key]

    def __iter__(self) -> Iterator:
        return iter(self.delays)

    def __len__(self) -> int:
        return len(self.delays)
