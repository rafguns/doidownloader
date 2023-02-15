import sqlite3
import httpx
import pytest
import doidownloader
from unittest.mock import patch, Mock, call

class AsyncMock(Mock):
    def __call__(self, *args, **kwargs):
        sup = super()

        async def coro():
            return sup.__call__(*args, **kwargs)

        return coro()

@pytest.mark.asyncio
async def test_save_metadata_runs_concurrently():
    con = sqlite3.connect(":memory:")
    dois = [
        # 3 Springer DOIs
        "10.1007/9778-3-030-51406-8",
        "10.1007/978-1-0716-0916-3_16",
        "10.1007/978-3-030-26057-6",
        # 2 Wiley DOIs
        "10.1002/CL2.1084",
        "10.1002/CL2.1117",
        "10.1002/GEO2.100",
    ]
    client = httpx.AsyncClient()
    client.get = AsyncMock()

    res = await doidownloader.save_metadata(dois, con, client)
