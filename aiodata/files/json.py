import asyncio
from ..utils import open_file
try:
    import ujson as json
except ImportError:
    import json


async def read_json(filepath_or_buffer, compression="infer", encoding=None, loop=None, executor=None):

    if loop is None:
        loop = asyncio.get_event_loop()

    def sync_parse():
        with open_file(filepath_or_buffer, compression=compression, encoding=encoding) as f:
            return json.load(f)

    return await loop.run_in_executor(executor, sync_parse)
