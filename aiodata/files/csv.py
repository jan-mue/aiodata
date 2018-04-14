import pandas as pd
import asyncio
import janus


async def read_csv(filepath_or_buffer, chunksize=10000, loop=None, executor=None, **kwargs):

    if loop is None:
        loop = asyncio.get_event_loop()

    queue = janus.Queue(loop=loop)

    def sync_parse(sync_q):
        for chunk in pd.read_csv(filepath_or_buffer, chunksize=chunksize, **kwargs):
            sync_q.put(chunk)
        sync_q.put(None)

    loop.run_in_executor(executor, sync_parse, queue.sync_q)

    q = queue.async_q

    while True:
        chunk = await q.get()
        if chunk is None:
            break
        for index, row in chunk.iterrows():
            yield row.to_dict()
        q.task_done()
