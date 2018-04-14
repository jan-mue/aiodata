import xmltodict
import asyncio
import janus
from ..utils import open_file


async def read_xml(filepath_or_buffer, compression="infer", encoding=None, loop=None, executor=None, **kwargs):

    if loop is None:
        loop = asyncio.get_event_loop()

    queue = janus.Queue(loop=loop)

    def sync_parse(sync_q):
        def item_callback(path, item):
            sync_q.put((path, item))
            return True
        with open_file(filepath_or_buffer, compression=compression, encoding=encoding) as f:
            xmltodict.parse(f, item_callback=item_callback, **kwargs)
            sync_q.put((None, None))

    loop.run_in_executor(executor, sync_parse, queue.sync_q)

    q = queue.async_q

    while True:
        p, it = await q.get()
        if it is None:
            break
        yield p, it
        q.task_done()
