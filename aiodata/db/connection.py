import sqlalchemy
from functools import partial


async def create_engine(*args, **kwargs):
    engine = sqlalchemy.create_engine(*args, **kwargs)

    if engine.driver == "psycopg2":
        import asyncpg
        p = await asyncpg.create_pool(str(engine.url))
    elif engine.driver == "pyodbc":
        import aioodbc
        p = await aioodbc.create_pool(**engine.url.translate_connect_args())
    elif engine.driver == "mysqldb":
        import aiomysql
        p = aiomysql.create_pool(**engine.url.translate_connect_args())
    else:
        p = engine.pool

    old_creator = engine.pool._creator

    def creator(*a, **kw):
        result = old_creator(*a, **kw)

        async def aenter(self):
            self._async_conn = p.acquire()
            return await self._async_conn.__aenter__()

        async def aexit(self):
            return await self._async_conn.__aexit__()

        result.__aenter__ = partial(aenter, result)
        result.__aexit__ = partial(aexit, result)

    engine.pool._creator = creator

    return engine
