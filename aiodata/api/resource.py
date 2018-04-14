import asyncio
import collections
from typing import AsyncIterable, Iterable, Union
from ..utils import returns, chain


class ResourceObject(collections.UserDict):

    def __init__(self, data=None, api=None):
        super().__init__(data)
        self.api = api

    @property
    def id(self):
        return self.get("id")

    @id.setter
    def id(self, value):
        self["id"] = value

    async def create(self):
        ret = await self.api.post(json=self.data)
        self.data.update(ret)
        return self

    async def load(self):
        ret = await self.api.get(self.id)
        self.data.update(ret)
        return self

    async def commit(self):
        ret = await self.api.put(json=self.data)
        self.data.update(ret)
        return self

    async def delete(self):
        await self.api.delete(self.id)

    async def to_sql(self, table):
        await table.insert(self.data)
        return self


class ResourceIterable(collections.AsyncIterable):

    def __init__(self, *iterables: Union[AsyncIterable[ResourceObject], Iterable[ResourceObject]]):
        self.data = chain(*iterables)

    async def __aiter__(self):
        async for item in self.data:
            yield item

    @returns
    async def create(self):
        futures = []
        async for item in self:
            futures.append(asyncio.ensure_future(item.create()))
        for f in asyncio.as_completed(futures):
            yield f.result()

    @returns
    async def commit(self):
        futures = []
        async for item in self:
            futures.append(asyncio.ensure_future(item.commit()))
        for f in asyncio.as_completed(futures):
            yield f.result()

    @returns
    async def delete(self):
        futures = []
        async for item in self:
            futures.append(asyncio.ensure_future(item.delete()))
        for f in asyncio.as_completed(futures):
            yield f.result()

    @returns
    async def to_sql(self, table):
        futures = [item.to_sql(table) async for item in self]
        for f in asyncio.as_completed(futures):
            yield f.result()

    def first(self):
        it = self.__aiter__()
        try:
            return it.__anext__()
        except StopAsyncIteration:
            return None

    async def all(self):
        return [item async for item in self]

    async def sorted(self, key=None, reverse=False):
        return sorted(await self.all(), key=key, reverse=reverse)

    @returns
    async def filter(self, predicate):
        async for item in self:
            if predicate(item):
                yield item

    @returns
    async def map(self, func):
        async for item in self:
            yield func(item)

    @returns
    async def distinct(self):
        items = []
        async for item in self:
            if item in items:
                continue
            items.append(item)
            yield item

