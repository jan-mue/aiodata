try:
    import ujson as json
except ImportError:
    import json

import aiohttp
import aiofiles
import yaml
from typing import Optional, Union, Iterable
from urllib.parse import urljoin
from functools import reduce
from .resource import ResourceIterable, ResourceObject
from ..utils import returns, get_content_type
from .spec import OpenAPISpec
from .spec.base import Spec, Endpoint
import xmltodict
import logging


class APISession:

    def __init__(self, base_url: Optional[str] = None, api_spec: Optional[Spec] = None,
                 session: Optional[aiohttp.ClientSession] = None):
        self.logger = logging.getLogger(__name__)
        if session is None:
            session = aiohttp.ClientSession(json_serialize=json.dumps)
        self.session = session
        if api_spec is not None:
            self.spec = api_spec
            base_url = api_spec.api_url
        self.base_url = base_url

    @classmethod
    async def from_url(cls, spec_url, session=None):
        async with session or aiohttp.ClientSession() as ses:
            async with ses.get(spec_url) as ret:
                return cls(api_spec=OpenAPISpec(await cls._parse_response(ret), spec_url, session))

    @classmethod
    async def from_filename(cls, filename):
        async with aiofiles.open(filename) as f:
            return cls(api_spec=yaml.safe_load(await f.read()))

    @staticmethod
    async def _parse_response(response):
        response.raise_for_status()
        content_type = response.headers.get(aiohttp.hdrs.CONTENT_TYPE, "").lower()
        ct = get_content_type(content_type, str(response.url))
        if ct == "json":
            return await response.json(loads=json.loads)
        if ct == "xml":
            return xmltodict.parse(await response.text())
        if ct == "yaml":
            return yaml.safe_load(await response.text())
        if "text" in content_type:
            return await response.text()
        return response

    async def request(self, method, url, params=None, json=None, file=None):
        if file is not None:
            data = {'file': file}
        else:
            data = None
        async with self.session.request(method, url, params=params, data=data, json=json) as response:
            return await self._parse_response(response)

    async def close(self):
        await self.session.close()

    def __repr__(self):
        return f"{self.__class__.__name__}({self.base_url})"

    def __getattr__(self, name):
        if self.spec is None:
            return super(APISession, self).__getattr__(name)
        endpoint = self.spec.endpoints.get(name)
        if not endpoint:
            raise AttributeError(f"No endpoint '{name}'")
        return API(session=self, endpoint=endpoint)

    def __dir__(self):
        return super().__dir__() + list((self.spec or []) and self.spec.endpoints.keys())


class API:

    def __init__(self, session: APISession, endpoint: Union[str, Endpoint], model=None):
        self.session = session
        self.endpoint = endpoint
        self.model = model

    def make_url(self, *args: str):
        fragments = [self.session.base_url, self.endpoint]
        fragments += [a.strip("/") for a in args]
        return reduce(urljoin, fragments)

    async def get(self, *args, **params):
        return await self.session.request("GET", self.make_url(*args), params=params)

    async def put(self, *args, json=None, file=None, params=None):
        return await self.session.request("PUT", self.make_url(*args), json=json, file=file, params=params)

    async def post(self, *args, json=None, file=None, params=None):
        return await self.session.request("POST", self.make_url(*args), json=json, file=file, params=params)

    async def delete(self, *args, **params):
        return await self.session.request("DELETE", self.make_url(*args), params=params)

    async def get_by_id(self, uid):
        ret = await self.get(uid)
        return ResourceObject(data=ret, api=self)

    @returns(ResourceIterable)
    async def create_multiple(self, data: Iterable[dict]):
        items = ResourceIterable([ResourceObject(data=d, api=self) for d in data])
        async for item in items.create():
            yield item

    @returns(ResourceIterable)
    async def list(self):
        items = await self.get()
        for item in items:
            yield ResourceObject(data=item, api=self)

    def create_sub_api(self, path:str):
        return API(self.session, "/".join([str(self.endpoint).rstrip("/"), path.lstrip("/")]))

    def __repr__(self):
        return f"{self.__class__.__name__}({self.endpoint})"

    def __getattr__(self, name):
        if isinstance(self.endpoint, str):
            return super(API, self).__getattr__(name)

        operation = getattr(self.endpoint, name, None)
        if not operation:
            raise AttributeError(f"No operation {name}")

        async def call_operation(**kwargs):
            url = self.make_url(operation.path_name)
            ret = await self.session.request(str(operation.http_method.upper()), url, **kwargs)
            if isinstance(ret, list):
                return ResourceIterable(ResourceObject(data=item, api=self) for item in ret)
            return ResourceObject(data=ret, api=self)

        return call_operation

    def __dir__(self):
        d = super().__dir__()
        if isinstance(self.endpoint, str):
            return d
        return d + dir(self.endpoint)
