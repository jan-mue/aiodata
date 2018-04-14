import aiohttp
import yaml
import json
from contextlib import closing
from urllib.request import urlopen
from urllib.parse import urlparse, urlunparse
from .base import Spec, Model, Field, Operation, Endpoint, FieldType
from openapi_spec_validator import validate_spec
from jsonschema.validators import RefResolver
from ...utils import get_content_type

type_mapping = {
    "string": FieldType.STRING,
    "integer": FieldType.INTEGER,
    "number": FieldType.DECIMAL,
    "array": FieldType.ARRAY,
    "boolean": FieldType.BOOLEAN
}


class OpenAPISpec(Spec):

    def __init__(self, spec_dict, spec_url=None, session=None):
        self.spec_url = spec_url

        if session is None:
            session = aiohttp.ClientSession()

        def get(uri):
            async def a_get(url):
                ret = await session.get(url)
                if get_content_type(ret.headers.get(aiohttp.hdrs.CONTENT_TYPE, ""), url) == "yaml":
                    return yaml.safe_load(await ret.text())
                else:
                    return await ret.json()
            return session.loop.run_until_complete(a_get(uri))

        def read_file(uri):
            with closing(urlopen(uri)) as f:
                if get_content_type(url=uri) == "yaml":
                    return yaml.safe_load(f)
                else:
                    return json.loads(f.read().decode("utf-8"))

        handlers = {
            "http": get,
            "https": get,
            "file": read_file
        }

        self.resolver = RefResolver(base_uri=spec_url or "", referrer=spec_dict, handlers=handlers)

        # validate_spec(spec_dict, spec_url=spec_url or "")

        self.models = {name: OpenAPIModel(fragment) for name, fragment in spec_dict["definitions"].items()}

        super(OpenAPISpec, self).__init__(spec_dict)

    @property
    def api_url(self):
        url = urlparse(self.spec_url or "http://localhost")
        netloc = self.spec_dict.get('host', url.netloc)
        path = self.spec_dict.get('basePath', url.path)
        schemes = self.spec_dict.get('schemes')
        scheme = url.scheme if not schemes or url.scheme in schemes else schemes[0]
        return urlunparse((scheme, netloc, path, None, None, None))

    @property
    def endpoints(self):
        return {path: OpenAPIEndpoint(spec) for path, spec in self.spec_dict["paths"].items()}


class OpenAPIEndpoint(Endpoint):

    @property
    def model(self):
        return OpenAPIModel({"properties":{}})

    @property
    def operations(self):
        return {op.get("operationId", method): OpenAPIOperation(method, op) for method, op in self.spec_dict.items()}


class OpenAPIOperation(Operation):
    pass


class OpenAPIModel(Model):

    @property
    def operations(self):
        return []

    @property
    def fields(self):
        return [Field(name, type_mapping[spec["type"]]) for name, spec in self.spec_dict["properties"].items()]
