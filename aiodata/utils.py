import os
import re
import asyncio
import aiohttp
import aiofiles
import gzip
import zipfile
import bz2
import lzma
from pandas.io.common import get_filepath_or_buffer, _infer_compression
from functools import wraps
from itertools import zip_longest
from collections import AsyncIterable


def returns(*classes):
    def wrapper(func):

        def cast_result(args, result):
            results = zip_longest(result if isinstance(result, tuple) else (result,), classes)
            results = [(v, c or (args[0] if isinstance(args[0], type) else args[0].__class__)) for v, c in results]
            results = tuple(v if isinstance(v, c) or v is None else c(v) for v, c in results)
            return results[0] if len(results) == 1 else results

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapped(*args, **kwargs):
                result = await func(*args, **kwargs)
                return cast_result(args, result)

            return wrapped

        @wraps(func)
        def wrapped(*args, **kwargs):
            result = func(*args, **kwargs)
            return cast_result(args, result)

        return wrapped

    if len(classes) == 1 and not isinstance(classes[0], type) and callable(classes[0]):
        return wrapper(classes[0])

    return wrapper


async def chain(*iterables):
    for it in iterables:
        if isinstance(it, AsyncIterable):
            async for element in it:
                yield element
        else:
            for element in it:
                yield element


def open_file(filepath_or_buffer, mode="r", encoding=None, compression="infer"):
    if encoding is not None:
        encoding = re.sub("_", "-", encoding).lower()

    compression = _infer_compression(filepath_or_buffer, compression)
    filepath_or_buffer, _, compression = get_filepath_or_buffer(filepath_or_buffer, encoding, compression)

    is_path = isinstance(filepath_or_buffer, str)

    if compression:

        # GZ Compression
        if compression == "gzip":
            if is_path:
                return gzip.open(filepath_or_buffer, mode)
            return gzip.GzipFile(fileobj=filepath_or_buffer)

        # BZ Compression
        elif compression == "bz2":
            if is_path:
                return bz2.BZ2File(filepath_or_buffer, mode)
            return bz2.BZ2File(filepath_or_buffer)

        # ZIP Compression
        elif compression == "zip":
            zip_file = zipfile.ZipFile(filepath_or_buffer)
            zip_names = zip_file.namelist()
            if len(zip_names) == 1:
                return zip_file.open(zip_names.pop())
            if len(zip_names) == 0:
                raise ValueError(f"Zero files found in ZIP file {filepath_or_buffer}")
            else:
                raise ValueError("Multiple files found in ZIP file."
                                 f" Only one file per ZIP: {filepath_or_buffer}")

        # XZ Compression
        elif compression == "xz":
            return lzma.LZMAFile(filepath_or_buffer, mode)

        # Unrecognized Compression
        raise ValueError(f"Unrecognized compression type: {compression}")

    elif is_path:
        return open(filepath_or_buffer, mode, encoding=encoding)


async def download(session, url, *, download_dir=None, params=None, chunk_size=100*1024, overwrite=False):
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        content_disposition = response.headers.get(aiohttp.hdrs.CONTENT_DISPOSITION)
        if content_disposition is None:
            u = response.url
            filename = os.path.basename(u.path)
        else:
            disptype, params = aiohttp.parse_content_disposition(content_disposition)
            filename = params["filename"]
        if download_dir is not None:
            filename = os.path.join(download_dir, filename)

        new_filename = filename
        i = 1
        while not overwrite and os.path.isfile(new_filename):
            path, ext = os.path.splitext(filename)
            new_filename = f"{path}({i}){ext}"
            i += 1
        filename = new_filename

        size = response.headers.get(aiohttp.hdrs.CONTENT_LENGTH)
        size = int(size or chunk_size)
        if size < chunk_size:
            chunk_size = size

        async with aiofiles.open(filename, "wb") as f:
            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                await f.write(chunk)

        return filename


def get_content_type(content_type: str = "", url: str = ""):
    content_types = {
        "yaml": {"application/yaml", "application/x-yaml", "text/yaml"},
        "xml": {"application/xml", "text/xml"},
        "json": {"application/json"},
    }

    file_extensions = {
        "yaml": {".yaml", ".yml"},
        "xml": {".xml"},
        "json": {".json"}
    }

    for t, cts in content_types.items():
        if content_type.lower() in cts:
            return t

    _, ext = os.path.splitext(url)
    for t, exts in file_extensions.items():
        if ext.lower() in exts:
            return t
