from setuptools import setup
import aiodata

setup(
    name='aiodata',
    version=aiodata.__version__,
    description='',
    author='Jan',
    url='',
    packages=['aiodata'],
    include_package_data=True,
    python_requires=">=3.6.0",
    install_requires=[
        'aiohttp',
        'aiofiles',
        'xmltodict',
        'sqlalchemy',
        'pyyaml',
        'pandas',
        'janus',
        'openapi-spec-validator',
    ],
    license='MIT',
    zip_safe=False,
    tests_require=[
        'pytest',
        'pytest-asyncio',
        'jsonschema',
    ],
    extras_require={
        'docs': [
            'sphinx'
        ],
        'postgres': [
            'asyncpg',
        ],
        'mssql': [
            'aioodbc',
        ],
        'mysql': [
            'aiomysql'
        ],
        'all': [
            'ujson',
            'cchardet',
            'aiodns',
        ]
    }
)
