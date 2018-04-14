from contextlib import contextmanager
from sqlalchemy.schema import MetaData, CreateTable, DropTable
from sqlalchemy import Table, Column, PrimaryKeyConstraint
from sqlalchemy.types import BigInteger, Integer, Float, Text, Boolean, DateTime, Date, Time
from ..api.spec import FieldType


async def create_table(model, name, engine, schema=None, if_exists='fail'):
    if if_exists not in ('fail', 'replace', 'append'):
        raise ValueError(f"'{if_exists}' is not valid for if_exists")

    db = SQLDatabase(engine, schema=schema)

    table = SQLTable(name, db, model=model, if_exists=if_exists, schema=schema)
    await table.create()
    return table


class SQLDatabase:
    """
    This class enables conversion between Python dicts and SQL databases
    using SQLAlchemy to handle DataBase abstraction.
    Parameters
    ----------
    engine : SQLAlchemy connectable
        Connectable to connect with the database. Using SQLAlchemy makes it
        possible to use any DB supported by that library.
    schema : string, default None
        Name of SQL schema in database to write to (if database flavor
        supports this). If None, use default schema (default).
    meta : SQLAlchemy MetaData object, default None
        If provided, this MetaData object is used instead of a newly
        created. This allows to specify database flavor specific
        arguments in the MetaData object.
    """

    def __init__(self, engine, schema=None):
        self.engine = engine
        self.meta = MetaData(schema=schema)

    @contextmanager
    def run_transaction(self):
        with self.engine.begin() as tx:
            if hasattr(tx, 'execute'):
                yield tx
            else:
                yield self.engine

    def execute(self, *args, **kwargs):
        """Simple passthrough to SQLAlchemy connectable"""
        return self.engine.execute(*args, **kwargs)

    @property
    def tables(self):
        return self.meta.tables

    async def has_table(self, name, schema=None):
        if schema is None:
            schema = self.meta.schema
        async with self.engine.connect() as connection:
            if schema is None:
                result = await connection.fetchval(
                    "SELECT 1 FROM information_schema.tables"
                    f" WHERE table_name = '{str(name)}'"
                )
            else:
                result = await connection.fetchval(
                    "SELECT 1 FROM information_schema.tables"
                    f" WHERE table_schema = '{str(schema)}'"
                    f" AND table_name = '{str(name)}'"
                )
            return bool(result)

    def get_table(self, table_name, schema=None):
        schema = schema or self.meta.schema
        if schema:
            tbl = self.meta.tables.get('.'.join([schema, table_name]))
        else:
            tbl = self.meta.tables.get(table_name)

        # Avoid casting double-precision floats into decimals
        from sqlalchemy import Numeric
        for column in tbl.columns:
            if isinstance(column.type, Numeric):
                column.type.asdecimal = False

        return tbl

    async def drop_table(self, table_name, schema=None):
        schema = schema or self.meta.schema
        if self.has_table(table_name, schema):
            async with self.engine.connect() as connection:
                query = DropTable(Table(table_name, self.meta, schema=schema)).compile(dialect=self.engine.dialect())
                await connection.execute(str(query))

    def _create_sql_schema(self, model, table_name, keys=None, dtype=None):
        table = SQLTable(table_name, self, model=model)
        return str(table.sql_schema())


class SQLTable:

    def __init__(self, name, sql_database, model=None, if_exists='fail', schema=None, keys=None):
        self.name = name
        self.db = sql_database
        self.model = model
        self.schema = schema
        self.if_exists = if_exists
        self.keys = keys

        if model is not None:
            # We want to initialize based on a model
            self.table = self._create_table_setup()
        else:
            # no data provided, read-only mode
            self.table = self.db.get_table(self.name, self.schema)

        if self.table is None:
            raise ValueError("Could not init table '%s'" % name)

    async def exists(self):
        return await self.db.has_table(self.name, self.schema)

    def sql_schema(self):
        return str(CreateTable(self.table).compile(dialect=self.db.pool.dialect()))

    async def _execute_create(self):
        self.table = self.table.tometadata(self.db.meta)
        async with self.db.pool.connect() as connection:
            await connection.execute(self.sql_schema())

    async def create(self):
        if await self.exists():
            if self.if_exists == 'fail':
                raise ValueError("Table '%s' already exists." % self.name)
            elif self.if_exists == 'replace':
                await self.db.drop_table(self.name, self.schema)
                await self._execute_create()
            elif self.if_exists == 'append':
                pass
            else:
                raise ValueError(
                    "'{0}' is not valid for if_exists".format(self.if_exists))
        else:
            await self._execute_create()

    def insert_statement(self, data=None):
        ins = self.table.insert()
        if data is not None:
            ins = ins.values(**data)
        return str(ins.compile(dialect=self.db.pool.dialect()))

    async def insert(self, data):
        query = self.insert_statement(data)
        async with self.db.connect() as connection:
            await connection.execute(query)

    def _get_column_names_and_types(self):
        field_type_mapping = {
            FieldType.BOOLEAN: Boolean,
            FieldType.STRING: Text,
            FieldType.INTEGER: Integer,
            FieldType.DECIMAL: Float,
            FieldType.DATETIME: DateTime,
            FieldType.TIME: Time,
            FieldType.DATE: Date,
            FieldType.OBJECT: Text,
            FieldType.ARRAY: Text
        }

        return [(f.name, field_type_mapping[f.type]) for f in self.model.fields]

    def _create_table_setup(self):
        column_names_and_types = self._get_column_names_and_types()

        columns = [Column(name, typ) for name, typ in column_names_and_types]

        if self.keys is not None:
            if not isinstance(self.keys, dict):
                keys = [self.keys]
            else:
                keys = self.keys
            pkc = PrimaryKeyConstraint(*keys, name=self.name + '_pk')
            columns.append(pkc)

        schema = self.schema or self.db.meta.schema

        meta = MetaData(schema=schema)

        return Table(self.name, meta, *columns, schema=schema)
