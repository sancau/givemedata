import os
import platform
import yaml

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# the below configuration specifies the places for Givemedata to search for config
PLATFORM = platform.system()

CONFIG_DIRS = []

if PLATFORM == 'Windows':
    home, appdata, programdata = map(os.getenv, ['HOME', 'APPDATA', 'PROGRAMDATA'])
    if home:
        CONFIG_DIRS.append(Path(home))
    if appdata:
        CONFIG_DIRS.append(Path(appdata))
    if programdata:
        CONFIG_DIRS.append(Path(programdata))

elif PLATFORM == 'Darwin':
    home = os.getenv('HOME')
    if home:
        CONFIG_DIRS.append(Path(home))
        CONFIG_DIRS.append(Path(home) / Path('Library') / Path('givemedata'))
    CONFIG_DIRS.append(Path('/Library') / Path('givemedata'))

elif PLATFORM == 'Linux':
    home = os.getenv('HOME')
    if home:
        CONFIG_DIRS.append(Path(home))
    CONFIG_DIRS.append(Path('/etc') / Path('givemedata'))

else:
    raise ValueError(f'Unsupported platform: {PLATFORM}')

# if GIVEMEDATA_CONFIG_DIR env variable is specified use it with priority
custom_dir = os.getenv('GIVEMEDATA_CONFIG_DIR')
if custom_dir:
    CONFIG_DIRS.insert(0, Path(custom_dir))

POSSIBLE_CONFIG_FILE_NAMES = [
    Path('.givemedata.yaml'),
    Path('.givemedata.yml'),
    Path('givemedata.yaml'),
    Path('givemedata.yml'),
]

CONFIG_SOURCES = []
for path in CONFIG_DIRS:
    for filename in POSSIBLE_CONFIG_FILE_NAMES:
        CONFIG_SOURCES.append(path / filename)


class DataProvider:
    def __init__(self, *, name, cs, members, lvl=0):
        assert isinstance(cs, str) or isinstance(cs, type(None)), (cs, 'should be a string or None')
        assert isinstance(name, str), (name, 'should be a string.')

        self._cs = cs
        self._name = name
        self._members = members
        self._lvl = lvl

    def __str__(self):
        spaces = '    ' * self._lvl
        arrow = '=>' if not self._members else ''
        s = f'\n{spaces}{self._name} {arrow} {self._cs or [m for m in self._members]}'
        return s.replace('[', '').replace(']', '').replace(',', '')

    def __repr__(self):
        return self.__str__()


class TablesList(pd.DataFrame):
    def __init__(self, df, *, engine):
        super().__init__(df)
        self._engine = engine

    def __get_table_name(self, arg):
        if isinstance(arg, int):
            return self['table_name'].iloc[arg]
        else:
            assert arg in self['table_name'].tolist(), f'[GIVEMEDATA] No table with the name {arg}'
            return arg

    def sample(self, index_or_name, limit=5):
        table_name = self.__get_table_name(index_or_name)

        print(f'\n\n[TABLE SAMPLE: {table_name}]\n')

        query = f"""
        select * from {table_name} limit {limit}
        """
        return pd.read_sql(query, self._engine)

    def describe(self, index_or_name):
        table_name = self.__get_table_name(index_or_name)

        print(f'\n\n[TABLE STRUCTURE: {table_name}]\n')

        return pd.merge(
            pd.read_sql(f"""
                select
                 *
                from
                 {table_name}
                limit 1;
                """,
                self._engine,
            ).T.reset_index().rename(
                columns={
                    'index': 'column_name',
                    0: 'example_value',
                },
            ),
            pd.read_sql(f"""
                select
                 column_name, data_type, is_nullable
                from
                 information_schema.columns
                where
                 table_name = '{table_name}';
                """,
                self._engine,
            ),
            how='left',
            on='column_name',
        ).loc[:, ['column_name', 'data_type', 'is_nullable']].set_index('column_name')

    def search(self, term):
        return TablesList(
            self.loc[
                self['table_name'].str.contains(term.lower()),
                :
            ].reset_index(drop=True),
            engine=self._engine
        )


class FieldsList(pd.DataFrame):
    def __init__(self, df, *, engine):
        super().__init__(df)
        self._engine = engine

    def search(self, term):
        return FieldsList(
            self.loc[
            self['column_name'].str.contains(term.lower()),
            :
            ].reset_index(drop=True),
            engine=self._engine
        )


class PostgresDB(DataProvider):
    def __init__(self, *, name, cs, members, lvl=0):
        super().__init__(name=name, cs=cs, members=members, lvl=lvl)
        self._engine = create_engine(cs)

    @property
    def public_tables(self):
        return TablesList(
            pd.read_sql("""
                select table_name
                from information_schema.tables
                where table_schema='public'
                and table_type='BASE TABLE'
                """,
                self._engine,
            ),
            engine=self._engine,
        )

    @property
    def all_tables(self):
        return TablesList(
            pd.read_sql("""
                select table_name
                from information_schema.tables
                """,
                self._engine,
            ),
            engine=self._engine,
        )

    def sql(self, query, limit=None):
        # emergency limiting so nothing will explode :)
        if limit is not None and 'limit' not in query.lower():
            query = query.replace('\n', ' ').strip().rstrip(';') + f' limit {limit};'

        return pd.read_sql(query, self._engine)

    def get_connection(self):
        return self._engine.connect()

    def get_engine(self):
        return self._engine

    @property
    def public_fields(self):
        return FieldsList(
            pd.read_sql(
                """
                select column_name, table_name, data_type, is_nullable from information_schema.columns where table_schema = 'public';
                """,
                self._engine,
            ),
            engine=self._engine,
        )

    @property
    def all_fields(self):
        return FieldsList(
            pd.read_sql(
                """
                select column_name, table_name, data_type, is_nullable from information_schema.columns;
                """,
                self._engine,
            ),
            engine=self._engine,
        )


CASSANDRA_CONNECT_TIMEOUT_DEFAULT = 20
CASSANDRA_CONTROL_CONNECTION_TIMEOUT_DEFAULT = 60
CASSANDRA_DEFAULT_FETCH_SIZE = 5000


def parse_cassandra_cs(cs: str) -> dict:
    assert cs.split(':')[0] == 'cassandra', ('Invalid DB type', cs)

    cs = cs.split('//')[1]
    credentials, cs = cs.split('@')

    username, password = credentials.split(':')
    hosts, keyspace = cs.split('/')
    hosts = hosts.split('--')

    return dict(
        hosts=hosts,
        keyspace=keyspace,
        username=username,
        password=password,
    )


class CassandraDB(DataProvider):
    def __init__(self, *, name, cs, members, lvl=0):
        super().__init__(name=name, cs=cs, members=members, lvl=lvl)

        self.connection = None
        self.cassandra_config = parse_cassandra_cs(cs=cs)

    def setup_global(self):
        # cassandra driver imports
        # make it optional so givemedata configs with no cassandra DBs will work without the driver
        from cassandra.cqlengine import connection
        from cassandra.auth import PlainTextAuthProvider

        self.connection = connection
        self.connection.setup(
            self.cassandra_config['hosts'],
            default_keyspace=self.cassandra_config['keyspace'],
            auth_provider=PlainTextAuthProvider(
                username=self.cassandra_config['username'],
                password=self.cassandra_config['password'],
            ),
            connect_timeout=CASSANDRA_CONNECT_TIMEOUT_DEFAULT,
            control_connection_timeout=CASSANDRA_CONTROL_CONNECTION_TIMEOUT_DEFAULT,
        )

    def cql(self, query: str) -> pd.DataFrame:
        # cassandra driver imports
        # make it optional so givemedata configs with no cassandra DBs will work without the driver
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        cluster = Cluster(
            self.cassandra_config['hosts'],
            auth_provider=PlainTextAuthProvider(
                username=self.cassandra_config['username'],
                password=self.cassandra_config['password'],
            ),
            connect_timeout=CASSANDRA_CONNECT_TIMEOUT_DEFAULT,
            control_connection_timeout=CASSANDRA_CONTROL_CONNECTION_TIMEOUT_DEFAULT,
        )

        with cluster.connect(self.cassandra_config['keyspace']) as session:
            session.default_fetch_size = CASSANDRA_DEFAULT_FETCH_SIZE
            q_result = session.execute(query)
            return pd.DataFrame(
                q_result.current_rows,
                columns=q_result.column_names,
            )


def get_provider_class(cs):
    # if cassandra return Cassandra etc
    db_type = cs.split(':')[0]
    return {
        'postgresql': PostgresDB,
        'cassandra': CassandraDB,
    }[db_type]


def get_provider_from_config(d, key=None, lvl=0):
    root = DataProvider(name=key or 'GIVEMEDATA', cs=None, members=[], lvl=lvl)
    next_lvl = root._lvl + 1

    for key, value in d.items():
        # node will be either a connection string or will have an inner structure of nodes
        if isinstance(value, dict):
            setattr(root, key, get_provider_from_config(value, key=key, lvl=next_lvl))
            root._members.append(getattr(root, key))
        else:
            db_class: type(DataProvider) = get_provider_class(value)
            setattr(root, key, db_class(name=key, cs=value, members=[], lvl=next_lvl))
            root._members.append(getattr(root, key))

    return root


def try_get_config_from_default_sources():
    for config_source in CONFIG_SOURCES:
        try:
            with open(os.path.expanduser(config_source), 'r') as f:
                print(f'[GIVEMEDATA] Using config file at {config_source}')
                return yaml.load(f.read(), Loader=yaml.SafeLoader)
        except FileNotFoundError:
            continue


def init_provider():
    config = try_get_config_from_default_sources()
    if config:
        return get_provider_from_config(config)

    print('[GIVEMEDATA] Could not configure data provider.'
          ' You can use get_provider_from_config function to pass a configuration object manually')


Data = init_provider()
