import os
import yaml

import pandas as pd
from sqlalchemy import create_engine


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


class DB(DataProvider):
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

    def sql(self, query, limit=2000):
        # emergency limiting so nothing will explode :)
        if 'limit' not in query.lower():
            query = query.rstrip(';') + f' LIMIT {limit};'

        return pd.read_sql(query, self._engine)

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

    
def get_provider_from_config(d, key=None, lvl=0):
    root = DataProvider(name=key or 'GIVEMEDATA', cs=None, members=[], lvl=lvl)
    next_lvl = root._lvl + 1

    for key, value in d.items():
        # node will be either a connection string or will have an inner structure of nodes
        if isinstance(value, dict):
            setattr(root, key, get_provider_from_config(value, key=key, lvl=next_lvl))
            root._members.append(getattr(root, key))
        else:
            setattr(root, key, DB(name=key, cs=value, members=[], lvl=next_lvl))
            root._members.append(getattr(root, key))

    return root


CONFIG_SOURCES = [
    '~/.givemedata.yaml',
    '~/givemedata.yaml',
    '/etc/givemedata/.givemedata.yaml',
    '/etc/givemedata/givemedata.yaml',
]


Data = None
dict_config = None
for config_source in CONFIG_SOURCES:
    try:
        with open(os.path.expanduser(config_source), 'r') as f:
            data = f.read()
            print(f'[GIVEMEDATA] Using config file at {config_source}')
            dict_config = yaml.load(data)
            break
    except FileNotFoundError:
        continue


if dict_config:
    Data = get_provider_from_config(dict_config)
else:
    print('[GIVEMEDATA] Could not configure data provider. You can use get_provider_from_config function to pass a configuration object manualy')
