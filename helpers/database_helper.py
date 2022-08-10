import os
import sqlalchemy
import yaml

import pandas as pd

_ENGINES = {}


def get_db_credentials_dict(credential_file_path=os.path.join(os.path.dirname(__file__),'db_credentials.yaml')):
    with open(credential_file_path, 'r') as credential_file:
        return yaml.safe_load(credential_file)


CREDENTIAL_DICTIONARY = get_db_credentials_dict()


def get_stocky_db_engine(credential_name):
    if credential_name not in _ENGINES:
        _create_engine(credential_name)

    return _ENGINES[credential_name]


def _create_engine(credential_name):
    db_credentials = CREDENTIAL_DICTIONARY[credential_name]

    _ENGINES[credential_name] = sqlalchemy.create_engine(sqlalchemy.engine.url.URL.create(
        drivername='postgresql',
        port=db_credentials['port'],
        database=db_credentials['database'],
        username=db_credentials['username'],
        password=db_credentials['password']
    ))


def pull_column_names(database, schema_name, table_name, engine):
    query = sqlalchemy.text('''
    SELECT column_name
    FROM information_schema.columns
    WHERE 
    table_catalog = :database AND
    table_schema = :schema_name AND
    table_name = :table_name;
    ''').bindparams(
        database=database,
        schema_name=schema_name,
        table_name=table_name
    )
    dataframe = pd.read_sql(query, engine)
    return set(dataframe['column_name'])

