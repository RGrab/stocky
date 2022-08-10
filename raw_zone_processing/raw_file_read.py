import os
import pathlib

import pandas as pd

import helpers.database_helper as dh
import helpers.logger as stocky_logger

# allowable file types
FILE_TYPE_LIST = ['.txt', '.csv']
DATABASE_NAME = 'stocky'
RAW_ZONE_SCHEMA_NAME = 'raw_zone'
DAILY_MARKET_PERFORMANCE_TABLE_NAME = 'daily_market_performance'

LOGGER = stocky_logger.get_basic_logger('asdf')

DB_ENGINE = dh.get_stocky_db_engine('stocky_raw_zone')

COLUMN_NAMES = dh.pull_column_names(DATABASE_NAME,
                                    RAW_ZONE_SCHEMA_NAME,
                                    DAILY_MARKET_PERFORMANCE_TABLE_NAME,
                                    DB_ENGINE)

DATA_RENAME_DICT = {
    'Date': 'date',
    'Open': 'open_price',
    'High': 'high_price',
    'Low': 'low_price',
    'Close': 'close_price',
    'Volume': 'volume',
    'OpenInt': 'openint'
}


def main():
    data_location_list = [
        {
            'base_file_path': pathlib.Path.cwd().parent / 'Data' / 'ETFs',
            'asset_type': 'ETF'
        },
        {
            'base_file_path': pathlib.Path.cwd().parent / 'Data' / 'Stocks',
            'asset_type': 'stock'
        }
    ]

    for data_location in data_location_list:
        process_file(data_location)


def extract(data_path) -> pd.DataFrame:
    return pd.read_csv(data_path)


def transform(transform_df, file_name, asset_type) -> pd.DataFrame:

    transform_df['asset_type'] = asset_type
    transform_df['file_name'] = file_name

    transform_df.rename(columns=DATA_RENAME_DICT, inplace=True)

    return transform_df.drop(transform_df.columns.difference(COLUMN_NAMES))



def load(load_df):
    load_df.to_sql(name=DAILY_MARKET_PERFORMANCE_TABLE_NAME,
                schema=RAW_ZONE_SCHEMA_NAME,
                con=DB_ENGINE,
                index=False,
                if_exists='append')


def process_file(data_dict):
    for file_path in [path for path in data_dict['base_file_path'].iterdir() if path.suffix in FILE_TYPE_LIST]:
        LOGGER.info(f'processing {file_path}')
        if os.stat(file_path).st_size != 0:
            raw_df = extract(file_path)
            clean_df = transform(raw_df, file_path.name, data_dict['asset_type'])
            load(clean_df)


if __name__ == '__main__':

    main()
