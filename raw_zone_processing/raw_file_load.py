import os
import pathlib

import pandas as pd

import helpers.database_helper as dh
import helpers.logger as stocky_logger

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
    data_file_path_list = [
        {
            'base_file_path': pathlib.Path.cwd().parent / 'Data' / 'ETFs',
            'asset_type': 'ETF'
        },
        {
            'base_file_path': pathlib.Path.cwd().parent / 'Data' / 'Stocks',
            'asset_type': 'stock'
        }
    ]

    for data_file_path in data_file_path_list:
        process_market_files(data_file_path)




def extract(data_path) -> pd.DataFrame:
    return pd.read_csv(data_path)


def transform(transform_df, file_name, asset_type, column_names) -> pd.DataFrame:
    transform_df['asset_type'] = asset_type
    transform_df['file_name'] = file_name

    transform_df.rename(columns=DATA_RENAME_DICT, inplace=True)

    transform_df.round({'open_price': 2,
                        'high_price': 2,
                        'low_price': 2,
                        'close_price': 2})

    return transform_df.drop(transform_df.columns.difference(column_names))


def load(load_df):
    load_df.to_sql(name='daily_market_performance',
                   schema='raw_zone',
                   con=dh.get_stocky_db_engine('stocky_raw_zone'),
                   index=False,
                   if_exists='append')


def process_market_files(file_type_dict):
    """

    """

    column_names = dh.pull_column_names('stocky',
                                        'raw_zone',
                                        'daily_market_performance',
                                        dh.get_stocky_db_engine('stocky_raw_zone'))

    for file_path in [path for path in file_type_dict['base_file_path'].iterdir() if path.suffix in ['.txt', '.csv']]:

        stocky_logger.STOCKY_INFO_LOGGER.info(f'processing {file_type_dict}')

        if os.stat(file_path).st_size != 0:
            raw_df = extract(file_path)
            clean_df = transform(raw_df, file_path.name, file_type_dict['asset_type'], column_names)
            load(clean_df)


if __name__ == '__main__':
    main()
