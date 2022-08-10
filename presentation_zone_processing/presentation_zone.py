import pandas as pd
import sqlalchemy as sqla
import helpers.logger as stocky_logger
import helpers.database_helper as dh

def get_all_file_names() -> pd.DataFrame:
    all_file_names_query = sqla.text('''
        SELECT
        DISTINCT(file_name)
        FROM raw_zone.daily_market_performance
    ''')

    return pd.read_sql(all_file_names_query, dh.get_stocky_db_engine('stocky_raw_zone'))


def get_all_asset_performance_from_file_name(file_name) -> pd.DataFrame:
    """
    query daily_market_performance for a single asset
    :param file_name: file_name for given asset
    :return DataFrame: complete record of assets performance from daily_market_performance
    """
    asset_performance_query = sqla.text(
        '''
            SELECT *
            FROM raw_zone.daily_market_performance
            WHERE file_name = :file_name
        '''
    ).bindparams(
        file_name=file_name
    )

    return pd.read_sql(asset_performance_query, dh.get_stocky_db_engine('stocky_raw_zone'))


def transform_raw_asset_performance(raw_asset_performance_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformation step to add rolling averages and rolling sums.
    :param raw_asset_performance_df:
    :return:
    """
    x=3

def main():
    all_files_df = get_all_file_names()
    for index, row in all_files_df.iterrows():
        raw_asset_performance_df = get_all_asset_performance_from_file_name(row['file_name'])
        cleaned_asset_performance = transform_raw_asset_performance(raw_asset_performance_df)

if __name__ == '__main__':
    main()
