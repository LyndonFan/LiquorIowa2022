import re
from pandas import DataFrame, read_csv, concat
import numpy as np
from tqdm import tqdm
import subprocess

from typing import List, Dict, Any

# TODO: load w/ chunks

kwargs = {
    "engine": 'python',
    "usecols": [
        'Invoice/Item Number', 'Date', 'Store Number', 'Address', 'City', 'Zip Code',
        'County Number', 'County', 'Category', 'Category Name',
        'Vendor Number', 'Vendor Name', 'Item Number', 'Item Description',
        'Pack', 'Bottle Volume (ml)', 'State Bottle Cost', 'State Bottle Retail', 'Bottles Sold'
    ],
    'parse_dates': ['Date'],
    "dtype": {
        'Store Number': 'float64', 'Address': 'object', 'City': 'object', 'Zip Code': 'object',
        'County Number': 'float64', 'Category': 'float64', 'Category Name': 'object',
        'Vendor Number': 'float64', 'Vendor Name': 'object',
        'Item Number': 'float64', 'Item Description': 'object',
        'Pack': 'Int64', 'Bottle Volume (ml)': 'float64', 'Bottles Sold': 'Int64',
        'State Bottle Cost': 'object', 'State Bottle Retail': 'object',
    },

    "on_bad_lines": 'skip'
}

star_dfs = {
    cat: [] for cat in ['transaction', 'store', 'county', 'category', 'vendor', 'item']
}


def rename_column(col_name: str) -> str:
    return re.sub('\(|\)', '', col_name.split('/')[0].lower().replace(' ', '_'))


def add_to_dfs(chunk: DataFrame, dfs: List[DataFrame], columns: List[str]) -> None:
    sub_df = chunk[columns]
    sub_df = sub_df.rename(columns=rename_column)
    sub_df = sub_df.drop_duplicates(sub_df.columns.tolist()[0])
    dfs.append(sub_df)


def process(chunk: DataFrame) -> None:
    add_to_dfs(chunk, star_dfs['transaction'], [
        'Invoice/Item Number', 'Date', 'Store Number', 'Category', 'Vendor Number',
        'Item Number', 'Bottles Sold'
    ])
    add_to_dfs(chunk, star_dfs['store'], [
               'Store Number', 'Address', 'City', 'Zip Code', 'County Number'])
    add_to_dfs(chunk, star_dfs['county'], ['County Number', 'County'])
    add_to_dfs(chunk, star_dfs['category'], ['Category', 'Category Name'])
    add_to_dfs(chunk, star_dfs['vendor'], ['Vendor Number', 'Vendor Name'])
    add_to_dfs(chunk, star_dfs['item'], [
        'Item Number', 'Item Description', 'Pack', 'Bottle Volume (ml)',
        'State Bottle Cost', 'State Bottle Retail'
    ])


def stack_dfs(dfs: List[DataFrame]) -> DataFrame:
    df = concat(dfs, ignore_index=True)
    df = df.drop_duplicates(df.columns.tolist()[0])
    return df


if __name__ == '__main__':
    print('Start loading data')
    import subprocess
    FILE_NAME = 'data/Iowa_Liquor_Sales.csv'
    NUM_LINES = int(subprocess.check_output(
        f"wc -l {FILE_NAME}", shell=True).split()[0]) - 1
    EST_ROWS = NUM_LINES // 3
    CHUNKSIZE = 10000
    with tqdm(total=EST_ROWS, bar_format="Number of rows processed: {postfix[0][value]:e}/"+f"{EST_ROWS:e}"+" ({postfix[1][value]: 2.5} %)",
              postfix=[dict(value=0), dict(value=0.0)]) as t:
        for chunk in tqdm(read_csv(FILE_NAME, chunksize=CHUNKSIZE, **kwargs)):
            process(chunk)
            t.postfix[0]['value'] += len(chunk)
            t.postfix[1]['value'] = t.postfix[0]['value'] * 100.0 / EST_ROWS
            t.update()

    print('Finished loading data')
    for k, vs in star_dfs.items():
        print('Start building', k, 'csv', end='... ', flush=True)
        df = stack_dfs(vs)
        df.to_csv(f'./data/{k}.csv', index=False)
        print('Done')
    print('All done')
