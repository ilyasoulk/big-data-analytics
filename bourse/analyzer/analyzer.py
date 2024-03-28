import pandas as pd
import requests
import numpy as np
import sklearn
import tarfile
import os
import dateutil
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker


def store_file(name, website):
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("bourse/data/boursorama/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("bourse/data/boursorama/" + year + "/" + name)
        # to be finished


def create_super_data_frame(market):
    files_2019 = glob.glob('/home/bourse/data/boursorama/' + '2019/' + market + '*')
    files_2020 = glob.glob('/home/bourse/data/boursorama/' + '2020/' + market + '*')
    files_2021 = glob.glob('/home/bourse/data/boursorama/' + '2021/' + market + '*')
    files_2022 = glob.glob('/home/bourse/data/boursorama/' + '2022/' + market + '*')
    files_2023 = glob.glob('/home/bourse/data/boursorama/' + '2023/' + market + '*')
    files = files_2019 + files_2020 + files_2021 + files_2022 + files_2023
    print(f"Found {len(files)} files for {market}")
    market_df = pd.concat({dateutil.parser.parse((f.split(market)[1].split('.'))[0]): pd.read_pickle(f) for f in files})
    market_df.sort_index(inplace=True)
    return market_df


def total_files():
    files_2019 = glob.glob('/home/bourse/data/boursorama/' + '2019/*')
    files_2020 = glob.glob('/home/bourse/data/boursorama/' + '2020/*')
    files_2021 = glob.glob('/home/bourse/data/boursorama/' + '2021/*')
    files_2022 = glob.glob('/home/bourse/data/boursorama/' + '2022/*')
    files_2023 = glob.glob('/home/bourse/data/boursorama/' + '2023/*')
    files = files_2019 + files_2020 + files_2021 + files_2022 + files_2023
    market_names = [f.split()[0].split('/')[-1] for f in files]
    market_names_unique = np.unique(market_names)
    print(f"Found {len(files)} total files")
    print("Markets found:")
    for m in market_names_unique:
        print(m)
    return len(files)


def rename_companies(df):
    df.rename(columns={'symbol' : 'symbol_column'}, inplace=True)
    df['name'] = df.groupby('symbol_column')['name'].transform('last')


def create_company_df(df, is_peapme):
    df = df.copy()
    df['is_peapme'] = is_peapme
    company_df = df[['symbol_column', 'name', 'is_peapme']]
    company_df.reset_index(drop=True, inplace=True)
    company_df = company_df.drop_duplicates(subset=['symbol_column'], keep='last')
    return company_df


if __name__ == '__main__':
    # store_file("compA 2020-01-01 09:02:02.532411", "boursorama")
    # store_file("compB 2020-01-01 09:02:02.532411", "boursorama")
    path = "/home/bourse/data/boursorama.tar"
    if os.path.exists(path):
        print("File already exists")
    else:
        url = 'https://www.lrde.epita.fr/~ricou/pybd/projet/boursorama.tar'
        stream = requests.get(url, stream=True)
        with open('/home/bourse/data/boursorama.tar', 'wb') as f:
            for chunk in stream.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    dir = "/home/bourse/data/boursorama"
    if os.path.exists(dir):
        print("Directory already exists")
    else:
        tar = tarfile.open(path)
        tar.extractall('/home/bourse/data')
        tar.close()
    df_compa = create_super_data_frame("compA")
    df_compb = create_super_data_frame("compB")
    df_amsterdam = create_super_data_frame("amsterdam")
    df_peapme = create_super_data_frame("peapme")
    rename_companies(df_compa)
    rename_companies(df_peapme)
    rename_companies(df_amsterdam)
    companies_peapme = create_company_df(df_peapme, True)
    companies_amsterdam = create_company_df(df_amsterdam, False)
    companies_compa = create_company_df(df_compa, False)
    companies_comb = create_company_df(df_compb, False)
    companies = pd.concat([companies_peapme, companies_amsterdam, companies_compa, companies_comb])
    companies.drop_duplicates(subset=['symbol_column'], keep='last', inplace=True)
    companies.apply(lambda x: db.insert_company(symbol=x['symbol_column'], name=x['name'], pea=x['is_peapme']), axis=1)