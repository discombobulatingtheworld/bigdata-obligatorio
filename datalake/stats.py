# stats.py

from pandas import DataFrame
from datalake import DATASETS
from pandas import read_csv
import json


def load_json_dataset(dataset_name):
    with open(DATASETS[dataset_name]) as f:
        return json.load(f)
    
def run_stats():
    steamapi_ds = load_json_dataset('steamapi')
    steamspy_ds = load_json_dataset('steamspy')
    
    steamapi_df = DataFrame(steamapi_ds)
    steamspy_df = DataFrame(steamspy_ds)

    steamapi_df = steamapi_df.drop_duplicates('appid')
    steamspy_df = steamspy_df.drop_duplicates('appid')

    print("Steam API dataset sample")
    print("========================")
    print(steamapi_df.head())
    print()

    print("Steam Spy dataset sample")
    print("========================")
    print(steamspy_df.head())
    print()

    print("Dataset shapes")
    print("==============")
    print(steamapi_df.shape)
    print(steamspy_df.shape)
    print()

    print("Appids not in both datasets")
    print("===========================")
    print(len(set(steamapi_df['appid'].to_list()).symmetric_difference(set(steamspy_df['appid'].to_list()))))
    print()

    mendeleys = read_csv(DATASETS['mendeley_apps'])

    print("Appids in Mendeley dataset")
    print("==========================")
    print(len(mendeleys))
    print()

    print("Appids in Mendeley dataset not in Steam API dataset")
    print("====================================================")
    print(len(set(mendeleys['appid'].to_list()) - set(steamapi_df['appid'].to_list())))