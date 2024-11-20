# stats.py

from pandas import DataFrame
from datalake import get_dataset, get_dataset_files, load_json_dataset
from pandas import read_csv
from os.path import join
import json
    
def stats_for_landing():
    ds_details = get_dataset('landing', 'steamapi_appdetails')
    ds_details_files = get_dataset_files(ds_details)
    print(f"Steam API Appdetails dataset: \n{'\n--'.join([ ', '.join(item['path']) for item in ds_details_files])}")

    ds_details_data = []
    for dir in ds_details_files:
        for path in dir['path']:
            data = load_json_dataset(path)
            ds_details_data.extend([ list(item.keys())[0] for item in data])
    print(f"Steam API Appdetails dataset: {len(ds_details_data)} records")
    print(f"Sample: {ds_details_data[:5]}")
    print(f"Duplicate appids: {len(ds_details_data) - len(set(ds_details_data))}")

    print()

    ds_applist = get_dataset('landing', 'steamapi_applist')
    ds_applist_files = get_dataset_files(ds_applist)
    print(f"Steam API Applist dataset: \n{'\n--'.join([ ', '.join(item['path']) for item in ds_applist_files])}")

    ds_applist_data = []
    for dir in ds_applist_files:
        for path in dir['path']:
            data = load_json_dataset(path)
            ds_applist_data.extend([ item['appid'] for item in data['applist']['apps']])
    print(f"Steam API Applist dataset: {len(ds_applist_data)} records")
    print(f"Sample: {ds_applist_data[:5]}")
    print(f"Duplicate appids: {len(ds_applist_data) - len(set(ds_applist_data))}")

    print()

def stats_for_raw():
    pass

    
def run_stats():
    # steamapi_ds = load_json_dataset('steamapi')
    # steamspy_ds = load_json_dataset('steamspy')
    
    # steamapi_df = DataFrame(steamapi_ds)
    # steamspy_df = DataFrame(steamspy_ds)

    # steamapi_df = steamapi_df.drop_duplicates('appid')
    # steamspy_df = steamspy_df.drop_duplicates('appid')

    # print("Steam API dataset sample")
    # print("========================")
    # print(steamapi_df.head())
    # print()

    # print("Steam Spy dataset sample")
    # print("========================")
    # print(steamspy_df.head())
    # print()

    # print("Dataset shapes")
    # print("==============")
    # print(steamapi_df.shape)
    # print(steamspy_df.shape)
    # print()

    # print("Appids not in both datasets")
    # print("===========================")
    # print(len(set(steamapi_df['appid'].to_list()).symmetric_difference(set(steamspy_df['appid'].to_list()))))
    # print()

    # mendeleys = read_csv(DATASETS['mendeley_apps'])

    # print("Appids in Mendeley dataset")
    # print("==========================")
    # print(len(mendeleys))
    # print()

    # print("Appids in Mendeley dataset not in Steam API dataset")
    # print("====================================================")
    # print(len(set(mendeleys['appid'].to_list()) - set(steamapi_df['appid'].to_list())))
    pass