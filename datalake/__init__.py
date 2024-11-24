from os.path import abspath, join
from os import listdir
from json import load
from dotenv import dotenv_values

CONFIG = dotenv_values(".env")

# Common functions
def load_json_dataset(file):
    with open(file, 'r') as f:
        return load(f)
    
def get_mongo_config():
    return 'mongodb://{}:{}@{}:{}/'.format(
        CONFIG['MONGO_USER'],
        CONFIG['MONGO_PASS'],
        CONFIG['MONGO_HOST'],
        CONFIG['MONGO_PORT']
    )

def get_elasticsearch_config():
    return 'http://{}:{}'.format(
        CONFIG['ELASTICSEARCH_HOST'],
        CONFIG['ELASTICSEARCH_PORT'],
    )

def get_proxies():
    proxies = {}
    if 'PROXY_HTTP' in CONFIG and CONFIG['PROXY_HTTP'] != '':
        proxies['http'] = f"http://{CONFIG['PROXY_HTTP']}"
    if 'PROXY_HTTPS' in CONFIG and CONFIG['PROXY_HTTPS'] != '':
        proxies['https'] = f"http://{CONFIG['PROXY_HTTPS']}"
    return proxies

def get_is_debug():
    return 'DEBUG' in CONFIG and CONFIG['DEBUG'] == 'True'

def get_download_sources_step_enable():
    return 'DOWNLOAD_SOURCES_STEP_ENABLE' in CONFIG and CONFIG['DOWNLOAD_SOURCES_STEP_ENABLE'] == 'True'

def get_worker_count():
    return int(CONFIG['WORKERS'])

def get_zone_dir(zone):
    match zone:
        case 'landing':
            return LANDING_ZONE_DIR
        case 'raw':
            return RAW_DATA_DIR
        case 'refined':
            return REFINED_DATA_DIR
        case _:
            return None

# Project Root and Data directory for all datasets
PROJECT_ROOT_DIR = abspath(join(__file__, "..", ".."))
PROJECT_DATA_DIR = join(PROJECT_ROOT_DIR, "data")

# Landing Zone directory
LANDING_ZONE_DIR = join(PROJECT_DATA_DIR, "landing_zone")

LANDING_STEAMAPI_APPLIST = "steam-api_app_list.json"
LANDING_STEAMAPI_APPDETAILS = "steam-api_app_details.json"
LANDING_STEAMSPY_APPDETAILS = "steam-spy_app_details.json"
LANDING_MENDELEY_PLAYERCOUNT_1 = "PlayerCountHistoryPart1.zip"
LANDING_MENDELEY_PLAYERCOUNT_2 = "PlayerCountHistoryPart2.zip"
LANDING_MENDELEY_PRICE = "PriceHistory.zip"
LANDING_MENDELEY_APPS = "applicationInformation.csv"

# Raw Data directory
RAW_DATA_DIR = join(PROJECT_DATA_DIR, "raw_data")

RAW_STEAMAPI_DIR = join(RAW_DATA_DIR, "steam_api")
RAW_STEAMSPY_DIR = join(RAW_DATA_DIR, "steam_spy")
RAW_MENDELEY_DIR = join(RAW_DATA_DIR, "mendeley")

RAW_STEAMAPI_APPLIST = "steam-api_app_list.json"
RAW_STEAMAPI_APPDETAILS = "steam-api_app_details.json"
RAW_STEAMSPY_APPDETAILS = "steam-spy_app_details.json"

RAW_MENDELEY_PLAYERCOUNT_DIR = join(RAW_MENDELEY_DIR, "playercount_history")
RAW_MENDELEY_PRICE_DIR = join(RAW_MENDELEY_DIR, "price_history")
RAW_MENDELEY_APPS = "application_information.csv"

# Refined Data directory
REFINED_DATA_DIR = join(PROJECT_DATA_DIR, "refined_data")

# Datasets configuration

def get_dataset(area: str, name: str):
    for dataset in DATASETS[area]:
        if dataset['name'] == name:
            return dataset
    return None

def get_datasets(area_filter: str, type_filter: str):
    for dataset in DATASETS[area_filter]:
        if dataset['type'] == type_filter:
            yield dataset

def get_dataset_files(dataset):
    match dataset['type']:
        case 'file':
            details = dataset['file_details']
            if dataset['history'] == True:
                return [ { 'dir': dir, 'path': [join(details['dir'], dir, details['file'])]} for dir in listdir(details['dir']) if details['file'] in dir]
            else:
                return [ { 'dir': None, 'path': [join(details['dir'], details['file'])]}]
        case 'zip':
            details = dataset['zip_details']
            if dataset['history'] == True:
                return [ {'dir': dir, 'path': [join(details['dir'], dir, details['file'])]} for dir in listdir(details['dir']) if details['file'] in dir]
            else:
                return [ {'dir': None, 'path': [join(details['file'])]}]
        case 'folder':
            details = dataset['folder_details']
            if dataset['history'] == True:
                return [ {'dir': dir, 'path': [join(details['dir'], dir, file) for file in listdir(join(details['dir'], dir))]} for dir in listdir(details['dir'])]
            else:
                return [ {'dir': None, 'path': [join(details['dir'], file) for file in listdir(details['dir'])]}]
        case _:
            return []

DATASETS = {
    'landing': [
        {
            'name': 'steamapi_applist',
            'type': 'file',
            'file_details': {
                'type': 'json',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_STEAMAPI_APPLIST,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'steamapi_applist',
                },
            },
            'transformation': None,
        },
        {
            'name': 'steamapi_appdetails',
            'type': 'file',
            'file_details': {
                'type': 'json',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_STEAMAPI_APPDETAILS,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'steamapi_appdetails',
                },
            },
            'transformation': None,
        },
        {
            'name': 'steamspy_appdetails',
            'type': 'file',
            'file_details': {
                'type': 'json',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_STEAMSPY_APPDETAILS,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'steamspy_appdetails',
                },
            },
            'transformation': None,
        },
        {
            'name': 'mendeley_playercount_1',
            'type': 'file',
            'file_details': {
                'type': 'zip',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_MENDELEY_PLAYERCOUNT_1,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'mendeley_playercount_1',
                },
            },
            'transformation': None,
        },
        {
            'name': 'mendeley_playercount_2',
            'type': 'file',
            'file_details': {
                'type': 'zip',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_MENDELEY_PLAYERCOUNT_2,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'mendeley_playercount_2',
                },
            },
            'transformation': None,
        },
        {
            'name': 'mendeley_price',
            'type': 'file',
            'file_details': {
                'type': 'zip',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_MENDELEY_PRICE,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'mendeley_price',
                },
            },
            'transformation': None,
        },
        {
            'name': 'mendeley_apps',
            'type': 'file',
            'file_details': {
                'type': 'csv',
                'dir': LANDING_ZONE_DIR,
                'file': LANDING_MENDELEY_APPS,
            },
            'mongo_details': None,
            'zip_details': None,
            'folder_details': None,
            'history': False,
            'source': {
                'type': 'source',
                'dataset_details': None,
                'source_details': {
                    'name': 'mendeley_apps',
                },
            },
            'transformation': None,
        },
    ],
    'raw': [
        {
            'name': 'steamapi_applist',
            'type': 'mongo',
            'file_details': None,
            'mongo_details': {
                'db': CONFIG['MONGO_RAW_DB'],
                'collection': CONFIG['MONGO_RAW_COLLECTION_STEAMAPI_APPLIST'],
            },
            'zip_details': None,
            'folder_details': None,
            'history': True,
            'source': {
                'type': 'dataset',
                'dataset_details': {
                    'area': 'landing',
                    'items': ['steamapi_applist'],
                },
                'source_details': None,
            },
            'transformation': lambda x: x['applist']['apps'],
        },
        {
            'name': 'steamapi_appdetails',
            'type': 'mongo',
            'file_details': None,
            'mongo_details': {
                'db': CONFIG['MONGO_RAW_DB'],
                'collection': CONFIG['MONGO_RAW_COLLECTION_STEAMAPI_APPDETAILS'],
            },
            'zip_details': None,
            'folder_details': None,
            'history': True,
            'source': {
                'type': 'dataset',
                'dataset_details': {
                    'area': 'landing',
                    'items': ['steamapi_appdetails'],
                },
                'source_details': None,
            },
            'transformation': None,
        },
        {
            'name': 'steamspy_appdetails',
            'type': 'mongo',
            'file_details': None,
            'mongo_details': {
                'db': CONFIG['MONGO_RAW_DB'],
                'collection': CONFIG['MONGO_RAW_COLLECTION_STEAMSPY_APPDETAILS'],
            },
            'zip_details': None,
            'folder_details': None,
            'history': True,
            'source': {
                'type': 'dataset',
                'dataset_details': {
                    'area': 'landing',
                    'items': ['steamspy_appdetails'],
                },
                'source_details': None,
            },
            'transformation': None,
        },
        {
            'name': 'mendeley_playercount',
            'type': 'mongo',
            'file_details': None,
            'mongo_details': {
                'db': CONFIG['MONGO_RAW_DB'],
                'collection': CONFIG['MONGO_RAW_COLLECTION_MENDELEY_PLAYERCOUNT'],
            },
            'zip_details': None,
            'folder_details': None,
            'history': True,
            'source': {
                'type': 'dataset',
                'dataset_details': {
                    'area': 'landing',
                    'items': ['mendeley_playercount_1', 'mendeley_playercount_2'],
                },
                'source_details': None,
            },
            'transformation': lambda data: [
                {
                    'appid': int(item['file'].split('/')[-1].split('.')[0]),
                    'data': data_point
                }
                for item in data
                for data_point in item['data']
            ],
        },
        {
            'name': 'mendeley_price',
            'type': 'mongo',
            'file_details': None,
            'mongo_details': {
                'db': CONFIG['MONGO_RAW_DB'],
                'collection': CONFIG['MONGO_RAW_COLLECTION_MENDELEY_PRICE'],
            },
            'zip_details': None,
            'folder_details': None,
            'history': True,
            'source': {
                'type': 'dataset',
                'dataset_details': {
                    'area': 'landing',
                    'items': ['mendeley_price'],
                },
                'source_details': None,
            },
            'transformation': lambda data: [
                {
                    'appid': int(item['file'].split('/')[-1].split('.')[0]),
                    'data': data_point,
                }
                for item in data
                for data_point in item['data']
            ],
        },
        {
            'name': 'mendeley_apps',
            'type': 'mongo',
            'file_details': None,
            'mongo_details': {
                'db': CONFIG['MONGO_RAW_DB'],
                'collection': CONFIG['MONGO_RAW_COLLECTION_MENDELEY_APPS'],
            },
            'zip_details': None,
            'folder_details': None,
            'history': True,
            'source': {
                'type': 'dataset',
                'dataset_details': {
                    'area': 'landing',
                    'items': ['mendeley_apps'],
                },
                'source_details': None,
            },
            'transformation': None,
        },
    ],
    'refined': [
        {
            'name': 'steam_apps',
            'elasticsearch_index': 'steam_apps_details'
        },
        {
            'name': 'price_history',
            'elasticsearch_index': 'steam_apps_prices_history'
        },
        {
            'name': 'players_count',
            'elasticsearch_index': 'steam_apps_players_count_history'
        }
    ]
}

def get_source(name: str):
    for source in SOURCES:
        if source['name'] == name:
            return source
    return None

SOURCES = [
    {
        'name': 'steamapi_applist',
        'type': 'api',
        'api_details': {
            'url': 'https://api.steampowered.com/ISteamApps/GetAppList/v2/',
            'ratelimit_max': 100000,
            'ratelimit_window': 60 * 60 * 24,
            'ratelimit_correlation': 1,
        },
        'file_details': None,
    },
    {
        'name': 'steamapi_appdetails',
        'type': 'api',
        'api_details': {
            'url': 'https://store.steampowered.com/api/appdetails?appids={}&filters=categories,genres,release_date,price_overview,metacritic,publishers,developers,basic,',
            'ratelimit_max': 100000,
            'ratelimit_window': 60 * 60 * 24,
            'ratelimit_correlation': 1,
        },
        'file_details': None,
    },
    {
        'name': 'steamspy_appdetails',
        'type': 'api',
        'api_details': {
            'url': 'https://steamspy.com/api.php?request=appdetails&appid={}',
            'ratelimit_max': 1,
            'ratelimit_window': 1,
            'ratelimit_correlation': 2,
        },
        'file_details': None,
    },
    {
        'name': 'mendeley_playercount_1',
        'type': 'file',
        'api_details': None,
        'file_details': {
            'url': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/c78f8614-23c8-4433-a3ec-7fea1353c15c/file_downloaded',
        },
    },
    {
        'name': 'mendeley_playercount_2',
        'type': 'file',
        'api_details': None,
        'file_details': {
            'url': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/771a7ac5-5241-4187-ba33-7c88f32b09f2/file_downloaded',
        },
    },
    {
        'name': 'mendeley_price',
        'type': 'file',
        'api_details': None,
        'file_details': {
            'url': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/a4e5ea6b-a464-43c9-adae-0751129eab9d/file_downloaded',
        },
    },
    {
        'name': 'mendeley_apps',
        'type': 'file',
        'api_details': None,
        'file_details': {
            'url': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/f3bd0df3-5455-4fbf-bb67-7acecfc8910f/file_downloaded',
        },
    },
]