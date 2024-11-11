from os.path import abspath, join

PROJECT_DIR = abspath(join(__file__, "..", ".."))
DATA_DIR = join(PROJECT_DIR, "data")
CUSTOM_DATASETS_DIR = join(DATA_DIR, "custom_built_datasets")
STEAMAPI_DATASET_DIR = join(CUSTOM_DATASETS_DIR, "steam_api")
STEAMSPY_DATASET_DIR = join(CUSTOM_DATASETS_DIR, "steam_spy")
READY_DATASETS_DIR = join(DATA_DIR, "ready_made_datasets")
MENDELEY_DATASET_DIR = join(READY_DATASETS_DIR, "mendeley_01-steam_games_dataset")

DATASETS = {
    'steamapi': join(STEAMAPI_DATASET_DIR, "steam-api_app_details.json"),
    'steamspy': join(STEAMSPY_DATASET_DIR, "steam-spy_app_details.json"),
    'mendeley_playercount_1': join(MENDELEY_DATASET_DIR, "PlayerCountHistoryPart1"),
    'mendeley_playercount_2': join(MENDELEY_DATASET_DIR, "PlayerCountHistoryPart2"),
    'mendeley_price': join(MENDELEY_DATASET_DIR, "PriceHistory"),
    'mendeley_apps': join(MENDELEY_DATASET_DIR, "applicationInformation.csv")
}

SOURCES = {
    'steamapi': {
        'ratelimit_max': 100000,
        'ratelimit_window': 60 * 60 * 24,
        'endpoints': {
            'appdetails': 'https://store.steampowered.com/api/appdetails?appids={}&filters=categories,genres,release_date,price_overview,metacritic,publishers,developers,basic,',
            'applist': 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'
        }
    },
    'steamspy': {
        'ratelimit_max': 1,
        'ratelimit_window': 1,
        'endpoints': {
            'appdetails': 'https://steamspy.com/api.php?request=appdetails&appid={}'
        }
    },
    'mendeley': {
        'files': {
            'playercount_1': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/c78f8614-23c8-4433-a3ec-7fea1353c15c/file_downloaded',
            'playercount_2': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/771a7ac5-5241-4187-ba33-7c88f32b09f2/file_downloaded',
            'price': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/a4e5ea6b-a464-43c9-adae-0751129eab9d/file_downloaded',
            'apps': 'https://data.mendeley.com/public-files/datasets/ycy3sy3vj2/files/f3bd0df3-5455-4fbf-bb67-7acecfc8910f/file_downloaded'
        }
    }
}