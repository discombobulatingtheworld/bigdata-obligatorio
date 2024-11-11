# Big Data Project: Steam Games Analysis - Data Collection

## Data Sources

### Steam API

- File: [steam-api_app_details.json](data\custom_built_datasets\steam_api\steam-api_app_details.json)
- Endpoint: `https://store.steampowered.com/api/appdetails`
- Method: `GET`
- Query Parameters:
    - `appids`: Comma separated list of Steam App IDs.
    - `cc`: Two letter country code. Example: `US`, `IN`, `GB`, etc.
    - `filters`: Comma separated list of filters. Example: `price_overview`.
- Example: `https://store.steampowered.com/api/appdetails?appids=570,730&cc=US&filters=price_overview`
- Documentation: https://github.com/Revadike/InternalSteamWebAPI/wiki

### Steamspy API

- File: [steam-spy_app_details.json](data\custom_built_datasets\steam_spy\steam-spy_app_details.json)
- Endpoint: `https://steamspy.com/api.php`
- Method: `GET`
- Query Parameters:
    - `request`: API request operation. Posible values: `appdetails`, `genre`, `tag`, `top100in2weeks`, `top100forever`, `top100owned`, `all`. Rate limit policy varies based on the request, so refer to the documentation for more details.
    - `appid`: Steam App ID. Required for `appdetails` request.
    - `genre`: Genre name. Required for `genre` request.
    - `tag`: Tag name. Required for `tag` request.
- Example: `https://steamspy.com/api.php?request=appdetails&appid=570`
- Documentation: https://steamspy.com/api.php

### Steam Games Dataset : Player count history, Price history and data about games

- Files: 
    - [PlayerCountHistoryPart1](data\ready_made_datasets\mendeley_01-steam_games_dataset\PlayerCountHistoryPart1)
    - [PlayerCountHistoryPart2](data\ready_made_datasets\mendeley_01-steam_games_dataset\PlayerCountHistoryPart2)
    - [PriceHistory](data\ready_made_datasets\mendeley_01-steam_games_dataset\PriceHistory)
    - [applicationInformation.csv](data\ready_made_datasets\mendeley_01-steam_games_dataset\applicationInformation.csv)
- Website: https://data.mendeley.com/datasets/ycy3sy3vj2/1
