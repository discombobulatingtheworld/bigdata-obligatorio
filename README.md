# Big Data Project: Steam Games Analysis - Data Collection

## Instrucciones de Uso

### Requisitos

- Python: (probado en 3.12)
- Docker

### Configuración

Copie el archivo `.env.sample` y renombrarlo a `.env`. Modifique las variables de entorno según sea necesario.

- **DEBUG**: Habilita o deshabilita el modo de depuración. En este modo se limita la cantidad de datos a procesar para que el programa se ejecute en menor tiempo y permita inspeccionar todo el proceso.
- **WORKERS**: Cantidad de hilos a utilizar para solicitudes a apis.

- **MONGO_HOST**: Host de la base de datos MongoDB.
- **MONGO_PORT**: Puerto de la base de datos MongoDB.
- **MONGO_USER**: Usuario de la base de datos MongoDB.
- **MONGO_PASS**: Contraseña de la base de datos MongoDB.
- **MONGO_EXPRESS_PORT**: Puerto de la interfaz web de MongoDB.
- **MONGO_EXPRESS_USER**: Usuario de la interfaz web de MongoDB.
- **MONGO_EXPRESS_PASS**: Contraseña de la interfaz web de MongoDB.
- **JUPYTER_PORT**: Puerto de la interfaz web de Jupyter.
- **JUPYTER_TOKEN**: Token de la interfaz web de Jupyter.

- **PROXY_HTTP**: Proxy para peticiones HTTP.
- **PROXY_HTTPS**: Proxy para peticiones HTTPS.

- **MONGO_RAW_DB**: Base de datos de la zona de datos crudos.
- **MONGO_RAW_COLLECTION_STEAMAPI_APPLIST**: Colección de la API de Steam con la lista de aplicaciones en la zona de datos crudos.
- **MONGO_RAW_COLLECTION_STEAMAPI_APPDETAILS**: Colección de la API de Steam con los detalles de las aplicaciones en la zona de datos crudos.
- **MONGO_RAW_COLLECTION_STEAMSPY_APPDETAILS**: Colección de la API de SteamSpy con los detalles de las aplicaciones en la zona de datos crudos.
- **MONGO_RAW_COLLECTION_MENDELEY_PLAYERCOUNT**: Colección de la base de datos de Mendeley con el historial de cantidad de jugadores en la zona de datos crudos.
- **MONGO_RAW_COLLECTION_MENDELEY_PRICE**: Colección de la base de datos de Mendeley con el historial de precios en la zona de datos crudos.
- **MONGO_RAW_COLLECTION_MENDELEY_APPS**: Colección de la base de datos de Mendeley con los detalles de las aplicaciones en la zona de datos crudos.

### Instalación

1. Clonar el repositorio.

```powershell
git clone https://github.com/discombobulatingtheworld/bigdata-obligatorio.git
```

2. Crear un entorno virtual.

```powershell
python -m venv .venv
```

3. Activar el entorno virtual.

```powershell
.\.venv\Scripts\Activate
```

4. Instalar las dependencias.

```powershell
pip install -r requirements.txt
```

7. Levantar los contenedores de Docker.

```powershell
docker-compose up -d
```

### Ejecución

Ejecutar con Python el script `program.py`.

```powershell
python program.py
```

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
