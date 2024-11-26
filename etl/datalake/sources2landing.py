from datalake import \
    get_dataset, \
    get_source, \
    get_mongo_config, \
    get_proxies, \
    get_is_debug, \
    get_worker_count, \
    get_zone_dir
from requests import get
from os import makedirs, listdir, remove
from os.path import dirname, join, exists
from queue import Queue, Empty as QueueEmpty
from threading import Event, Lock
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pymongo import MongoClient
from json import load, dump, JSONDecodeError, dumps
from time import sleep


datetime.now().isoformat()

def _api_worker(task_queue, result_queue, failure_queue, stop_event, proxies={}):
    while not stop_event.is_set():
        try:
            if task_queue.empty():
                task = failure_queue.get(timeout=1)
            else:
                task = task_queue.get(timeout=1)
        except Exception as e:
            if stop_event.is_set():
                break
            continue

        try:
            response = get(task, proxies=proxies, timeout=10)
            match response.status_code:
                case 200:
                    result_queue.put(response.json())
                case 429:
                    failure_queue.put(task)
                    response.raise_for_status()
                case _:
                    response.raise_for_status()
        except Exception as e:
            continue

def _queue_worker(target_file, saved_queue, result_queue, stop_event, buffer_lock, extract_app):
    def write_buffer():
        if exists(target_file):
            try:
                with open(target_file, 'r') as f:
                    existing_data = load(f)
            except JSONDecodeError as e:
                print(f"Error loading {target_file}: {e}")
                existing_data = []
        else:
            existing_data = []

        if not isinstance(existing_data, list):
            print(f"Error: {target_file} is not a list.")
            existing_data = []

        existing_data.extend(buffer)

        try:
            with open(target_file, 'w') as f:
                dump(existing_data, f, indent=4)
                # Collect app IDs before clearing the buffer
                for result in buffer:
                    app = extract_app(result)
                    if app is not None:
                        saved_queue.put(app)
                buffer.clear()
        except Exception as e:
            print(f"Error writing to {target_file}: {e}")
            return

    buffer = []

    while not stop_event.is_set():
        try:
            result = result_queue.get(timeout=1)
        except QueueEmpty:
            if stop_event.is_set():
                break
            continue

        with buffer_lock:
            buffer.append(result)
            if len(buffer) >= 1000:
                try:
                    write_buffer()
                except Exception as e:
                    print(f"Error writing to {target_file}: {e}")
                    continue

    with buffer_lock:
        try:
            write_buffer()
        except Exception as e:
            print(f"Error writing to {target_file}: {e}")
            pass

def _status_worker(task_queue, failure_queue, stop_event, appids):
    total = len(appids)
    while not stop_event.is_set():
        try:
            remaining = task_queue.qsize()
            remaining += failure_queue.qsize()
            failed = failure_queue.qsize()

            msg = f"R: {remaining}, C: {(total - remaining)}, F: {failed}"
            print(f"\r--{msg}", end='')
        except Exception as e:
            pass
        sleep(2)

    print("\n--Done.")



def _get_steam_appdetails(new_appids, proxies, workers=1):
    dataset = get_dataset('landing', 'steamapi_appdetails')
    target_file = join(dataset['file_details']['dir'], dataset['file_details']['file'])
    source = get_source('steamapi_appdetails')
    target_endpoint = source['api_details']['url']

    print(f"{datetime.now()} - --Getting {source['name']}... ")

    task_queue = Queue()
    result_queue = Queue()
    saved_queue = Queue()
    failure_queue = Queue()
    stop_event = Event()
    buffer_lock = Lock()

    idx = 0
    for appid in new_appids:
        idx += 1
        if get_is_debug() and idx > 500:
            break
        task_queue.put(target_endpoint.format(appid))

    def extract_app(result):
        try:
            appid = list(result)[0]
            type = result[appid]['data']['type']
            return (int(appid), type)
        except KeyError:
            return None

    print()
    try:
        with ThreadPoolExecutor(max_workers=(workers+2)) as executor:
            for _ in range(workers):
                executor.submit(_api_worker, task_queue, result_queue, failure_queue, stop_event, proxies)
            
            executor.submit(_queue_worker, target_file, saved_queue, result_queue, stop_event, buffer_lock, extract_app)
            executor.submit(_status_worker, task_queue, failure_queue, stop_event, new_appids)

            # Wait until all queues are empty
            while not (task_queue.empty() and failure_queue.empty() and result_queue.empty()):
                sleep(1)
            
            stop_event.set()
    except KeyboardInterrupt:
        stop_event.set()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error processing {source['name']} data: {e}")

    # Collect saved app IDs from saved_queue
    saved_appids = []
    while not saved_queue.empty():
        try:
            appid = saved_queue.get_nowait()
            saved_appids.append(appid)
        except QueueEmpty:
            break

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")
    return saved_appids

def _get_steamspy_appdetails(new_appids, proxies, workers=1):
    dataset = get_dataset('landing', 'steamspy_appdetails')
    target_file = join(dataset['file_details']['dir'], dataset['file_details']['file'])
    source = get_source('steamspy_appdetails')
    target_endpoint = source['api_details']['url']

    print(f"{datetime.now()} - --Getting {source['name']}... ")

    task_queue = Queue()
    result_queue = Queue()
    saved_queue = Queue()
    failure_queue = Queue()
    stop_event = Event()
    buffer_lock = Lock()

    idx = 0
    for appid in new_appids:
        idx += 1
        if get_is_debug() and idx > 500:
            break
        task_queue.put(target_endpoint.format(appid))

    print()
    try:
        with ThreadPoolExecutor(max_workers=(workers+2)) as executor:
            for _ in range(workers):
                executor.submit(_api_worker, task_queue, result_queue, failure_queue, stop_event, proxies)
            
            executor.submit(_queue_worker, target_file, saved_queue, result_queue, stop_event, buffer_lock, lambda x: x['appid'])
            executor.submit(_status_worker, task_queue, failure_queue, stop_event, new_appids)

            # Wait until all queues are empty
            while not (task_queue.empty() and failure_queue.empty() and result_queue.empty()):
                sleep(1)
            
            stop_event.set()
    except KeyboardInterrupt:
        stop_event.set()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error processing {source['name']} data: {e}")

    # Collect saved app IDs from saved_queue
    saved_appids = []
    while not saved_queue.empty():
        try:
            appid = saved_queue.get_nowait()
            saved_appids.append(appid)
        except QueueEmpty:
            break

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")
    return saved_appids



def _get_steam_applist(proxies):
    dataset = get_dataset('landing', 'steamapi_applist')
    target_file = join(dataset['file_details']['dir'], dataset['file_details']['file'])
    source = get_source('steamapi_applist')
    target_endpoint = source['api_details']['url']

    print(f"{datetime.now()} - --Getting {source['name']}... ")
    try:
        response = get(target_endpoint, proxies=proxies, timeout=10)
        match response.status_code:
            case 200:
                pass
            case 429:
                print(f"{datetime.now()} - --Getting {source['name']}... Rate limit exceeded for {source['name']} from {target_endpoint}.")
                return
            case _:
                response.raise_for_status()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error getting {source['name']} from {target_endpoint}: {e}.")
        return

    try:
        makedirs(dirname(target_file), exist_ok=True, mode=0o755)
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error creating directory for {target_file}: {e}.")
        return
    
    with open(target_file, 'w') as f:
        f.write(dumps(response.json(), indent=4))

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")

    appids = [app['appid'] for app in response.json()['applist']['apps']]
    return appids

def _get_mendeley_playercount1():
    dataset = get_dataset('landing', 'mendeley_playercount_1')
    source = get_source('mendeley_playercount_1')

    print(f"{datetime.now()} - --Getting {source['name']}... ")
    try:
        response = get(source['file_details']['url'], timeout=10)
        match response.status_code:
            case 200:
                pass
            case 429:
                print(f"{datetime.now()} - --Getting {source['name']}... Rate limit exceeded for {source['name']} from {source['api_details']['url']}.")
                return
            case _:
                response.raise_for_status()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error getting {source['name']} from {source['api_details']['url']}: {e}.")
        return
    
    try:
        makedirs(dirname(dataset['file_details']['dir']), exist_ok=True, mode=0o755)
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error creating directory for {dataset['file_details']['dir']}: {e}.")
        return
    
    with open(join(dataset['file_details']['dir'], dataset['file_details']['file']), 'wb') as f:
        f.write(response.content)

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")

def _get_mendeley_playercount2():
    dataset = get_dataset('landing', 'mendeley_playercount_2')
    source = get_source('mendeley_playercount_2')

    print(f"{datetime.now()} - --Getting {source['name']}... ")
    try:
        response = get(source['file_details']['url'], timeout=10)
        match response.status_code:
            case 200:
                pass
            case 429:
                print(f"{datetime.now()} - --Getting {source['name']}... Rate limit exceeded for {source['name']} from {source['api_details']['url']}.")
                return
            case _:
                response.raise_for_status()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error getting {source['name']} from {source['api_details']['url']}: {e}.")
        return
    
    try:
        makedirs(dirname(dataset['file_details']['dir']), exist_ok=True, mode=0o755)
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error creating directory for {dataset['file_details']['dir']}: {e}.")
        return
    
    with open(join(dataset['file_details']['dir'], dataset['file_details']['file']), 'wb') as f:
        f.write(response.content)

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")

def _get_mendeley_price():
    dataset = get_dataset('landing', 'mendeley_price')
    source = get_source('mendeley_price')

    print(f"{datetime.now()} - --Getting {source['name']}... ")
    try:
        response = get(source['file_details']['url'], timeout=10)
        match response.status_code:
            case 200:
                pass
            case 429:
                print(f"{datetime.now()} - --Getting {source['name']}... Rate limit exceeded for {source['name']} from {source['api_details']['url']}.")
                return
            case _:
                response.raise_for_status()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error getting {source['name']} from {source['api_details']['url']}: {e}.")
        return
    
    try:
        makedirs(dirname(dataset['file_details']['dir']), exist_ok=True, mode=0o755)
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error creating directory for {dataset['file_details']['dir']}: {e}.")
        return
    
    with open(join(dataset['file_details']['dir'], dataset['file_details']['file']), 'wb') as f:
        f.write(response.content)

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")

def _get_mendeley_apps():
    dataset = get_dataset('landing', 'mendeley_apps')
    source = get_source('mendeley_apps')

    print(f"{datetime.now()} - --Getting {source['name']}... ")
    try:
        response = get(source['file_details']['url'], timeout=10)
        match response.status_code:
            case 200:
                pass
            case 429:
                print(f"{datetime.now()} - --Getting {source['name']}... Rate limit exceeded for {source['name']} from {source['api_details']['url']}.")
                return
            case _:
                response.raise_for_status()
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error getting {source['name']} from {source['api_details']['url']}: {e}.")
        return
    
    try:
        makedirs(dirname(dataset['file_details']['dir']), exist_ok=True, mode=0o755)
    except Exception as e:
        print(f"{datetime.now()} - --Getting {source['name']}... Error creating directory for {dataset['file_details']['dir']}: {e}.")
        return
    
    with open(join(dataset['file_details']['dir'], dataset['file_details']['file']), 'wb') as f:
        f.write(response.content)

    print(f"{datetime.now()} - --Getting {source['name']}... Done.")

def _get_old_steamapi_appdetails_appids(mongo_client):
    raw_steamapi_appdetails_ds = get_dataset('raw', 'steamapi_appdetails')

    collection = mongo_client[raw_steamapi_appdetails_ds['mongo_details']['db']][raw_steamapi_appdetails_ds['mongo_details']['collection']]

    if collection.count_documents({}) == 0:
        return []

    pipeline = [
        {
            "$project": {
                "data_array": {"$objectToArray": "$data"}
            }
        },
        {
            "$unwind": "$data_array"
        },
        {
            "$project": {
                "steam_appid": "$data_array.v.data.steam_appid"
            }
        },
        {
            "$match": {
                "steam_appid": {"$exists": True, "$ne": None}
            }
        }
    ]

    cursor = collection.aggregate(pipeline)
    steam_appids = [doc['steam_appid'] for doc in cursor]
    return steam_appids

def _get_old_steamspy_appdetails_appids(mongo_client):
    raw_steamspy_appdetails_ds = get_dataset('raw', 'steamspy_appdetails')

    collection = mongo_client[raw_steamspy_appdetails_ds['mongo_details']['db']][raw_steamspy_appdetails_ds['mongo_details']['collection']]
    cursor = collection.find({}, {'_id': 0, 'appid': 1})
    appid_list = [doc['appid'] for doc in cursor if 'appid' in doc]

    return appid_list

def _process_api_sources(proxies, workers, mongo_client):
    steamapi_applist_appids = _get_steam_applist(proxies)

    old_steamapi_appdetails_appids = _get_old_steamapi_appdetails_appids(mongo_client)
    new_steamapi_appdetails_appids = [appid for appid in steamapi_applist_appids if appid not in old_steamapi_appdetails_appids]
    steamapi_appdetails_appids = _get_steam_appdetails(new_steamapi_appdetails_appids, proxies, workers)
    
    steamapi_appdetails_appids_games = [ app[0] for app in steamapi_appdetails_appids if app[1] == 'game' ]
    old_steamspy_appdetails_appids = _get_old_steamspy_appdetails_appids(mongo_client)
    new_steamspy_appdetails_appids = [appid for appid in steamapi_appdetails_appids_games if appid not in old_steamspy_appdetails_appids]
    _get_steamspy_appdetails(new_steamspy_appdetails_appids, proxies, workers)

def _process_files_sources():
    print("{} - Processing file sources...".format(datetime.now()))
    _get_mendeley_playercount1()
    _get_mendeley_playercount2()
    _get_mendeley_price()
    _get_mendeley_apps()
    print("{} - Processing file sources... Done.".format(datetime.now()))

def _clear_landing():
    print("{} - Clearing landing zone...".format(datetime.now()))

    dirpath = get_zone_dir('landing')
    if not exists(dirpath):
        print("{} - Clearing landing zone... Done.".format(datetime.now()))
        return
    
    for file in listdir(dirpath):
        try:
            print(f"{datetime.now()} - --Removing {join(dirpath, file)}")
            remove(join(dirpath, file))
        except Exception as e:
            print(f"{datetime.now()} - --Error removing {file}: {e}")

    print("{} - Clearing landing zone... Done.".format(datetime.now()))


def load_landing():
    print("Fetching sources")
    print("================\n")

    mongo_url = get_mongo_config()
    print("{} - Connecting to MongoDB: {}".format(datetime.now(), mongo_url))
    client = MongoClient(mongo_url)

    try:
        workers = get_worker_count()
    except Exception as e:
        workers = 1

    _clear_landing()
    _process_api_sources(get_proxies(), workers, client)
    _process_files_sources()