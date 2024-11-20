from datetime import datetime
from zipfile import ZipFile
from datalake import get_datasets, get_dataset, get_mongo_config, get_is_debug
from os.path import join, dirname, exists
from os import makedirs
from pymongo import MongoClient
from json import load
from pandas import read_csv


def _process_zips(timestamp):
    pass

def _process_file2file_source(dataset, source, timestamp):
    print(f"----Processing file2file source: {source['name']} -> {dataset['name']}...", end='')

    source_path = join(source['file_details']['dir'], source['file_details']['file'])
    target_path = join(dataset['file_details']['dir'], timestamp, dataset['file_details']['file'])

    try:
        makedirs(dirname(source_path), exist_ok=True, mode=0o777)
        makedirs(dirname(target_path), exist_ok=True, mode=0o777)
    except Exception as e:
        print(f"Error creating directories: {e}")
        return

    try:
        # TODO: copy file
        pass
    except Exception as e:
        print(f"Error copying file: {e}")
        return
    
    print("----Done!")

def _json4mongo(file):
    for encoding in ['utf-8', 'latin1', 'windows-1252']:
        try:
            with open(file, 'r', encoding=encoding) as f:
                return load(f)
        except Exception as e:
            pass
        
    print(f"Error reading JSON file with all encodings: {file}")
    return []
    
def _csv4mongo(file):
    for encoding in ['utf-8', 'latin1', 'windows-1252']:
        try:
            ds = read_csv(file, encoding=encoding)
            return ds.to_dict(orient='records')
        except Exception as e:
            pass
            
    print(f"Error reading CSV file with all encodings: {file}")
    return []

def _zip4mongo(file):
    def format_datum(file_name, datum):
        return {
            'file': file_name,
            'data': datum,
        }

    print(f"------Processing zip file: {file}...", end='')

    data = []
    with ZipFile(file, 'r') as z:
        file_count = len(z.namelist())
        print(f"Found {file_count} files.")
        idx = 1
        msg_len = 0
        for name in z.namelist():
            idx += 1
            if get_is_debug() and idx > 10:
                break
            msg = f"processing file {idx}/{file_count}: {name}..."
            print(f"{msg}{" "*(max(0, msg_len-len(msg)))}", end='\r')
            msg_len = len(msg)
            if name.endswith('.csv'):
                with z.open(name) as f:
                    for encoding in ['utf-8', 'latin1', 'windows-1252']:
                        try:
                            datum = read_csv(f, encoding=encoding)
                            break
                        except Exception as e:
                            print(f"Error reading CSV file: {e}")
                            continue
                    data.append(format_datum(name, datum.to_dict(orient='records')))
            if name.endswith('.json'):
                with z.open(name) as f:
                    data.append(format_datum(name, load(f)))
            print()

    print(f"------Done!")

    return data


def _process_file2mongo_source(dataset, source, timestamp, client):
    print(f"----Processing file2mongo source: {source['name']} -> {dataset['name']}...", end='')

    source_path = join(source['file_details']['dir'], source['file_details']['file'])
    if not exists(source_path):
        print(f"----Source file does not exist: {source_path}")
        return

    dataset_db = dataset['mongo_details']['db']
    dataset_collection = dataset['mongo_details']['collection']

    db = client[dataset_db]
    collection = db[dataset_collection]

    new_data = []

    match source['file_details']['type']:
        case 'csv':
            data = _csv4mongo(source_path)
        case 'json':
            data = _json4mongo(source_path)
        case 'zip':
            data = _zip4mongo(source_path)
        case _:
            print(f"----Unsupported file type: {source['file_details']['type']}")
            return

    if dataset['transformation'] is not None:
        data = dataset['transformation'](data)
    
    if isinstance(data, list):
        if len(data) == 0:
            print(f"----No data found in file: {source_path}")
            return
        for datum in data:
            new_data.append({
                'timestamp': timestamp,
                'data': datum,
            })
    else:
        new_data.extend([{
            'timestamp': timestamp,
            'data': data,
        }])

    try:
        collection.insert_many(new_data)
    except Exception as e:
        print(f"----Error inserting data into MongoDB: {e}")
        return

    print("----Done!")

def _process_mongo(timestamp):
    print(f"Processing mongo datasets...")
    
    mongo_url = get_mongo_config()

    print("Connecting to MongoDB: {}".format(mongo_url))
    client = MongoClient(mongo_url)

    for dataset in get_datasets('raw', 'mongo'):
        print(f"--Processing dataset: {dataset['name']}")
        source = dataset['source']
        if source['type'] == 'dataset':
            source_details = source['dataset_details']
            sources = [(source_details['area'], source_name) for source_name in source_details['items']]
            for ds in sources:
                source_dataset = get_dataset(ds[0], ds[1])
                if source_dataset['type'] == 'file':
                    _process_file2mongo_source(dataset, source_dataset, timestamp, client)
                else:
                    print(f"--Unsupported source type: {source['type']}:{source_dataset['type']} for target type: {dataset['type']}")
        else:
            print(f"--Unsupported source type: {source['type']} for target type: {dataset['type']}")

    print("Done!")

def _process_files(timestamp):
    print(f"Processing file datasets...")
    for dataset in get_datasets('raw', 'file'):
        print(f"--Processing dataset: {dataset['name']}")
        source = dataset['source']
        if source['type'] == 'dataset':
            source_details = source['dataset_details']
            sources = [(source_details['area'], source_name) for source_name in source_details['items']]
            for ds in sources:
                source_dataset = get_dataset(ds[0], ds[1])
                if source_dataset['type'] == 'file':
                    _process_file2file_source(dataset, source_dataset, timestamp)
                else:
                    print(f"--Unsupported source type: {source['type']}:{source_dataset['type']} for target type: {dataset['type']}")
        else:
            print(f"--Unsupported source type: {source['type']} for target type: {dataset['type']}")

    print("Done!")


def load_raw():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    _process_files(timestamp)
    _process_mongo(timestamp)
