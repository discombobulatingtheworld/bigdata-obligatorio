from datetime import datetime
from zipfile import ZipFile
from datalake import get_datasets, get_dataset, get_mongo_config, get_is_debug, get_encodings
from os.path import join, dirname, exists
from os import makedirs
from pymongo import MongoClient
from json import load
from pandas import read_csv
from io import BytesIO, StringIO

def _process_zips(timestamp):
    pass

def _process_file2file_source(dataset, source, timestamp):
    print(f"{datetime.now()} - ----Source file2file: {source['name']} -> {dataset['name']}...")

    source_path = join(source['file_details']['dir'], source['file_details']['file'])
    target_path = join(dataset['file_details']['dir'], timestamp, dataset['file_details']['file'])

    try:
        makedirs(dirname(source_path), exist_ok=True, mode=0o777)
        makedirs(dirname(target_path), exist_ok=True, mode=0o777)
    except Exception as e:
        print(f"{datetime.now()} - ----Error creating directories: {e}")
        return

    try:
        # TODO: copy file
        pass
    except Exception as e:
        print(f"{datetime.now()} - ----Error copying file: {e}")
        return
    
    print(f"{datetime.now()} - ----Source file2file: {source['name']} -> {dataset['name']}... Done!")

def _json4mongo(file):
    print(f"{datetime.now()} - ------JSON file: {file}...")
    for encoding in get_encodings():
        try:
            with open(file, 'r', encoding=encoding) as f:
                return load(f)
        except Exception:
            pass
        
    print(f"{datetime.now()} - ------Error reading JSON file with all encodings: {file}")
    return []

def _csv4mongo(file, chunk_size=1000):
    print(f"{datetime.now()} - ------CSV file: {file}...")
    for encoding in get_encodings():
        try:
            for chunk in read_csv(file, encoding=encoding, chunksize=chunk_size):
                yield chunk.to_dict(orient='records')
            return
        except Exception:
            pass
    print(f"{datetime.now()} - ------Error reading CSV file with all encodings: {file}")

def _zip4mongo(file):
    def format_datum(file_name, datum):
        return {'file': file_name, 'data': datum}

    print(f"{datetime.now()} - ------Zip file: {file}...")

    successes = 0
    failures = 0
    start = datetime.now()

    with ZipFile(file, 'r') as z:
        file_count = len(z.namelist())
        print(f"{datetime.now()} - --------Found {file_count} files in zip file: {file}.")
        
        for idx, name in enumerate(z.namelist(), start=1):
            if get_is_debug() and idx > 10:
                break
            msg = (
                f"{datetime.now()} - --------Progress: Successes: {successes}, Failures: {failures}, "
                f"Files: {idx}/{file_count} ({(idx/file_count)*100:.2f}%), "
                f"Elapsed: {datetime.now() - start}, "
                f"Remaining: {(datetime.now() - start) / idx * (file_count - idx)}"
            )
            print(f"\r{msg.ljust(80)}", end='')  # Overwrites previous content

            if name.endswith('/'):
                # Skip directories inside zip
                continue

            try:
                with z.open(name) as f:
                    content = f.read()
            except Exception as e:
                print(f"\n{datetime.now()} - ------Error reading file {name} from zip: {e}")
                failures += 1
                continue

            if name.endswith('.csv'):
                for encoding in get_encodings():
                    try:
                        content_io = BytesIO(content)
                        data_chunks = []
                        for chunk in read_csv(content_io, encoding=encoding, chunksize=1000):
                            data_chunk = chunk.to_dict(orient='records')
                            data_chunks.extend(data_chunk)
                        data = [format_datum(name, data_chunks)]
                        successes += 1
                        yield data
                        break  # Exit the encoding loop
                    except Exception:
                        continue
                else:
                    print(f"\n{datetime.now()} - ------Error reading CSV file {name} with all encodings.")
                    failures += 1
            elif name.endswith('.json'):
                for encoding in get_encodings():
                    try:
                        text = content.decode(encoding)
                        data = [format_datum(name, load(StringIO(text)))]
                        successes += 1
                        yield data
                        break
                    except Exception:
                        continue
                else:
                    print(f"\n{datetime.now()} - ------Error reading JSON file {name} with all encodings.")
                    failures += 1
            else:
                print(f"\n{datetime.now()} - ------Unsupported file type in zip: {name}")
                failures += 1

    print(f"\n{datetime.now()} - ------Zip file: {file}... Done!")

def _process_file2mongo_source(dataset, source, timestamp, client, batch_size=10000):
    print(f"{datetime.now()} - ----Source file2mongo: {source['name']} -> {dataset['name']}...")

    source_path = join(source['file_details']['dir'], source['file_details']['file'])
    if not exists(source_path):
        print(f"{datetime.now()} - ----File not found: {source_path}")
        return

    dataset_db = dataset['mongo_details']['db']
    dataset_collection = dataset['mongo_details']['collection']

    db = client[dataset_db]
    collection = db[dataset_collection]

    def _read_and_transform():
        match source['file_details']['type']:
            case 'csv':
                yield from _csv4mongo(source_path)
            case 'json':
                yield _json4mongo(source_path)
            case 'zip':
                yield from _zip4mongo(source_path)
            case _:
                print(f"{datetime.now()} - ----Unsupported file type: {source['file_details']['type']}")
                return

    batch = []
    for data_chunk in _read_and_transform():
        if dataset.get('transformation'):
            data_chunk = dataset['transformation'](data_chunk)

        for datum in data_chunk:
            batch.append({'timestamp': timestamp, 'data': datum})
            if len(batch) >= batch_size:
                try:
                    collection.insert_many(batch)
                    batch.clear()
                except Exception as e:
                    print(f"{datetime.now()} - ----Error inserting batch into MongoDB: {e}")
                    continue  # Continue processing even if there's an error

    if batch:
        try:
            collection.insert_many(batch)
        except Exception as e:
            print(f"{datetime.now()} - ----Error inserting final batch into MongoDB: {e}")

    print(f"{datetime.now()} - ----Source file2mongo: {source['name']} -> {dataset['name']}... Done!")

def _process_mongo(timestamp):
    print(f"{datetime.now()} - Processing mongo datasets...")	

    datasets = get_datasets('raw', 'mongo')
    if not datasets:
        print(f"{datetime.now()} - --No datasets found.")
        return
    print(f"{datetime.now()} - --Found {len(datasets)} datasets.")

    mongo_url = get_mongo_config()
    print(f"{datetime.now()} - --Connecting to MongoDB: {mongo_url}.")
    client = MongoClient(mongo_url)

    for dataset in datasets:
        print(f"{datetime.now()} - --Dataset: {dataset['name']}.")
        source = dataset['source']
        if source['type'] == 'dataset':
            source_details = source['dataset_details']
            sources = [(source_details['area'], source_name) for source_name in source_details['items']]
            for ds in sources:
                source_dataset = get_dataset(ds[0], ds[1])
                if source_dataset['type'] == 'file':
                    _process_file2mongo_source(dataset, source_dataset, timestamp, client)
                else:
                    print(f"{datetime.now()} - --Unsupported source type: {source['type']}:{source_dataset['type']} for target type: {dataset['type']}")
        else:
            print(f"{datetime.now()} - --Unsupported source type: {source['type']} for target type: {dataset['type']}")

    print(f"{datetime.now()} - Processing mongo datasets... Done.")

def _process_files(timestamp):
    print(f"{datetime.now()} - Processing file datasets...")

    datasets = get_datasets('raw', 'file')
    if not datasets:
        print(f"{datetime.now()} - --No datasets found.")
        return
    print(f"{datetime.now()} - --Found {len(datasets)} datasets.")

    for dataset in datasets:
        print(f"{datetime.now()} - --Dataset: {dataset['name']}.")
        source = dataset['source']
        if source['type'] == 'dataset':
            source_details = source['dataset_details']
            sources = [(source_details['area'], source_name) for source_name in source_details['items']]
            for ds in sources:
                source_dataset = get_dataset(ds[0], ds[1])
                if source_dataset['type'] == 'file':
                    _process_file2file_source(dataset, source_dataset, timestamp)
                else:
                    print(f"{datetime.now()} - --Unsupported source type: {source['type']}:{source_dataset['type']} for target type: {dataset['type']}")	
        else:
            print(f"{datetime.now()} - --Unsupported source type: {source['type']} for target type: {dataset['type']}")

    print(f"{datetime.now()} - Processing file datasets... Done.")

def load_raw():
    print("\nLoading Raw Zone...")
    print("===================\n")

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    print(f"Timestamp: {timestamp}\n")

    _process_files(timestamp)
    _process_mongo(timestamp)
