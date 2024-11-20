# # elt2landing.py

# from pymongo import MongoClient 
# from datalake import DATASETS
# from psycopg2 import connect
# from csv import reader, writer
# from time import time
# from sys import stdout
# from os.path import join
# from os import listdir
# from io import StringIO
# import json


# def _load_json_dataset(dataset_name):
#     with open(DATASETS[dataset_name]) as f:
#         return json.load(f)

# class DataFile:
#     def __init__(self, appid, file_path, total_lines, progress_callback=None):
#         self.appid = appid
#         self.file_path = file_path
#         self.file = open(file_path, 'r', newline='', encoding='utf-8')
#         self.reader = reader(self.file)
#         self.output = StringIO()
#         self.writer = writer(self.output)
#         self.header = next(self.reader)
#         self.total_lines = total_lines
#         self.lines_read = 0
#         self.progress_callback = progress_callback
#         self.last_update_time = time()
#         self.update_interval = 1  # Update every second

#     def __iter__(self):
#         return self

#     def __next__(self):
#         row = next(self.reader)
#         if self.appid:
#             row.insert(0, self.appid)
#         self.lines_read += 1

#         # Write the row to the output StringIO using csv.writer
#         self.output.seek(0)
#         self.output.truncate(0)
#         self.writer.writerow(row)
#         data = self.output.getvalue()

#         # Call progress callback periodically
#         current_time = time()
#         if self.progress_callback and (current_time - self.last_update_time >= self.update_interval):
#             self.progress_callback(self.lines_read, self.total_lines, self.file_path)
#             self.last_update_time = current_time

#         return data

#     def read(self, size=-1):
#         if size < 0:
#             data = ''.join([line for line in self])
#             # Final progress update
#             if self.progress_callback:
#                 self.progress_callback(self.lines_read, self.total_lines, self.file_path)
#             return data
#         else:
#             data = ''
#             try:
#                 while len(data) < size:
#                     data += next(self)
#             except StopIteration:
#                 # Final progress update
#                 if self.progress_callback:
#                     self.progress_callback(self.lines_read, self.total_lines, self.file_path)
#             return data

#     def close(self):
#         self.file.close()
#         self.output.close()

# def _get_total_lines(file_path):
#     with open(file_path, 'r') as f:
#         total = sum(1 for _ in f) - 1  # Exclude header line
#     return total

# def _progress_callback(lines_read, total_lines, file_path):
#     percent = (lines_read / total_lines) * 100
#     stdout.write(f"\rProcessing {file_path}: {lines_read}/{total_lines} lines ({percent:.2f}%)")
#     stdout.flush()

# def _copy_csv_file(cursor, file_path, schema, table):
#     total_lines = _get_total_lines(file_path)
#     data_file = DataFile(None, file_path, total_lines, _progress_callback)
#     sql = f"COPY {schema}.{table} ({' ,'.join(data_file.header)}) FROM STDIN WITH CSV"
#     stdout.write(f"Processing {sql}: 0/{total_lines} lines (0.00%)")
#     stdout.flush()
#     cursor.copy_expert(sql, data_file)
#     data_file.close()
#     # Move to the next line after processing each file
#     stdout.write("\n")
#     stdout.flush()

# def _copy_csv_folder(cursor, file_path, appid, schema, table):
#     total_lines = _get_total_lines(file_path)
#     data_file = DataFile(appid, file_path, total_lines, _progress_callback)
#     sql = f"COPY {schema}.{table} (appid, {', '.join(data_file.header)}) FROM STDIN WITH CSV"
#     cursor.copy_expert(sql, data_file)
#     data_file.close()
#     # Move to the next line after processing each file
#     stdout.write("\n")
#     stdout.flush()

# def _ensure_postresql_schema(conn, config):
#     schema = config['POSTGRES_LANDING_SCHEMA']
#     mendeley_playercount = config['POSTGRES_LANDING_TABLE_MENDELEY_PLAYERCOUNT']
#     mendeley_price = config['POSTGRES_LANDING_TABLE_MENDELEY_PRICE']
#     mendeley_apps = config['POSTGRES_LANDING_TABLE_MENDELEY_APPS']

#     cursor = conn.cursor()

#     cursor.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(schema))

#     cursor.execute('DROP TABLE IF EXISTS {}.{}'.format(schema, mendeley_playercount))
#     cursor.execute('DROP TABLE IF EXISTS {}.{}'.format(schema, mendeley_price))
#     cursor.execute('DROP TABLE IF EXISTS {}.{}'.format(schema, mendeley_apps))

#     '''
#     Sample
#     appid,Time,Playercount
#     578080,2017-12-14 00:00,3354
#     '''
#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS {}.{} (
#             appid TEXT,
#             time TEXT,
#             playercount TEXT
#         )
#     '''.format(schema, mendeley_playercount))

#     '''
#     Sample
#     appid,Date,Initialprice,Finalprice,Discount
#     578080,2019-04-07,9.99,9.99,0
#     '''
#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS {}.{} (
#             appid TEXT,
#             date TEXT,
#             initialprice TEXT,
#             finalprice TEXT,
#             discount TEXT
#         )
#     '''.format(schema, mendeley_price))

#     '''
#     Sample
#     appid,type,name,releasedate,freetoplay
#     578080,game,PLAYERUNKNOWN'S BATTLEGROUNDS,21-Dec-17,0
#     '''
#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS {}.{} (
#             appid TEXT,
#             type TEXT,
#             name TEXT,
#             releasedate TEXT,
#             freetoplay TEXT
#         )
#     '''.format(schema, mendeley_apps))

#     conn.commit()

# def _load_datasets_to_postgres(config):
#     conn = connect(
#         dbname=config['POSTGRES_DB'],
#         user=config['POSTGRES_USER'],
#         password=config['POSTGRES_PASS'],
#         host=config['POSTGRES_SERVER'],
#         port=config['POSTGRES_PORT']
#     )

#     _ensure_postresql_schema(conn, config)

#     cursor = conn.cursor()

#     print("Loading Mendeley datasets: playercount")
#     mendeley_playercount = config['POSTGRES_LANDING_TABLE_MENDELEY_PLAYERCOUNT']
    
#     total_files = sum(len(listdir(DATASETS[folder_name])) for folder_name in ['mendeley_playercount_1', 'mendeley_playercount_2'])
#     files_processed = 0

#     # Process each CSV file
#     for folder_name in ['mendeley_playercount_1', 'mendeley_playercount_2']:
#         folder_path = DATASETS[folder_name]
#         file_list = listdir(folder_path)
#         for file_name in file_list:
#             appid = file_name.split('.')[0]
#             file_path = join(folder_path, file_name)
#             print(f"Processing file {files_processed + 1}/{total_files}: {file_name}")
#             _copy_csv_folder(cursor, file_path, appid, config['POSTGRES_LANDING_SCHEMA'], mendeley_playercount)
#             files_processed += 1

#     conn.commit()

#     print("Loading Mendeley datasets: price")
#     mendeley_price = config['POSTGRES_LANDING_TABLE_MENDELEY_PRICE']

#     total_files = len(listdir(DATASETS['mendeley_price']))
#     files_processed = 0

#     # Process each CSV file
#     folder_path = DATASETS['mendeley_price']
#     file_list = listdir(folder_path)
#     for file_name in file_list:
#         appid = file_name.split('.')[0]
#         file_path = join(folder_path, file_name)
#         print(f"Processing file {files_processed + 1}/{total_files}: {file_name}")
#         _copy_csv_folder(cursor, file_path, appid, config['POSTGRES_LANDING_SCHEMA'], mendeley_price)
#         files_processed += 1

#     conn.commit()

#     print("Loading Mendeley datasets: apps")
#     mendeley_apps = config['POSTGRES_LANDING_TABLE_MENDELEY_APPS']

#     file_path = DATASETS['mendeley_apps']
#     print(f"Processing file: {file_path}")
#     _copy_csv_file(cursor, file_path, config['POSTGRES_LANDING_SCHEMA'], mendeley_apps)

#     conn.commit()

#     cursor.close()
#     conn.close()

# def _load_datasets_to_mongo(config):
#     mongo_url = 'mongodb://{}:{}@{}:{}/'.format(
#         config['MONGO_USER'],
#         config['MONGO_PASS'],
#         config['MONGO_HOST'],
#         config['MONGO_PORT']
#     )

#     print("Connecting to MongoDB: {}".format(mongo_url))

#     client = MongoClient(mongo_url)
#     db = client[config['MONGO_LANDING_DB']]
    
#     print("Loading datasets to MongoDB")

#     print("Loading Steam API dataset")
#     steamapi_ds = _load_json_dataset('steamapi')
#     db.drop_collection(config['MONGO_LANDING_COLLECTION_STEAMAPI'])
#     steamapi = db[config['MONGO_LANDING_COLLECTION_STEAMAPI']]
#     steamapi.insert_many(steamapi_ds)

#     print("Loading Steam Spy dataset")
#     steamspy_ds = _load_json_dataset('steamspy')
#     db.drop_collection(config['MONGO_LANDING_COLLECTION_STEAMSPY'])
#     steamspy = db[config['MONGO_LANDING_COLLECTION_STEAMSPY']]
#     steamspy.insert_many(steamspy_ds)


# def load_datasets(config):
#     _load_datasets_to_mongo(config)
#     _load_datasets_to_postgres(config)


