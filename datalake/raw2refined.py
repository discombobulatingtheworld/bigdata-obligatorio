import re
from datetime import datetime

from pyspark.sql.functions import explode, col, udf, when
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, BooleanType, ArrayType, IntegerType, FloatType

from datalake import get_elasticsearch_config, CONFIG, get_dataset, get_spark_config, get_mongo_config_uri_raw, get_mongo_config

def get_usd_conversion_rate():
    return {
    "USD": 1,
    "AED": 3.6725,
    "AFN": 69.0899,
    "ALL": 93.9822,
    "AMD": 389.5171,
    "ANG": 1.79,
    "AOA": 934.0772,
    "ARS": 1006.75,
    "AUD": 1.5382,
    "AWG": 1.79,
    "AZN": 1.7,
    "BAM": 1.8769,
    "BBD": 2,
    "BDT": 120.2699,
    "BGN": 1.8769,
    "BHD": 0.376,
    "BIF": 2972.8892,
    "BMD": 1,
    "BND": 1.3476,
    "BOB": 7.0026,
    "BRL": 5.8132,
    "BSD": 1,
    "BTN": 84.4995,
    "BWP": 13.7563,
    "BYN": 3.4117,
    "BZD": 2,
    "CAD": 1.3981,
    "CDF": 2871.8698,
    "CHF": 0.892,
    "CLP": 974.0245,
    "CNY": 7.2537,
    "COP": 4443.1117,
    "CRC": 513.659,
    "CUP": 24,
    "CVE": 105.8142,
    "CZK": 24.3202,
    "DJF": 177.721,
    "DKK": 7.16,
    "DOP": 61.0075,
    "DZD": 134.9007,
    "EGP": 49.6717,
    "ERN": 15,
    "ETB": 122.2186,
    "EUR": 0.9596,
    "FJD": 2.2728,
    "FKP": 0.7978,
    "FOK": 7.16,
    "GBP": 0.7978,
    "GEL": 2.7393,
    "GGP": 0.7978,
    "GHS": 16.0153,
    "GIP": 0.7978,
    "GMD": 72.3775,
    "GNF": 8689.0575,
    "GTQ": 7.8058,
    "GYD": 211.6475,
    "HKD": 7.784,
    "HNL": 25.5491,
    "HRK": 7.2304,
    "HTG": 132.2359,
    "HUF": 394.4944,
    "IDR": 15916.7177,
    "ILS": 3.7163,
    "IMP": 0.7978,
    "INR": 84.4995,
    "IQD": 1325.1661,
    "IRR": 42282.1618,
    "ISK": 139.7586,
    "JEP": 0.7978,
    "JMD": 157.8867,
    "JOD": 0.709,
    "JPY": 154.5952,
    "KES": 129.33,
    "KGS": 86.4416,
    "KHR": 4105.6156,
    "KID": 1.5382,
    "KMF": 472.1099,
    "KRW": 1403.9977,
    "KWD": 0.3097,
    "KYD": 0.8333,
    "KZT": 498.2646,
    "LAK": 22213.6751,
    "LBP": 89500,
    "LKR": 291.6983,
    "LRD": 182.9667,
    "LSL": 18.0985,
    "LYD": 4.9328,
    "MAD": 10.0721,
    "MDL": 18.241,
    "MGA": 4732.5911,
    "MKD": 59.0262,
    "MMK": 2123.4326,
    "MNT": 3447.4633,
    "MOP": 8.0176,
    "MRU": 40.4329,
    "MUR": 46.5856,
    "MVR": 15.4202,
    "MWK": 1753.9342,
    "MXN": 20.4478,
    "MYR": 4.4665,
    "MZN": 64.4964,
    "NAD": 18.0985,
    "NGN": 1676.5712,
    "NIO": 37.1949,
    "NOK": 11.0836,
    "NPR": 135.1992,
    "NZD": 1.7136,
    "OMR": 0.3845,
    "PAB": 1,
    "PEN": 3.7931,
    "PGK": 4.0701,
    "PHP": 58.9815,
    "PKR": 277.8096,
    "PLN": 4.1621,
    "PYG": 7930.5571,
    "QAR": 3.64,
    "RON": 4.7739,
    "RSD": 112.0446,
    "RUB": 102.2361,
    "RWF": 1405.0941,
    "SAR": 3.75,
    "SBD": 8.586,
    "SCR": 14.3004,
    "SDG": 592.9745,
    "SEK": 11.0533,
    "SGD": 1.3476,
    "SHP": 0.7978,
    "SLE": 22.8887,
    "SLL": 22888.6638,
    "SOS": 578.1383,
    "SRD": 36.058,
    "SSP": 3407.3824,
    "STN": 23.5111,
    "SYP": 13130.9897,
    "SZL": 18.0985,
    "THB": 34.5799,
    "TJS": 10.744,
    "TMT": 3.5249,
    "TND": 3.1768,
    "TOP": 2.3859,
    "TRY": 34.5637,
    "TTD": 6.8365,
    "TVD": 1.5382,
    "TWD": 32.5468,
    "TZS": 2642.2198,
    "UAH": 41.3089,
    "UGX": 3729.6898,
    "UYU": 43.2184,
    "UZS": 12971.2019,
    "VES": 46.6176,
    "VND": 25435.2135,
    "VUV": 120.6669,
    "WST": 2.7961,
    "XAF": 629.4798,
    "XCD": 2.7,
    "XDR": 0.7694,
    "XOF": 629.4798,
    "XPF": 114.5153,
    "YER": 252.6387,
    "ZAR": 18.0986,
    "ZMW": 27.9335,
    "ZWL": 25.3326
}

def present_field_transformer(field):
    return False if field is None else True

def extract_categories(categories):
    if categories is not None:
        return [category['description'] for category in categories]
    return []

def array_count(array):
    if array is not None:
        return len(array)
    return 0

def date_converter(date):
    if date is None:
        return None
    elif date.lower() in ['coming soon', 'to be announced']:
        return None
    elif re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return datetime.strptime(date, "%Y-%m-%d").strftime("%Y/%m/%d %H:%M:%S")
    elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", date):
        return datetime.strptime(date, "%Y-%m-%d %H:%M").strftime("%Y/%m/%d %H:%M:%S")
    elif re.match(r"^\d{1,2} [A-Za-z]{3}, \d{4}$", date):
        return datetime.strptime(date, "%d %b, %Y").strftime("%Y/%m/%d %H:%M:%S")
    elif re.match(r"^[A-Za-z]{3} \d{1,2}, \d{4}$", date):
        return datetime.strptime(date, "%b %d, %Y").strftime("%Y/%m/%d %H:%M:%S")
    elif re.match(r"^[A-Za-z]{3} \d{4}$", date):
        return datetime.strptime(date, "%b %Y").strftime("%Y/%m/%d %H:%M:%S")
    elif re.match(r"^[A-Za-z]+ \d{4}$", date):
        return datetime.strptime(date, "%B %Y").strftime("%Y/%m/%d %H:%M:%S")
    elif re.match(r"^Q[1-4] \d{4}$", date):
        match = re.match(r"^Q([1-4]) (\d{4})$", date)
        quarter = int(match.group(1))
        year = int(match.group(2))
        month = (quarter - 1) * 3 + 1
        return datetime(year, month, 1).strftime("%Y/%m/%d %H:%M:%S")
    elif date.isdigit() and len(date) == 4:
        return datetime(int(date), 1, 1).strftime("%Y/%m/%d %H:%M:%S")
    else:
        return None

def min_owners(owners_range):
    if owners_range is not None:
        parts = owners_range.replace(" ", "").replace(",", "").split("..")
        return int(parts[0])
    return None

def max_owners(owners_range):
    if owners_range is not None:
        parts = owners_range.replace(" ", "").replace(",", "").split("..")
        return int(parts[1])
    return None

def price_converter(price_details):
    if price_details is not None and 'final' in price_details and 'currency' in price_details:

        conversion_rate = get_usd_conversion_rate().get(price_details['currency'])
        return (price_details['final']/100) / conversion_rate
    return None


present_field_udf = udf(present_field_transformer, BooleanType())
extract_categories_udf = udf(extract_categories, ArrayType(StringType()))
array_count_udf = udf(array_count, IntegerType())
date_converter_udf = udf(date_converter, StringType())
min_owners_udf = udf(min_owners, IntegerType())
max_owners_udf = udf(max_owners, IntegerType())
price_converter_udf = udf(price_converter, FloatType())


def get_valid_steam_api_data(spark_session):
    steam_api_details = spark_session.read.format("mongodb").option("collection", "steamapi_appdetails").load()
    steam_api_details_exploded = steam_api_details.select(explode(col("data")).alias("app_id", "steam_api"))
    steam_api_details_exploded = steam_api_details_exploded.filter((col("steam_api.success") == True))
    return steam_api_details_exploded

def get_steam_apps_details(spark_session):
    dataset_info = get_dataset('refined', 'steam_apps')

    print(f"--Starting {dataset_info['elasticsearch_index']} elastic index population")

    steam_api_details = get_valid_steam_api_data(spark_session)
    steam_spy_details = spark_session.read.format("mongodb").option("collection", "steamspy_appdetails").load()
    steam_spy_details_data = steam_spy_details.select(col("data").alias("steam_spy"))
    steam_app_details = steam_api_details.join(steam_spy_details_data, steam_api_details["app_id"] == steam_spy_details_data["steam_spy.appid"], "left")

    df_steam_app = steam_app_details.select(
        col("steam_api.data.name").alias("name"),
        col("steam_api.data.type").alias("type"),
        array_count_udf(col("steam_api.data.dlc")).alias("dlcs_count"),
        col("steam_api.data.steam_appid").alias("app_id"),
        col("steam_api.data.is_free").alias("is_free"),
        col("steam_api.data.required_age").cast("int").alias("required_age"),
        present_field_udf(col("steam_api.data.website")).alias("has_website"),
        present_field_udf(col("steam_api.data.about_the_game")).alias("has_about_the_game"),
        present_field_udf(col("steam_api.data.short_description")).alias("has_short_description"),
        present_field_udf(col("steam_api.data.detailed_description")).alias("has_detailed_description"),
        col("steam_api.data.metacritic.score").alias("metacritic_score"),
        col("steam_api.data.developers").alias("developers"),
        col("steam_api.data.publishers").alias("publishers"),
        extract_categories_udf(col("steam_api.data.categories")).alias("categories"),
        extract_categories_udf(col("steam_api.data.genres")).alias("genres"),
        col("steam_api.data.release_date.coming_soon").alias("is_coming_soon"),
        date_converter_udf(col("steam_api.data.release_date.date")).alias("release_date"),
        price_converter_udf(col("steam_api.data.price_overview")).alias("usd_price"),
        col("steam_spy.positive").alias("positives_reviews"),
        col("steam_spy.negative").alias("negatives_reviews"),
        col("steam_spy.userscore").alias("users_score"),
        min_owners_udf(col("steam_spy.owners")).alias("min_owners"),
        max_owners_udf(col("steam_spy.owners")).alias("max_owners"),
    )

    df_steam_app.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", dataset_info['elasticsearch_index']) \
        .option("es.mapping.id", "app_id") \
        .mode("overwrite") \
        .save()

    print(f"--Finalized {dataset_info['elasticsearch_index']} elastic index population")

def get_steam_apps_price_history(spark_session):
    dataset_info = get_dataset('refined', 'price_history')

    print(f"--Starting {dataset_info['elasticsearch_index']} elastic index population")

    steam_api_details = get_valid_steam_api_data(spark_session)
    mendeley_price_details = spark_session.read.format("mongodb").option("collection", "mendeley_price").load()
    mendeley_price_details_data = mendeley_price_details.select(col("data").alias("mendeley_price_data"))
    steam_apps_price_details = steam_api_details.join(mendeley_price_details_data, steam_api_details["app_id"] == mendeley_price_details_data["mendeley_price_data.appid"], "inner")

    df_steam_app = steam_apps_price_details.select(
        col("steam_api.data.name").alias("name"),
        col("steam_api.data.type").alias("type"),
        col("steam_api.data.steam_appid").alias("app_id"),
        date_converter_udf(col("steam_api.data.release_date.date")).alias("release_date"),
        price_converter_udf(col("steam_api.data.price_overview")).alias("initial_usd_price"),
        date_converter_udf(col("mendeley_price_data.data.Date")).alias("date"),
        col("mendeley_price_data.data.Initialprice").alias("initial_day_price"),
        col("mendeley_price_data.data.Finalprice").alias("final_day_price"),
        col("mendeley_price_data.data.Discount").alias("discount_percentage"),
    )

    df_steam_app.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", dataset_info['elasticsearch_index']) \
        .mode("overwrite") \
        .save()

    print(f"--Finalized {dataset_info['elasticsearch_index']} elastic index population")


def get_steam_apps_players_count_history(spark_session):
    dataset_info = get_dataset('refined', 'players_count')

    print(f"--Starting {dataset_info['elasticsearch_index']} elastic index population")

    steam_api_details = get_valid_steam_api_data(spark_session)
    mendeley_players_details = spark_session.read.format("mongodb").option("collection", "mendeley_playercount").load()
    mendeley_players_details_data = mendeley_players_details.select(col("data").alias("mendeley_playercount_data"))

    steam_apps_price_details = steam_api_details.join(mendeley_players_details_data,
                                                      steam_api_details["app_id"] == mendeley_players_details_data[
                                                          "mendeley_playercount_data.appid"], "inner")
    df_steam_app = steam_apps_price_details.select(
        col("steam_api.data.name").alias("name"),
        col("steam_api.data.type").alias("type"),
        col("steam_api.data.steam_appid").alias("app_id"),
        date_converter_udf(col("steam_api.data.release_date.date")).alias("release_date"),
        date_converter_udf(col("mendeley_playercount_data.data.Time")).alias("date"),
        when(col("mendeley_playercount_data.data.Playercount") == "NaN", None).otherwise(col("mendeley_playercount_data.data.Playercount")).alias("player_count")
    )


    df_steam_app.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", dataset_info['elasticsearch_index']) \
        .mode("overwrite") \
        .save()

    print(f"--Finalized {dataset_info['elasticsearch_index']} elastic index population")



def load_refined():
    print(f"--Initializing refine process...")

    
    master_url = get_spark_config()
    mongo_uri = get_mongo_config()
    host_ip = 'host.docker.internal'
    driver_port = '9999'

    spark_session = SparkSession.builder \
        .appName("Raw to Refined") \
        .master(master_url) \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.16.0") \
        .config("spark.driver.host", host_ip) \
        .config("spark.driver.port", driver_port) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.read.database", "raw") \
        .config("es.nodes", get_elasticsearch_config()) \
        .config("es.nodes.wan.only", "true") \
        .config("es.nodes.discovery", "false") \
        .config("es.net.http.auth.user", CONFIG['ELASTICSEARCH_USERNAME']) \
        .config("es.net.http.auth.pass", CONFIG['ELASTICSEARCH_PASSWORD']) \
        .config("es.net.ssl", "true") \
        .config("es.net.ssl.cert.allow.self.signed", "true") \
        .getOrCreate()

    get_steam_apps_details(spark_session)
    get_steam_apps_price_history(spark_session)
    get_steam_apps_players_count_history(spark_session)
