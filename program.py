from datalake.stats import run_stats
from datalake.landing import load_datasets
from dotenv import dotenv_values

def main():
    config = dotenv_values(".env")

    run_stats()
    load_datasets(config)

if __name__ == "__main__":
    main()