from datalake.stats import stats_for_raw
from datalake.landing2raw import load_raw
from datalake.sources2landing import get_sources

def main():
    get_sources()
    load_raw()

if __name__ == "__main__":
    main()