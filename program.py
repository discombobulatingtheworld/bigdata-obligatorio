from datalake import get_download_sources_step_enable
from datalake.landing2raw import load_raw
from datalake.raw2refined import load_refined
from datalake.sources2landing import get_sources

def main():
    if get_download_sources_step_enable():
        get_sources()
        load_raw()
    load_refined()

if __name__ == "__main__":
    main()