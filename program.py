from datalake import get_load_raw_step_enable, get_load_landing_step_enable, get_load_refined_step_enable
from datalake.landing2raw import load_raw
from datalake.raw2refined import load_refined
from datalake.sources2landing import load_landing

def main():
    if get_load_landing_step_enable():
        load_landing()

    if get_load_raw_step_enable():
        load_raw()
    
    if get_load_refined_step_enable():
        load_refined()

if __name__ == "__main__":
    main()