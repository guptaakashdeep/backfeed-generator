import subprocess as sp
from io import StringIO
import pandas as pd
import os
from etlparam import ENV, read_basel_backfeed
import argparse


def get_max_avail_mem():
    cmd_result = sp.run(["df -h"], shell=True, capture_output=True)
    if cmd_result.stdout:
        result_str = cmd_result.stdout.decode('utf-8').replace("Mounted on", "Mounted_on")
        disk_df = pd.read_csv(StringIO(result_str), delim_whitespace=True)
        #TODO: check here if anything is in MB or KB
        disk_df["avail_mem"] = disk_df['Avail'].apply(lambda x: float(x.replace('G','')))
        max_space_list = disk_df[disk_df['avail_mem'] == disk_df['avail_mem'].max()].to_dict(orient='records')
        if max_space_list:
            return max_space_list[0].get("Mounted_on", None)
        else:
            print("No disk space available.")
    else:
        raise Exception("No output from command df -h")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-fn",
        "--feed_name",
        type=str,
        required=True,
        help="Key to be referred in basel backfeed config.",
    )
    args = parser.parse_args()
    FEED_NAME = args.feed_name
    
    # get the path with max memory
    base_disk = get_max_avail_mem()
    # create folder for arc fe feed generation: base-location
    emr_base_location = f"{base_disk}/arc_fe_feed_generation{ENV}"
    os.makedirs(emr_base_location, exist_ok=True)
    sp.run([f"chmod 777 {emr_base_location}"], shell=True)
    # create folder as per filename inside this common folder: from config file
    CONFIG = read_basel_backfeed()
    FEED_CONFIG = CONFIG.get(FEED_NAME)
    FEED_TBL_NM = FEED_CONFIG.get('table_name')
    emr_sub_locations = []
    if "sqls" in FEED_CONFIG.keys():
        feed_names = list(FEED_CONFIG["sqls"].keys())
        print("Creating space for each feed file.")
        for fname in feed_names:
            feed_base_loc = f"{emr_base_location}/{FEED_NAME}/{fname}"
            os.makedirs(feed_base_loc, exist_ok=True)
            print(f"{feed_base_loc} created.")
            emr_sub_locations.append(feed_base_loc)
            # generate part-files folder:
            os.makedirs(f"{feed_base_loc}/part-files", exist_ok=True)
    else:
        print("No sqls available. Taking the default feed filename from table name.")
        feed_base_loc = f"{emr_base_location}/{FEED_NAME}/{FEED_TBL_NM}"
        os.makedirs(feed_base_loc, exist_ok=True)
        emr_sub_locations.append(feed_base_loc)
        print(f"{feed_base_loc} generated.")
        # generate part-files folder:
        os.makedirs(f"{feed_base_loc}/part-files", exist_ok=True)
    # return a string containing base location
    emr_sub_loc_str = " ".join(emr_sub_locations)
    sp.run([f"chmod -R 777 {emr_base_location}"], shell=True, check=True)
    print(f"--feed_name {FEED_NAME} --base_location {emr_base_location}")
    
