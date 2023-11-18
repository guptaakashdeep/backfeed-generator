import subprocess as sp
from datetime import datetime
import argparse
from etlparam import read_basel_backfeed
import re


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-pf",
        "--part_files_loc",
        type=str,
        required=True,
        help="EMR feed basel location.",
    )
    parser.add_argument(
        "-fn",
        "--feed_name",
        type=str,
        required=True,
        help="EMR feed basel location.",
    )
    parser.add_argument(
        "-sn",
        "--sql_name",
        type=str,
        required=True,
        help="EMR feed basel location.",
    )

    args = parser.parse_args()
    response = {}
    tmp_location = args.part_files_loc
    feed_name = args.feed_name
    sql_name = args.sql_name
    BACKFEED_CONFIG = read_basel_backfeed()
    FEED_CONFIG = BACKFEED_CONFIG[feed_name]
    base_location = tmp_location.split("part-files")[0]
    print(f"emr location: {tmp_location} \n base_location: {base_location}")
    FILE_NM_SUFFIX = datetime.strftime(datetime.now(), "%y%m%d%H%M")
    FEED_TBL_NM = FEED_CONFIG.get("table_name")
    # get the final file names
    header_file = f"headers_{feed_name}.txt"
    if "sqls" in FEED_CONFIG.keys() and FEED_CONFIG.get("sqls").get(sql_name, None):
        feed_file = list(FEED_CONFIG.get("sqls").get(sql_name).keys())[0]
        data_file_name = f"{feed_file}_back_feed_{FILE_NM_SUFFIX}.txt" # config
    else:
        data_file_name = f"{FEED_TBL_NM}_back_feed_{FILE_NM_SUFFIX}.txt" 
    content = ["#! /bin/bash",f"cat {tmp_location}*.csv >> {base_location}{data_file_name}"]
    print (content)
    merge_script_loc = f'{base_location}merge.sh'
    with open(f'{merge_script_loc}', 'w', encoding='utf-8') as fw:
        fw.write("\n".join(content))
    ch_mod_op = sp.run([f"chmod 775 {merge_script_loc}"], shell=True)
    if ch_mod_op.returncode == 0:
        print("Permission updated.")

    # MERGE Process start
    print(datetime.now())
    merge_header = sp.run([f"cat {base_location}{header_file} >> {base_location}{data_file_name}"], shell=True)
    if merge_header.returncode == 0:
        print("headers are set.")
    else:
        raise Exception("headers merge failed.")
    print(datetime.now())
    print(datetime.now())
    merge_op = sp.run([f"bash {merge_script_loc}"], shell=True)
    if merge_op.returncode == 0:
        print("merge successful.")
    else:
        raise Exception("merge failed.")
    print(datetime.now())
    
    merged_file_loc = f"{base_location}{data_file_name}"
	# Calculate checksum
    print(datetime.now())
    checksum_op = sp.run([f"md5sum {merged_file_loc}"], shell=True, capture_output=True)
    if checksum_op.returncode == 0:
        checksum = re.split(r'\s+',checksum_op.stdout.decode('utf-8'))[0]
        print(checksum)
    else:
        raise Exception("checksum failed")
    print(datetime.now())

    print(datetime.now())
    wcl_cmd = sp.run([f"wc -l {merged_file_loc}"], shell=True, capture_output=True)
    num_records = re.split(r'\s+',wcl_cmd.stdout.decode('utf-8'))[0]
    # zip the file
    zip_op = sp.run([f"gzip {merged_file_loc}"], shell=True)
    if zip_op.returncode == 0:
        print("gzip is success.")
    else:
        raise Exception("zip failed.")
    print(datetime.now())
    # create ctl file
    # content = ["data_filename|data_total_records|run_rk|aws_insert_ts|snap_dt|business_date|checksum"]
    response = {
        "data_filename": f"{data_file_name}.gz",
        "data_total_records": num_records,
        "run_rk": FILE_NM_SUFFIX,
        "aws_insert_ts": datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        "snap_dt": datetime.strftime(datetime.now(), '%Y-%m-%d'),
        "business_date": datetime.strftime(datetime.now(), '%Y-%m-%d'),
        "checksum": checksum
    }
    delimiter = FEED_CONFIG.get("delimiter")
    ctl_headers = delimiter.join(list(response.keys())) + "\n"
    ctl_values = delimiter.join(list(response.values()))
    ctl_filename = f"ctl_{data_file_name}"
    ctl_file_loc = f"{base_location}{ctl_filename}"
    with open(ctl_file_loc, 'w', encoding='utf-8') as fw:
        fw.write(f"{ctl_headers}{ctl_values}")
    
    # rename ddl file
    dd_file_loc = f"{base_location}ddl_{feed_name}.txt"
    new_name_loc = f"{base_location}ddl_{data_file_name}"
    rename_cmd = sp.run([f"mv {dd_file_loc} {new_name_loc}"],shell=True, check=True)
    print("DDL file generation successful.")

    #create .ok file
    if int(num_records):
        feed_ok_filename = f"{data_file_name.split('.')[0]}.ok"
        with open(f"{base_location}{feed_ok_filename}", 'w', encoding='utf-8') as fw:
            fw.write("")
        print(".ok file is generated.")
    else:
        raise Exception("No records present in feed file. Not creating OK file.")
    
    print(f"""--feed_name {feed_name} --base_location {base_location} --feed_file {data_file_name}.gz --ddl_file ddl_{data_file_name} --ctl_file {ctl_filename} --ok_file {feed_ok_filename}""")
