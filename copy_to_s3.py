import subprocess as sp
import argparse
from etlparam import read_basel_backfeed
from utils import get_account_id


#--feed_name {feed_name} --base_location {base_location} --feed_file {data_file_name}.gz 
 #   --ddl_file ddl_{data_file_name} --ctl_file {ctl_filename} --ok_file {feed_ok_filename}
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base_location",
        type=str,
        required=True,
        help="EMR feed basel location.",
    )
    parser.add_argument(
        "--feed_name",
        type=str,
        required=True,
        help="Back feed name.",
    )
    parser.add_argument(
        "--feed_file",
        type=str,
        required=True,
        help="FEED filename.",
    )
    parser.add_argument(
        "--ddl_file",
        type=str,
        required=True,
        help="DDL filename.",
    )
    parser.add_argument(
        "--ctl_file",
        type=str,
        required=True,
        help="ctl filename.",
    )
    parser.add_argument(
        "--ok_file",
        type=str,
        required=True,
        help="ok filename.",
    )

    args = parser.parse_args()
    BASE_LOCATION = args.base_location
    FEED_NAME = args.feed_name
    cp_request_dict = {
        "FEED_FILE_NM": args.feed_file,
        "CTL_FILE_NM": args.ctl_file,
        "DDL_FILE_NM": args.ddl_file,
        "OK_FILE_NM": args.ok_file
    }

    # read config
    BACKFEED_CONFIG = read_basel_backfeed()
    FEED_CONFIG = BACKFEED_CONFIG[FEED_NAME]
    TGT_LOCATION = FEED_CONFIG["feed_location"].format(aws_acct_id=get_account_id())
    script_content = ["#! /bin/bash"]
    aws_cp_cmd = "aws s3 cp {src_loc} {tgt_loc}"
    for fname in cp_request_dict.values():
        src = f"{BASE_LOCATION}{fname}"
        script_content.append(aws_cp_cmd.format(src_loc=src, tgt_loc=TGT_LOCATION))

    script_content_str = "\n".join(script_content)
    print("script_content: \n", script_content_str)
    copy_script = f"{BASE_LOCATION}/emr_to_s3.sh"
    with open(copy_script, 'w', encoding='utf-8') as fw:
        fw.write(script_content_str)
    
    # run script
    sp.run([f"chmod 777 {copy_script}"], shell=True)
    sp.run([f"bash {copy_script}"], shell=True, check=True)

    # cleanup space to release memory used.
    rm_cmd = sp.run([f"rm -rf {BASE_LOCATION}"], shell=True, capture_output=True, check=True)
    print(rm_cmd.stdout.decode('utf-8'))
