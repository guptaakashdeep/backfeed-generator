from etlparam import read_basel_backfeed
import argparse
from datetime import datetime
import subprocess as sp
from utils import get_account_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-bl",
        "--base_location",
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

    # get the SQL name for which data needs to be copied
    args = parser.parse_args()
    FEED_NAME = args.feed_name
    BASE_LOC = args.base_location
    SQL_NAME = args.sql_name
    DATE_STR = datetime.strftime(datetime.date(datetime.now()),'%Y%m%d')
    BACKFEED_CONFIG = read_basel_backfeed()
    FEED_CONFIG = BACKFEED_CONFIG.get(FEED_NAME)
    FEED_TBL_NM = FEED_CONFIG.get("table_name")
    # get S3 path: part files written
    AWS_ACCT_ID = get_account_id()
    S3_TMP_LOCATION = FEED_CONFIG.get("tmp_location").format(aws_acct_id=AWS_ACCT_ID)
    if "sqls" in FEED_CONFIG.keys() and FEED_CONFIG.get("sqls").get(SQL_NAME, None):
        for fname, sql in FEED_CONFIG.get("sqls").get(SQL_NAME).items():
            s3_data_folder_path = f"{S3_TMP_LOCATION}{SQL_NAME}/{fname}_{DATE_STR}/"
    else:
        print("No sqls present.")
        s3_data_folder_path = f"{S3_TMP_LOCATION}{FEED_TBL_NM}_{DATE_STR}/"
    # get emr path for copying part files.
    emr_part_files = f"{BASE_LOC}/{FEED_NAME}/{SQL_NAME}/part-files/"
    # copy data from S3 to EMR: .sh vs subprocess
    # bash_content = ["#! /bin/bash", f"aws s3 cp {s3_data_folder_path} {emr_part_files} --recursive"]
    print(f"s3_path: {s3_data_folder_path} \n emr_path: {emr_part_files}")
    copy_cmd = sp.run([f"aws s3 cp {s3_data_folder_path} {emr_part_files} --recursive"], shell=True, capture_output=True)
    if copy_cmd.stdout:
        print("Data is copied into EMR")
    else:
        print(copy_cmd.stderr.decode('utf-8'))
        raise Exception("No files were copied.")
    print(f"--feed_name {FEED_NAME} --sql_name {SQL_NAME} --part_files_loc {emr_part_files}")
