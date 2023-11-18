# Spark script
from pyspark.sql import SparkSession
from etlparam import ENV, read_basel_backfeed
from utils import get_account_id
import argparse
from datetime import datetime

spark = SparkSession.builder.master("yarn").config("spark.sql.broadcastTimeout", "36000")\
    .config("spark.sql.crossJoin.enabled", "true").enableHiveSupport().getOrCreate()
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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
    args = parser.parse_args()
    EMR_BASE_LOC = args.base_location
    FEED_NAME = args.feed_name
    SQL_NAME = args.sql_name
    BACKFEED_CONFIG = read_basel_backfeed()
    FEED_CONFIG = BACKFEED_CONFIG.get(FEED_NAME)
    delimiter = FEED_CONFIG.get("delimiter")
    FEED_TBL_NM = FEED_CONFIG.get("table_name").format(ENV=ENV)
    FEED_DB = FEED_CONFIG.get("feed_table_db")
    AWS_ACCT_ID = get_account_id()
    S3_TMP_LOCATION = FEED_CONFIG.get("tmp_location").format(aws_acct_id=AWS_ACCT_ID)
    DATE_STR = datetime.strftime(datetime.date(datetime.now()),'%Y%m%d')
    SQL_PRESENT = False
    # check whether if anything is there in sqls
    if "sqls" in FEED_CONFIG.keys() and FEED_CONFIG.get("sqls").get(SQL_NAME):
        SQL_PRESENT = True
        for fname, sql in FEED_CONFIG.get("sqls").get(SQL_NAME).items():
            backfeed_df = spark.sql(sql)
            data_folder_path = f"{S3_TMP_LOCATION}/{SQL_NAME}/{fname}_{DATE_STR}/"
            print(f"S3 DATA FOLDER PATH: {data_folder_path}")
            backfeed_df.write.option("escape","").option("delimiter",delimiter).option("emptyValue","").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("header",False).mode("overwrite").csv(data_folder_path)
    else:
        backfeed_df = spark.read.table(FEED_TBL_NM)
        data_folder_path = f"{S3_TMP_LOCATION}/{FEED_TBL_NM}_{DATE_STR}/"
        print(f"S3 DATA FOLDER PATH: {data_folder_path}")
        backfeed_df.write.option("escape","").option("delimiter",delimiter).option("emptyValue","").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("header",False).mode("overwrite").csv(data_folder_path)
    # create temp headers file
    headers = backfeed_df.columns
    headers_str = delimiter.join(headers)
    header_file_name = f"headers_{FEED_NAME}.txt"
    if SQL_PRESENT:
        header_filepath = f"{EMR_BASE_LOC}/{FEED_NAME}/{SQL_NAME}"
    else:
        header_filepath = f"{EMR_BASE_LOC}/{FEED_NAME}/{FEED_TBL_NM}"
    
    with open(f'{header_filepath}/{header_file_name}', 'w', encoding='utf-8') as fw:
        fw.write(headers_str+"\n")
    print(f"Headers file is generated in {header_filepath}/{header_file_name}")

    # create tmp DDL file.
    # ddl_feed_ge_exp_pra_m_u_recon_back_feed_2310200443.txt
    ddl_filename = f"ddl_{FEED_NAME}.txt"
    schema_row_list = spark.sql(f"describe table {FEED_DB}.{FEED_TBL_NM}").collect()
    schema_list = []
    for row in schema_row_list:
        schema_list.append(f"{row.col_name}{delimiter}{row.data_type}")
    schema_str = "\n".join(schema_list)

    ddl_filepath = f"{header_filepath}/{ddl_filename}"
    with open(ddl_filepath, 'w', encoding='utf-8') as fw:
        fw.write(schema_str)