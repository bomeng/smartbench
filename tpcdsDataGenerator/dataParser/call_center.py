from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)
sc.setLogLevel("ERROR")

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "call_center.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(scc_call_center_sk = x[0],cc_call_center_id = x[1],cc_rec_start_date = x[2],cc_rec_end_date = x[3],cc_closed_date_sk = x[4],cc_open_date_sk = x[5],cc_name = x[6],cc_class = x[7],cc_employees = x[8],cc_sq_ft = x[9],cc_hours = x[10],cc_manager = x[11],cc_mkt_id = x[12],cc_mkt_class = x[13],cc_mkt_desc = x[14],cc_market_manager = x[15],cc_division = x[16],cc_division_name = x[17],cc_company = x[18],cc_company_name = x[19],cc_street_number = x[20],cc_street_name = x[21],cc_street_type = x[22],cc_suite_number = x[23],cc_city = x[24],cc_county = x[25],cc_state = x[26],cc_zip = x[27],cc_country = x[28],cc_gmt_offset = x[29],cc_tax_percentage = x[30])).toDF()
textDataDF.write.saveAsTable("tpcds.call_center", format="parquet", mode="overwrite")
