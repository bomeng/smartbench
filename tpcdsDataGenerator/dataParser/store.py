from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "store.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(s_store_sk = x[0],s_store_id = x[1],s_rec_start_date = x[2],s_rec_end_date = x[3],s_closed_date_sk = x[4],s_store_name = x[5],s_number_employees = x[6],s_floor_space = x[7],s_hours = x[8],s_manager = x[9],s_market_id = x[10],s_geography_class = x[11],s_market_desc = x[12],s_market_manager = x[13],s_division_id = x[14],s_division_name = x[15],s_company_id = x[16],s_company_name = x[17],s_street_number = x[18],s_street_name = x[19],s_street_type = x[20],s_suite_number = x[21],s_city = x[22],s_county = x[23],s_state = x[24],s_zip = x[25],s_country = x[26],s_gmt_offset = x[27],s_tax_precentage = x[28])).toDF()
textDataDF.write.saveAsTable("tpcds.store", format="parquet", mode="overwrite")
