from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "warehouse.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(w_warehouse_sk = x[0],w_warehouse_id = x[1],w_warehouse_name = x[2],w_warehouse_sq_ft = x[3],w_street_number = x[4],w_street_name = x[5],w_street_type = x[6],w_suite_number = x[7],w_city = x[8],w_county = x[9],w_state = x[10],w_zip = x[11],w_country = x[12],w_gmt_offset = x[13])).toDF()
textDataDF.write.saveAsTable("tpcds.warehouse", format="parquet", mode="overwrite")
