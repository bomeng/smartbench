from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "customer_address.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(ca_address_sk = x[0],ca_address_id = x[1],ca_street_number = x[2],ca_street_name = x[3],ca_street_type = x[4],ca_suite_number = x[5],ca_city = x[6],ca_county = x[7],ca_state = x[8],ca_zip = x[9],ca_country = x[10],ca_gmt_offset = x[11],ca_location_type = x[12])).toDF()
textDataDF.write.saveAsTable("tpcds.customer_address", format="parquet", mode="overwrite")
