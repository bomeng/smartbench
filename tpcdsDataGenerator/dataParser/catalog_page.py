from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "catalog_page.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(cp_catalog_page_sk = x[0],cp_catalog_page_id = x[1],cp_start_date_sk = x[2],cp_end_date_sk = x[3],cp_department = x[4],cp_catalog_number = x[5],cp_catalog_page_number = x[6],cp_description = x[7],cp_type = x[8])).toDF()
textDataDF.write.saveAsTable("tpcds.catalog_page", format="parquet", mode="overwrite")
