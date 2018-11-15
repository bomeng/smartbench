from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "ship_mode.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(sm_ship_mode_sk = x[0],sm_ship_mode_id = x[1],sm_type = x[2],sm_code = x[3],sm_carrier = x[4],sm_contract = x[5])).toDF()
textDataDF.write.saveAsTable("tpcds.ship_mode", format="parquet", mode="overwrite")
