from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "inventory.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(inv_date_sk = x[0],inv_item_sk = x[1],inv_warehouse_sk = x[2],inv_quantity_on_hand = x[3])).toDF()
textDataDF.write.saveAsTable("tpcds.inventory", format="parquet", mode="overwrite")
