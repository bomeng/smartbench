from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "time_dim.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(t_time_sk = x[0],t_time_id = x[1],t_time = x[2],t_hour = x[3],t_minute = x[4],t_second = x[5],t_am_pm = x[6],t_shift = x[7],t_sub_shift = x[8],t_meal_time = x[9])).toDF()
textDataDF.write.saveAsTable("tpcds.time_dim", format="parquet", mode="overwrite")
