from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/reason.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(r_reason_sk = x[0],r_reason_id = x[1],r_reason_desc = x[2])).toDF()
textDataDF.write.saveAsTable("tpcds.reason", format="parquet", mode="overwrite")
