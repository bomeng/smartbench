from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/household_demographics.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(hd_demo_sk = x[0],hd_income_band_sk = x[1],hd_buy_potential = x[2],hd_dep_count = x[3],hd_vehicle_count = x[4])).toDF()
textDataDF.write.saveAsTable("tpcds.household_demographics", format="parquet", mode="overwrite")
