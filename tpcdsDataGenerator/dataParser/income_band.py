from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/income_band.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(ib_income_band_sk = x[0],ib_lower_bound = x[1],ib_upper_bound = x[2])).toDF()
textDataDF.write.saveAsTable("tpcds.income_band", format="parquet", mode="overwrite")
