from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "customer_demographics.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(cd_demo_sk = x[0],cd_gender = x[1],cd_marital_status = x[2],cd_education_status = x[3],cd_purchase_estimate = x[4],cd_credit_rating = x[5],cd_dep_count = x[6],cd_dep_employed_count = x[7],cd_dep_college_count = x[8])).toDF()
textDataDF.write.saveAsTable("tpcds.customer_demographics", format="parquet", mode="overwrite")
