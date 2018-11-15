from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "web_page.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(wp_web_page_sk=x[0],wp_web_page_id=x[1],wp_rec_start_date=x[2],wp_rec_end_date=x[3],wp_creation_date_sk=x[4],wp_access_date_sk=x[5],wp_autogen_flag=x[6],wp_customer_sk=x[7],wp_url=x[8],wp_type=x[9],wp_char_count=x[10],wp_link_count=x[11],wp_image_count=x[12],wp_max_ad_count=x[13])).toDF()
textDataDF.write.saveAsTable("tpcds.web_page", format="parquet", mode="overwrite")
