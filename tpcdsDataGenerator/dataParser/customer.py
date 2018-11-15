from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "customer.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(c_customer_sk = x[0],c_customer_id = x[1],c_current_cdemo_sk = x[2],c_current_hdemo_sk = x[3],c_current_addr_sk = x[4],c_first_shipto_date_sk = x[5],c_first_sales_date_sk = x[6],c_salutation = x[7],c_first_name = x[8],c_last_name = x[9],c_preferred_cust_flag = x[10],c_birth_day = x[11],c_birth_month = x[12],c_birth_year = x[13],c_birth_country = x[14],c_login = x[15],c_email_address = x[16],c_last_review_date = x[17])).toDF()
textDataDF.write.saveAsTable("tpcds.customer", format="parquet", mode="overwrite")
