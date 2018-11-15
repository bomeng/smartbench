from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "store_returns.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(sr_returned_date_sk = x[0],sr_return_time_sk = x[1],sr_item_sk = x[2],sr_customer_sk = x[3],sr_cdemo_sk = x[4],sr_hdemo_sk = x[5],sr_addr_sk = x[6],sr_store_sk = x[7],sr_reason_sk = x[8],sr_ticket_number = x[9],sr_return_quantity = x[10],sr_return_amt = x[11],sr_return_tax = x[12],sr_return_amt_inc_tax = x[13],sr_fee = x[14],sr_return_ship_cost = x[15],sr_refunded_cash = x[16],sr_reversed_charge = x[17],sr_store_credit = x[18],sr_net_loss = x[19])).toDF()
textDataDF.write.saveAsTable("tpcds.store_returns", format="parquet", mode="overwrite")
