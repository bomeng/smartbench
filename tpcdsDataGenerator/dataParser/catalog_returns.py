from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/catalog_returns.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(cr_returned_date_sk = x[0],cr_returned_time_sk = x[1],cr_item_sk = x[2],cr_refunded_customer_sk = x[3],cr_refunded_cdemo_sk = x[4],cr_refunded_hdemo_sk = x[5],cr_refunded_addr_sk = x[6],cr_returning_customer_sk = x[7],cr_returning_cdemo_sk = x[8],cr_returning_hdemo_sk = x[9],cr_returning_addr_sk = x[10],cr_call_center_sk = x[11],cr_catalog_page_sk = x[12],cr_ship_mode_sk = x[13],cr_warehouse_sk = x[14],cr_reason_sk = x[15],cr_order_number = x[16],cr_return_quantity = x[17],cr_return_amount = x[18],cr_return_tax = x[19],cr_return_amt_inc_tax = x[20],cr_fee = x[21],cr_return_ship_cost = x[22],cr_refunded_cash = x[23],cr_reversed_charge = x[24],cr_store_credit = x[25],cr_net_loss = x[26])).toDF()
textDataDF.write.saveAsTable("tpcds.catalog_returns", format="parquet", mode="overwrite")
