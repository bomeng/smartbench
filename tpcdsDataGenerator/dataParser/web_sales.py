from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "web_sales.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(ws_sold_date_sk = x[0],ws_sold_time_sk = x[1],ws_ship_date_sk = x[2],ws_item_sk = x[3],ws_bill_customer_sk = x[4],ws_bill_cdemo_sk = x[5],ws_bill_hdemo_sk = x[6],ws_bill_addr_sk = x[7],ws_ship_customer_sk = x[8],ws_ship_cdemo_sk = x[9],ws_ship_hdemo_sk = x[10],ws_ship_addr_sk = x[11],ws_web_page_sk = x[12],ws_web_site_sk = x[13],ws_ship_mode_sk = x[14],ws_warehouse_sk = x[15],ws_promo_sk = x[16],ws_order_number = x[17],ws_quantity = x[18],ws_wholesale_cost = x[19],ws_list_price = x[20],ws_sales_price = x[21],ws_ext_discount_amt = x[22],ws_ext_sales_price = x[23],ws_ext_wholesale_cost = x[24],ws_ext_list_price = x[25],ws_ext_tax = x[26],ws_coupon_amt = x[27],ws_ext_ship_cost = x[28],ws_net_paid = x[29],ws_net_paid_inc_tax = x[30],ws_net_paid_inc_ship = x[31],ws_net_paid_inc_ship_tax = x[32],ws_net_profit = x[33])).toDF()
textDataDF.write.saveAsTable("tpcds.web_sales", format="parquet", mode="overwrite")
