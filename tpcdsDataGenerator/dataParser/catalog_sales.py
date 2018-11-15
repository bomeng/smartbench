from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/catalog_sales.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(cs_sold_date_sk = x[0],cs_sold_time_sk = x[1],cs_ship_date_sk = x[2],cs_bill_customer_sk = x[3],cs_bill_cdemo_sk = x[4],cs_bill_hdemo_sk = x[5],cs_bill_addr_sk = x[6],cs_ship_customer_sk = x[7],cs_ship_cdemo_sk = x[8],cs_ship_hdemo_sk = x[9],cs_ship_addr_sk = x[10],cs_call_center_sk = x[11],cs_catalog_page_sk = x[12],cs_ship_mode_sk = x[13],cs_warehouse_sk = x[14],cs_item_sk = x[15],cs_promo_sk = x[16],cs_order_number = x[17],cs_quantity = x[18],cs_wholesale_cost = x[19],cs_list_price = x[20],cs_sales_price = x[21],cs_ext_discount_amt = x[22],cs_ext_sales_price = x[23],cs_ext_wholesale_cost = x[24],cs_ext_list_price = x[25],cs_ext_tax = x[26],cs_coupon_amt = x[27],cs_ext_ship_cost = x[28],cs_net_paid = x[29],cs_net_paid_inc_tax = x[30],cs_net_paid_inc_ship = x[31],cs_net_paid_inc_ship_tax = x[32],cs_net_profit = x[33])).toDF()
textDataDF.write.saveAsTable("tpcds.catalog_sales", format="parquet", mode="overwrite")
