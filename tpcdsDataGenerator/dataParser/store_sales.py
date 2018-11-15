from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/store_sales.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(ss_sold_date_sk = x[0],ss_sold_time_sk = x[1],ss_item_sk = x[2],ss_customer_sk = x[3],ss_cdemo_sk = x[4],ss_hdemo_sk = x[5],ss_addr_sk = x[6],ss_store_sk = x[7],ss_promo_sk = x[8],ss_ticket_number = x[9],ss_quantity = x[10],ss_wholesale_cost = x[11],ss_list_price = x[12],ss_sales_price = x[13],ss_ext_discount_amt = x[14],ss_ext_sales_price = x[15],ss_ext_wholesale_cost = x[16],ss_ext_list_price = x[17],ss_ext_tax = x[18],ss_coupon_amt = x[19],ss_net_paid = x[20],ss_net_paid_inc_tax = x[21],ss_net_profit = x[22])).toDF()
textDataDF.write.saveAsTable("tpcds.store_sales", format="parquet", mode="overwrite")
