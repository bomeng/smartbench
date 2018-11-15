from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/web_returns.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(wr_returned_date_sk = x[0],wr_returned_time_sk = x[1],wr_item_sk = x[2],wr_refunded_customer_sk = x[3],wr_refunded_cdemo_sk = x[4],wr_refunded_hdemo_sk = x[5],wr_refunded_addr_sk = x[6],wr_returning_customer_sk = x[7],wr_returning_cdemo_sk = x[8],wr_returning_hdemo_sk = x[9],wr_returning_addr_sk = x[10],wr_web_page_sk = x[11],wr_reason_sk = x[12],wr_order_number = x[13],wr_return_quantity = x[14],wr_return_amt = x[15],wr_return_tax = x[16],wr_return_amt_inc_tax = x[17],wr_fee = x[18],wr_return_ship_cost = x[19],wr_refunded_cash = x[20],wr_reversed_charge = x[21],wr_account_credit = x[22],wr_net_loss = x[23])).toDF()
textDataDF.write.saveAsTable("tpcds.web_returns", format="parquet", mode="overwrite")
