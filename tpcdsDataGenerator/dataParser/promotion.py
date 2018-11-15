from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/promotion.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(p_promo_sk=x[0],p_promo_id=x[1],p_start_date_sk=x[2],p_end_date_sk=x[3],p_item_sk=x[4],p_cost=x[5],p_response_target=x[6],p_promo_name=x[7],p_channel_dmail=x[8],p_channel_email=x[9],p_channel_catalog=x[10],p_channel_tv=x[11],p_channel_radio=x[12],p_channel_press=x[13],p_channel_event=x[14],p_channel_demo=x[15],p_channel_details=x[16],p_purpose=x[17],p_discount_active=x[18])).toDF()
textDataDF.write.saveAsTable("tpcds.promotion", format="parquet", mode="overwrite")
