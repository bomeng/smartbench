from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "date_dim.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(d_date_sk = x[0],d_date_id = x[1],d_date = x[2],d_month_seq = x[3],d_week_seq = x[4],d_quarter_seq = x[5],d_year = x[6],d_dow = x[7],d_moy = x[8],d_dom = x[9],d_qoy = x[10],d_fy_year = x[11],d_fy_quarter_seq = x[12],d_fy_week_seq = x[13],d_day_name = x[14],d_quarter_name = x[15],d_holiday = x[16],d_weekend = x[17],d_following_holiday = x[18],d_first_dom = x[19],d_last_dom = x[20],d_same_day_ly = x[21],d_same_day_lq = x[22],d_current_day = x[23],d_current_week = x[24],d_current_month = x[25],d_current_quarter = x[26],d_current_year = x[27])).toDF()
textDataDF.write.saveAsTable("tpcds.date_dim", format="parquet", mode="overwrite")
