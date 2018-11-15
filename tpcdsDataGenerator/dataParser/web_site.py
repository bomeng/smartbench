from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile(os.environ["DATA_HDFS"] + "web_site.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(web_site_sk = x[0],web_site_id = x[1],web_rec_start_date = x[2],web_rec_end_date = x[3],web_name = x[4],web_open_date_sk = x[5],web_close_date_sk = x[6],web_class = x[7],web_manager = x[8],web_mkt_id = x[9],web_mkt_class = x[10],web_mkt_desc = x[11],web_market_manager = x[12],web_company_id = x[13],web_company_name = x[14],web_street_number = x[15],web_street_name = x[16],web_street_type = x[17],web_suite_number = x[18],web_city = x[19],web_county = x[20],web_state = x[21],web_zip = x[22],web_country = x[23],web_gmt_offset = x[24],web_tax_percentage = x[25])).toDF()
textDataDF.write.saveAsTable("tpcds.web_site", format="parquet", mode="overwrite")
