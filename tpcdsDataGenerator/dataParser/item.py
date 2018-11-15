from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = HiveContext(sc)

textDataRDD = sc.textFile("hdfs://master:9000/hibench/data/tables/item.dat")
textDataDF = textDataRDD.map(lambda x: x.split("|")).map(lambda x: Row(i_item_sk = x[0],i_item_id = x[1],i_rec_start_date = x[2],i_rec_end_date = x[3],i_item_desc = x[4],i_current_price = x[5],i_wholesale_cost = x[6],i_brand_id = x[7],i_brand = x[8],i_class_id = x[9],i_class = x[10],i_category_id = x[11],i_category = x[12],i_manufact_id = x[13],i_manufact = x[14],i_size = x[15],i_formulation = x[16],i_color = x[17],i_units = x[18],i_container = x[19],i_manager_id = x[20],i_product_name = x[21])).toDF()
textDataDF.write.saveAsTable("tpcds.item", format="parquet", mode="overwrite")
