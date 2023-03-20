from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql import functions as F

conf = SparkConf().setMaster("spark://103.248.60.14:7077").setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()


def generate_dates(spark,range_list,interval=60*60*24,dt_col="date_time_ref"):
    start,stop = range_list
    temp_df = spark.createDataFrame([(start, stop)], ("start", "stop"))
    temp_df = temp_df.select([F.col(c).cast("timestamp") for c in ("start", "stop")])
    temp_df = temp_df.withColumn("stop",F.date_add("stop",1).cast("timestamp"))
    temp_df = temp_df.select([F.col(c).cast("long") for c in ("start", "stop")])
    start, stop = temp_df.first()
    return spark.range(start,stop,interval).select(F.col("id").cast("timestamp").alias(dt_col))

date_range = ["2018-01-20 00:00:00","2018-01-23 00:00:00"]
generate_dates(spark,date_range).show()