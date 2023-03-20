#Snapshot of Stock Query script
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,col,month,year,lit,concat,max as max_,min as min_
import re,keyring,os,datetime
import time,sys
from pyspark.sql.types import *
from pyspark.sql.concatenate import *
import pandas as pd
from numpy.core._multiarray_umath import empty

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')

try:
    conf = SparkConf().setMaster('local[8]').setAppName("Market99")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Market99").getOrCreate()
    sqlctx = SQLContext(sc)
    
    Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=LOGICDBS99;user=sa;password=sa@123"
    Postgresurl = "jdbc:postgresql://localhost:5432/kockpit"
    
    
    Postgresprop= {
        "user":"postgres",
        "password":"sa@123",
        "driver": "org.postgresql.Driver" 
    }
    
    def last_day_of_month(date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)  
    
    
    
    
    
    '''table1'''  
    stock="(SELECT *  FROM market99.Stock) AS doc"
    stock=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=stock,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    date = stock.select("Vouch_Date").filter(stock["Vouch_Date"]!= '')
    #date = date.collect()
    Calendar_Startdate = date.agg({"Vouch_Date": "min"}).collect()[0][0].split()[0]
    Calendar_Enddate = date.agg({"Vouch_Date": "max"}).collect()[0][0].split()[0]

    Calendar_Startdate = datetime.datetime.strptime(Calendar_Startdate, '%Y-%m-%d').date()
    Calendar_Enddate = datetime.datetime.strptime(Calendar_Enddate, '%Y-%m-%d').date()
    
    data =[]
    for single_date in daterange(Calendar_Startdate, Calendar_Enddate):
        data.append({'Link_date':single_date})

    schema = StructType([
        StructField("Link_date", DateType(),True)
    ])
    records=spark.createDataFrame(data,schema)#recordes every date btw the startDate and EndDate

    records=records.select(last_day(records.Link_date).alias('Link_date')).distinct().sort('Link_date')#Contains Last Date of the month for every date btw the range
    
    records=records.withColumn("Link_date", \
                  when(records["Link_date"] == last_day_of_month(Calendar_Enddate), Calendar_Enddate).otherwise(records["Link_date"]))#reverts the changes to the last date which was previouslt showing last month date of last Date
    records.show()
    records.cache()
    stock.cache()
    cond= [(stock["Vouch_Date"]<=records.Link_date)]
    ageing = stock.join(records,cond,"inner")
    ageing.cache()
    ageing.write.jdbc(url=Postgresurl, table="market99"+".StockSnapshot", mode="overwrite", properties=Postgresprop)
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
   