from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf 
from pyspark.sql.types import *
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
import re,os,datetime
import time,sys
from datetime import datetime
import pyodbc
import csv

#For reading data from the SQL server
def read_data_sql(table_string):
    database = "LOGICDBS99"
    user = "sa"
    password  = "Market!@999"
    SQLurl = "jdbc:sqlserver://103.234.187.190:2499;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

def write_data_sql(df,name,mode):
    database = "KOCKPIT"
    user = "sa"
    password  = "Market!@999"
    SQLurl = "jdbc:sqlserver://103.234.187.190:2499;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df.write.jdbc(url = SQLurl , table = name, properties = SQLprop, mode = mode)

stime = time.time()
Datelog = datetime.now().strftime('%Y-%m-%d')
start_time = datetime.now()#.strftime('%H:%M:%S')

stime = start_time.strftime('%H:%M:%S')

schema_log = StructType([
            StructField('Date',StringType(),True),
            StructField('Start_Time',StringType(),True),
            StructField('End_Time', StringType(),True),
            StructField('Run_Time',StringType(),True),
            StructField('File_Name',StringType(),True),
            StructField('DB',StringType(),True),
            StructField('EN', StringType(),True),
            StructField('Status',StringType(),True),
            StructField('ErrorLineNo',StringType(),True),
            StructField('Operation',StringType(),True),
            StructField('Rows',StringType(),True),
            StructField('BeforeETLRows',StringType(),True),
            StructField('AfterETLRows',StringType(),True)]
            )

    

config = os.path.dirname(os.path.realpath(__file__))
# print(config)
# exit()
Market = config[config.rfind("/")+1:]

Etn = Market[Market.rfind("E"):]
# print(Etn)
# exit()
DB = Market[:Market.rfind("E")]
# print(DB)
# exit()
path = config = config[0:config.rfind("DB")]
print(config)
exit()
config = pd.read_csv(config+"/Config/conf.csv")
print(config.head(100))
exit()
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("Stage1_transaction")\
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .set("spark.executor.cores","4")\
                .set("spark.executor.memory","5g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.local.dir", "/tmp/spark-temp")\
                .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()
sqlctx = SQLContext(sc)
print(datetime.now())


# po= sqlctx.read.parquet(hdfspath+"/Market/Stage2/Sales")
# l= sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
#
# #i= sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
# po=po.join(l,"lot_code",how ='left')
# po=po.filter(po['Branch_Code']==5010).filter(po['item_det_code']==68845)\
   # .filter(po['vouch_date'].between('2020-02-01','2020-02-28') )
# # po=po.groupby('item_det_code').agg(sum('Tot_Qty').alias('qty'))
# po.show()
l= sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
l=l.filter(l['lot_code']==4231956)
l.show()
# po.filter(po['Link_Code']== 0).filter(po['Vouch_Date'] =='2021-05-28').show()
# exit()
# SCH.show()
# SCGM = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Scheme_Campaign_Group_Mst")
# PR= sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLSCH")
# PR=PR.filter(PR['vouch_date'].between('2018-04-01','2021-12-31') ).filter(PR.Deleted==0)
# PR = PR.join(SCGM,PR.Sch_Det_Code==SCGM.Code,how='left' )
# PR = PR.join(SCH,PR.Scheme_Campaign_Code==SCH.Code,how='left' )
# PR.show()
# print(PR.count())
#

