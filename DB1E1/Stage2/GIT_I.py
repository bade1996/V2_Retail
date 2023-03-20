from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf,year as yearf , month as monthf,when, substring as suf
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime, timedelta

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

config = os.path.dirname(os.path.realpath(__file__))
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("GIT")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set("spark.executor.cores","4")\
                .set("spark.executor.memory","5g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.sql.parquet.enableVectorizedReader","false")\
                .set("spark.local.dir", "/tmp/spark-temp")\
                    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)
print(datetime.now())
os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/GIT")
Acc = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Accounts")
Acc = Acc.select('ST_Branch_Code','act_code').drop_duplicates()
Acc = Acc.withColumnRenamed('ST_Branch_Code','Branch_Code').withColumnRenamed('act_code','Cust_Code')
sales = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales").where(col('YearMonth').isin([202202,202203,202204,202205]))
sales = sales.select('vouch_date','vouch_num','NEw_Vouch_Num','item_det_code','Tot_Qty','Stock_Trans','Deleted','Cust_Code')
sales = sales.filter(sales['Stock_Trans'] == 1)
sales = sales.filter(sales['Deleted'] == 0)
sales = sales.join(Acc, 'Cust_Code', 'left')
sales = sales.drop('Stock_Trans','Deleted','Cust_Code')
sales = sales.withColumn('vouch_num',concat_ws('-',suf(sales['vouch_num'],3,4),suf(sales['vouch_num'],7,2)))
sales = sales.withColumn('vouch_num',when(sales['NEw_Vouch_Num'].isNull(),sales['vouch_num']).otherwise(sales['NEw_Vouch_Num']))
sales = sales.drop('NEw_Vouch_Num')
sales = sales.groupby('vouch_date','vouch_num','item_det_code').agg({'Tot_Qty':'sum'})
sales = sales.withColumnRenamed('sum(Tot_Qty)','Tot_Qty')
sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
sales = sales.withColumn('Num_Item',concat_ws('-',sales['vouch_num'],sales['item_det_code']))
print(sales.count())
sales.cache()
Purchase = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase").where(col('YearMonth').isin([202202,202203,202204,202205]))
vouch_num = list(set(sales.select('Num_Item').toPandas()['Num_Item']))
Purchase = Purchase.select('vouch_date','Bill_No','item_det_code','Tot_Qty','Txn_Deleted_','Cust_Code')
Purchase = Purchase.filter(Purchase['Txn_Deleted_'] == 0)
Purchase = Purchase.join(Acc, 'Cust_Code', 'left')
Purchase = Purchase.drop('Cust_Code')
Purchase = Purchase.withColumn('Num_Item',concat_ws('-',Purchase['Bill_No'],Purchase['item_det_code']))
print(Purchase.count())
Purchase.cache()
Purchase = Purchase.filter(Purchase['Num_Item'].isin(vouch_num))
Purchase = Purchase.groupby('vouch_date','Bill_No','item_det_code','Num_Item').agg({'Tot_Qty':'sum'})
Purchase = Purchase.withColumnRenamed('sum(Tot_Qty)','Tot_Qty').withColumnRenamed('Bill_No','vouch_num')
Purchase = Purchase.withColumn('Tot_Qty',-1*Purchase['Tot_Qty'])
Purchase = Purchase.withColumn("vouch_date",Purchase['vouch_date'].cast(DateType()))
sales = sales.unionByName(Purchase)
sales = sales.withColumn('Key',concat_ws('-',sales['vouch_date'],sales['Num_Item']))
Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
Date_format_range = Date_format_range.filter(Date_format_range['Key'] == 202205)
SDate = sales.agg({"vouch_date": "min"}).collect()[0][0]
EDate = Date_format_range.agg({"ED": "max"}).collect()[0][0]
sales_distinct = sales.select('vouch_num','item_det_code','Num_Item').drop_duplicates()
sales_distinct = sales_distinct.withColumn('Key',lit('a'))
sales = sales.drop('item_det_code','vouch_num','vouch_date','Num_Item')     
Date_range = pd.DataFrame(pd.date_range(start = SDate,end =EDate, freq='D'),columns = ['Date'])
Date_range = spark.createDataFrame(Date_range)
Date_range = Date_range.withColumn("Date",Date_range['Date'].cast(DateType()))
Date_range = Date_range.withColumn('Key',lit('a'))
Date_range = Date_range.join(sales_distinct,'Key','left')
Date_range = Date_range.withColumn('Key',concat_ws('-',Date_range['Date'],Date_range['Num_Item']))
sales = Date_range.join(sales,'Key','left')
sales = sales.fillna(0, subset=['Tot_Qty'])                
my_window = Window.partitionBy('vouch_num','item_det_code','Num_Item').orderBy("Date")
sales = sales.withColumn('GIT', sumf('Tot_Qty').over(my_window).alias('GIT'))
sales = sales.drop('Tot_Qty','Key')
sales = sales.withColumn('YearMonth',yearf(sales['Date'])*100+date_format(sales['Date'],'MM'))
sales = sales.withColumn('YearMonth',sales['YearMonth'].cast(IntegerType()))
sales = sales.withColumn('YearMonth',sales['YearMonth'].cast(StringType()))
sales = sales.filter(sales['GIT'] != 0)
sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/GIT")
write_data_sql(sales,"GIT",owmode)
print(datetime.now())