from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf,year as yearf , month as monthf,when,last_day
from pyspark.sql.types import *
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime, timedelta
import time,sys

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
Market = config[config.rfind("/")+1:]
Etn = Market[Market.rfind("E"):]   
DB = Market[:Market.rfind("E")]
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("StockAgeing")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set("spark.executor.cores","3")\
                .set("spark.executor.memory","4g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.sql.parquet.enableVectorizedReader","false")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)
print(datetime.now())
Start_Year = 2018
try:
    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/prastest")
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/prastest") !=0):
        End_Year = (datetime.today().year)
        month = 4
        while(Start_Year <= End_Year):
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
            if(month <= 9):
                cdm = str(Start_Year) +'0'+str(month)
            else:
                cdm = str(Start_Year) + str(month)
            if(month == 1):
                cdm_back = str(int(cdm) - 89)
            else:
                cdm_back = str(int(cdm) - 1)
            if(month == 12):
                month = 0
                Start_Year = Start_Year + 1
            month = month + 1
            print(cdm)
            print(cdm_back)
            try:
                stock = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm)
                lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                stock = stock.join(lot, 'lot_code', how = 'left')
                stock = stock.select('Branch_Code','Item_Det_Code','Godown_Code','lot_code','date_','net_qty')
                stock = stock.withColumn('Link_Date',last_day('date_'))
                stock.show(10)
                if(cdm_back == '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm_back)
                    lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                    lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                    stock_back_month = stock_back_month.join(lot, 'lot_code', how = 'left')
                    stock_back_month = stock_back_month.select('Branch_Code','Item_Det_Code','Godown_Code','lot_code','date_','net_qty')                
                    stock_back_month = stock_back_month.withColumn('Link_Date',last_day('date_'))
                    stock = stock.unionByName(stock_back_month)
                    stock.show(10)
                else:
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/prastest/YearMonth="+cdm_back)
                    stock_back_month = stock_back_month.drop('NOD','Interval','Bucket_Sort')
                    stock = stock.unionByName(stock_back_month)
                    stock.show(10)
            except Exception as e:
                print(e)
                if(cdm_back == '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm_back)
                    lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                    lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                    stock_back_month = stock_back_month.join(lot, 'lot_code', how = 'left')
                    stock_back_month = stock_back_month.withColumn('Link_Date',last_day('date_'))
                    stock = stock_back_month.select('Branch_Code','Item_Det_Code','Godown_Code','lot_code','date_','net_qty')
                else:
                    stock = sqlctx.read.parquet(hdfspath+"/Market/Stage2/prastest/YearMonth="+cdm_back)
                    stock = stock.drop('NOD','Interval','Bucket_Sort')
                    
            if(stock.count() > 0):
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm)
                EDate = Date_format_range.agg({"ED": "max"}).collect()[0][0]
                print(EDate)
                stock = stock.groupby('Branch_Code','Item_Det_Code','Godown_Code','lot_code').agg({'date_':'min','net_qty':'sum'})
                stock = stock.withColumnRenamed('min(date_)','date_').withColumnRenamed('sum(net_qty)','net_qty')
                stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
                stock = stock.withColumn('Link_Date',lit(EDate))
                stock = stock.withColumn("NOD",datediff(stock['Link_Date'],stock['date_']))
                stock = stock.withColumn('Interval',when(stock['NOD'] < 0,'Not Due').
                                                    when((stock['NOD'] >= 0) & (stock['NOD'] <= 15),'0-15').
                                                    when((stock['NOD'] >= 16) & (stock['NOD'] <= 30),'16-30').
                                                    when((stock['NOD'] >= 31) & (stock['NOD'] <= 45),'31-45').
                                                    when((stock['NOD'] >= 46) & (stock['NOD'] <= 60),'46-60').
                                                    when((stock['NOD'] >= 61) & (stock['NOD'] <= 75),'61-75').
                                                    when((stock['NOD'] >= 76) & (stock['NOD'] <= 90),'76-90').
                                                    when((stock['NOD'] >= 91) & (stock['NOD'] <= 120),'91-120').
                                                    when((stock['NOD'] >= 121) & (stock['NOD'] <= 150),'121-150').
                                                    when((stock['NOD'] >= 151) & (stock['NOD'] <= 180),'151-180').otherwise('180+'))
                stock = stock.withColumn('Bucket_Sort',when(stock['Interval'] == 'Not Due',1).
                                                    when(stock['Interval'] == '0-15',2).
                                                    when(stock['Interval'] == '16-30',3).
                                                    when(stock['Interval'] == '31-45',4).
                                                    when(stock['Interval'] == '46-60',5).
                                                    when(stock['Interval'] == '61-75',6).
                                                    when(stock['Interval'] == '76-90',7).
                                                    when(stock['Interval'] == '91-120',8).
                                                    when(stock['Interval'] == '121-150',9).
                                                    when(stock['Interval'] == '151-180',10).otherwise(11))
                stock = stock.filter(stock['net_qty'] != 0)
                stock = stock.withColumn('YearMonth',yearf(stock['Link_Date'])*100+date_format(stock['Link_Date'],'MM'))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(IntegerType()))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(StringType()))
                stock.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/prastest")
                # if(cdm == '201804'):
                    # write_data_sql(stock,"prastestI",owmode)
                # else:
                    # write_data_sql(stock,"prastestI","append")
                print("stock Done")
            if((int(datetime.today().month)+1 == month) & (int(datetime.today().year) == Start_Year)):
                stock = sqlctx.read.parquet(hdfspath+"/Market/Stage2/prastest")
                stock = stock.filter(stock['net_qty'] != 0)
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] <= cdm)
                Date_format_range = Date_format_range.withColumn('date_',when(Date_format_range['Key'] == cdm,datetime.now().date() - timedelta(days = 1)).otherwise(Date_format_range['ED']))
                Date_format_range = Date_format_range.select('date_')
                stock = Date_format_range.join(stock,'date_','left')
                write_data_sql(stock,"prastestI",owmode)
                print(datetime.now())
                exit()
    else:
        for i in range(2,-1,-1):
            month = datetime.today().month - i
            if(month <= 3):
                Start_Year = (datetime.today().year) - 1
                End_Year = (datetime.today().year)
            else:
                Start_Year = (datetime.today().year) 
                End_Year = int(datetime.today().year) + 1
            FY = str(Start_Year)+str(End_Year)
            cdm = str(datetime.today().year)
            if(month <= 9):
                cdm = cdm + '0' + str(month)
            else:
                cdm = cdm + str(month)
            if(month == 1):
                cdm_back = str(int(cdm) - 89)
            else:
                cdm_back = str(int(cdm) - 1)
            print(cdm)
            print(cdm_back)            
            try:
                stock = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm)
                lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                stock = stock.join(lot, 'lot_code', how = 'left')
                stock = stock.select('Branch_Code','Item_Det_Code','Godown_Code','lot_code','date_','net_qty')
                stock = stock.withColumn('Link_Date',last_day('date_'))
                stock.show(10)
                if(cdm_back == '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm_back)
                    lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                    lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                    stock_back_month = stock_back_month.join(lot, 'lot_code', how = 'left')
                    stock_back_month = stock_back_month.select('Branch_Code','Item_Det_Code','Godown_Code','lot_code','date_','net_qty')                
                    stock_back_month = stock_back_month.withColumn('Link_Date',last_day('date_'))
                    stock = stock.unionByName(stock_back_month)
                    stock.show(10)
                else:
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/prastest/YearMonth="+cdm_back)
                    stock_back_month = stock_back_month.drop('NOD','Interval','Bucket_Sort')
                    stock = stock.unionByName(stock_back_month)
                    stock.show(10)
            except Exception as e:
                print(e)
                if(cdm_back == '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm_back)
                    lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                    lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                    stock_back_month = stock_back_month.join(lot, 'lot_code', how = 'left')
                    stock_back_month = stock_back_month.withColumn('Link_Date',last_day('date_'))
                    stock = stock_back_month.select('Branch_Code','Item_Det_Code','Godown_Code','lot_code','date_','net_qty')
                else:
                    stock = sqlctx.read.parquet(hdfspath+"/Market/Stage2/prastest/YearMonth="+cdm_back)
                    stock = stock.drop('NOD','Interval','Bucket_Sort')
                    
            if(stock.count() > 0):
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm)
                EDate = Date_format_range.agg({"ED": "max"}).collect()[0][0]
                print(EDate)
                stock = stock.groupby('Branch_Code','Item_Det_Code','Godown_Code','lot_code').agg({'date_':'min','net_qty':'sum'})
                stock = stock.withColumnRenamed('min(date_)','date_').withColumnRenamed('sum(net_qty)','net_qty')
                stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
                stock = stock.withColumn('Link_Date',lit(EDate))
                stock = stock.withColumn("NOD",datediff(stock['Link_Date'],stock['date_']))
                stock = stock.withColumn('Interval',when(stock['NOD'] < 0,'Not Due').
                                                    when((stock['NOD'] >= 0) & (stock['NOD'] <= 15),'0-15').
                                                    when((stock['NOD'] >= 16) & (stock['NOD'] <= 30),'16-30').
                                                    when((stock['NOD'] >= 31) & (stock['NOD'] <= 45),'31-45').
                                                    when((stock['NOD'] >= 46) & (stock['NOD'] <= 60),'46-60').
                                                    when((stock['NOD'] >= 61) & (stock['NOD'] <= 75),'61-75').
                                                    when((stock['NOD'] >= 76) & (stock['NOD'] <= 90),'76-90').
                                                    when((stock['NOD'] >= 91) & (stock['NOD'] <= 120),'91-120').
                                                    when((stock['NOD'] >= 121) & (stock['NOD'] <= 150),'121-150').
                                                    when((stock['NOD'] >= 151) & (stock['NOD'] <= 180),'151-180').otherwise('180+'))
                stock = stock.withColumn('Bucket_Sort',when(stock['Interval'] == 'Not Due',1).
                                                    when(stock['Interval'] == '0-15',2).
                                                    when(stock['Interval'] == '16-30',3).
                                                    when(stock['Interval'] == '31-45',4).
                                                    when(stock['Interval'] == '46-60',5).
                                                    when(stock['Interval'] == '61-75',6).
                                                    when(stock['Interval'] == '76-90',7).
                                                    when(stock['Interval'] == '91-120',8).
                                                    when(stock['Interval'] == '121-150',9).
                                                    when(stock['Interval'] == '151-180',10).otherwise(11))
                stock = stock.filter(stock['net_qty'] != 0)
                stock = stock.withColumn('YearMonth',yearf(stock['Link_Date'])*100+date_format(stock['Link_Date'],'MM'))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(IntegerType()))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(StringType()))
                stock.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/prastest")
                # if(cdm == '201804'):
                    # write_data_sql(stock,"prastestI",owmode)
                # else:
                    # write_data_sql(stock,"prastestI","append")
                print("stock Done")
            if((int(datetime.today().month)+1 == month) & (int(datetime.today().year) == Start_Year)):
                stock = sqlctx.read.parquet(hdfspath+"/Market/Stage2/prastest")
                stock = stock.filter(stock['net_qty'] != 0)
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] <= cdm)
                Date_format_range = Date_format_range.withColumn('date_',when(Date_format_range['Key'] == cdm,datetime.now().date() - timedelta(days = 1)).otherwise(Date_format_range['ED']))
                Date_format_range = Date_format_range.select('date_')
                stock = Date_format_range.join(stock,'date_','left')
                write_data_sql(stock,"prastestI",owmode)
                print(datetime.now())
                exit()   
    print(datetime.now())
except Exception as ex:
    print(ex)
    