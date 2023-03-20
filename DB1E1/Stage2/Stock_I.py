from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf,year as yearf , month as monthf,when
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
conf = SparkConf().setMaster(smaster).setAppName("Stock")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .set("spark.executor.cores","4")\
                .set("spark.executor.memory","5g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.sql.parquet.enableVectorizedReader","false")
                # .set("spark.local.dir", "/tmp/spark-temp")\
                    # .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    # set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    # set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    # set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    # set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    # set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)

print(datetime.now())
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Stock") !=0):
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
                stock = stock.select('date_','Branch_Code','lot_code','net_qty','Godown_Code')
                stock.cache()
                stock = stock.withColumn('YearMonth',yearf(stock['date_'])*100+date_format(stock['date_'],'MM'))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(IntegerType()))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(StringType()))
                # print(stock.count())
                # exit()
                lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                stock = stock.join(lot, 'lot_code', how = 'left')
                stock = stock.withColumn('CLOSING_VALUE_ON_CP_Lot',stock['net_qty'] * stock['pur_rate_lot'])        
                stock = stock.withColumn('CLOSING_VALUE_ON_SP_Lot',stock['net_qty'] * stock['mrp_lot'])
                stock = stock.groupby('date_','Branch_Code','Item_Det_Code','YearMonth','Godown_Code').agg({'net_qty':'sum','CLOSING_VALUE_ON_CP_Lot':'sum','CLOSING_VALUE_ON_SP_Lot':'sum'})
                stock = stock.withColumnRenamed('sum(net_qty)','Stock_Quantity').withColumnRenamed('sum(CLOSING_VALUE_ON_CP_Lot)','CLOSING_VALUE_ON_CP_Lot').withColumnRenamed('sum(CLOSING_VALUE_ON_SP_Lot)','CLOSING_VALUE_ON_SP_Lot')
                stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
                stock = stock.drop('YearMonth')
    
                if(cdm_back == '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm_back)
                    stock_back_month.cache()
                    stock_back_month = stock_back_month.withColumn('YearMonth',yearf(stock_back_month['date_'])*100+date_format(stock_back_month['date_'],'MM'))
                    stock_back_month = stock_back_month.withColumn('YearMonth',stock_back_month['YearMonth'].cast(IntegerType()))
                    stock_back_month = stock_back_month.withColumn('YearMonth',stock_back_month['YearMonth'].cast(StringType()))
                    
                    lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                    lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                    stock_back_month = stock_back_month.join(lot, 'lot_code', how = 'left')
                    stock_back_month = stock_back_month.withColumn('CLOSING_VALUE_ON_CP_Lot',stock_back_month['net_qty'] * stock_back_month['pur_rate_lot'])        
                    stock_back_month = stock_back_month.withColumn('CLOSING_VALUE_ON_SP_Lot',stock_back_month['net_qty'] * stock_back_month['mrp_lot'])
                    stock_back_month = stock_back_month.groupby('date_','Branch_Code','Item_Det_Code','YearMonth','Godown_Code').agg({'net_qty':'sum','CLOSING_VALUE_ON_CP_Lot':'sum','CLOSING_VALUE_ON_SP_Lot':'sum'})
                    stock_back_month = stock_back_month.withColumnRenamed('sum(net_qty)','Stock_Quantity').withColumnRenamed('sum(CLOSING_VALUE_ON_CP_Lot)','CLOSING_VALUE_ON_CP_Lot').withColumnRenamed('sum(CLOSING_VALUE_ON_SP_Lot)','CLOSING_VALUE_ON_SP_Lot')
                    stock_back_month = stock_back_month.withColumn("date_",stock_back_month['date_'].cast(DateType()))
                    stock_back_month = stock_back_month.drop('YearMonth')
                    stock = stock.unionByName(stock_back_month)
                else:
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock/YearMonth="+cdm_back)
                    max_date = stock_back_month.agg({"date_": "max"})
                    stock_back_month = stock_back_month.filter(stock_back_month['date_'] == max_date.collect()[0][0])
                    stock_back_month = stock_back_month.withColumn('Stock_Quantity',stock_back_month['CQTY'])
                    stock_back_month = stock_back_month.drop('CQTY')
                    stock = stock.unionByName(stock_back_month)
    
            except:
                if(cdm_back == '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm_back)
                    stock = stock.select('date_','Branch_Code','lot_code','net_qty','Godown_Code')
                    stock_back_month.cache()
                    stock_back_month = stock_back_month.withColumn('YearMonth',yearf(stock_back_month['date_'])*100+date_format(stock_back_month['date_'],'MM'))
                    stock_back_month = stock_back_month.withColumn('YearMonth',stock_back_month['YearMonth'].cast(IntegerType()))
                    stock_back_month = stock_back_month.withColumn('YearMonth',stock_back_month['YearMonth'].cast(StringType()))
                    
                    lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                    lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                    stock_back_month = stock_back_month.join(lot, 'lot_code', how = 'left')
                    stock_back_month = stock_back_month.withColumn('CLOSING_VALUE_ON_CP_Lot',stock_back_month['net_qty'] * stock_back_month['pur_rate_lot'])        
                    stock_back_month = stock_back_month.withColumn('CLOSING_VALUE_ON_SP_Lot',stock_back_month['net_qty'] * stock_back_month['mrp_lot'])
                    stock_back_month = stock_back_month.groupby('date_','Branch_Code','Item_Det_Code','YearMonth','Godown_Code').agg({'net_qty':'sum','CLOSING_VALUE_ON_CP_Lot':'sum','CLOSING_VALUE_ON_SP_Lot':'sum'})
                    stock_back_month = stock_back_month.withColumnRenamed('sum(net_qty)','stock_back_month_Quantity').withColumnRenamed('sum(CLOSING_VALUE_ON_CP_Lot)','CLOSING_VALUE_ON_CP_Lot').withColumnRenamed('sum(CLOSING_VALUE_ON_SP_Lot)','CLOSING_VALUE_ON_SP_Lot')
                    stock_back_month = stock_back_month.withColumn("date_",stock_back_month['date_'].cast(DateType()))
                    stock = stock_back_month.drop('YearMonth')
                else:
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock/YearMonth="+cdm_back)
                    max_date = stock_back_month.agg({"date_": "max"})
                    stock_back_month = stock_back_month.filter(stock_back_month['date_'] == max_date.collect()[0][0])
                    stock_back_month = stock_back_month.withColumn('Stock_Quantity',stock_back_month['CQTY'])
                    stock = stock_back_month.drop('CQTY')
                    
            if(stock.count() > 0):
                stock = stock.orderBy('date_','Branch_Code','Item_Det_Code','Godown_Code')
                stock.cache()
                print(stock.count())
                stock_distinct = stock.select('Branch_Code','Item_Det_Code','Godown_Code').drop_duplicates()
                stock_distinct = stock_distinct.withColumn('Key',lit('a'))
                
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm)
                SDate = stock.agg({"date_": "min"}).collect()[0][0]
                EDate = Date_format_range.agg({"ED": "max"}).collect()[0][0]
                
                Date_range = pd.DataFrame(pd.date_range(start = SDate,end =EDate, freq='D'),columns = ['date_'])
                Date_range = spark.createDataFrame(Date_range)
                Date_range = Date_range.withColumn("date_",Date_range['date_'].cast(DateType()))
                Date_range = Date_range.withColumn('Key',lit('a'))
                Date_range = Date_range.join(stock_distinct,'Key','left')
                Date_range = Date_range.withColumn('Key' , concat_ws('_',Date_range['date_'],Date_range['Branch_Code'],Date_range['Item_Det_Code'],Date_range['Godown_Code']))
                
                stock = stock.withColumn('Key' , concat_ws('_',stock['date_'],stock['Branch_Code'],stock['Item_Det_Code'],stock['Godown_Code']))
                stock = stock.drop('date_','Branch_Code','Item_Det_Code','Godown_Code')
                
                stock = Date_range.join(stock,'Key', how = 'left')
                stock = stock.withColumn('YearMonth',yearf(stock['date_'])*100+date_format(stock['date_'],'MM'))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(IntegerType()))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(StringType()))
                stock = stock.fillna(0, subset=['Stock_Quantity'])
                stock = stock.fillna(0, subset=['CLOSING_VALUE_ON_CP_Lot'])
                stock = stock.fillna(0, subset=['CLOSING_VALUE_ON_SP_Lot'])
                stock.cache()
                print(stock.count())
                
                my_window = Window.partitionBy('Branch_Code','Item_Det_Code','Godown_Code').orderBy("date_","YearMonth")
                stock = stock.withColumn('CQTY', sumf('Stock_Quantity').over(my_window).alias('CQTY'))
                stock = stock.withColumn('CLOSING_VALUE_ON_CP_Lot', sumf('CLOSING_VALUE_ON_CP_Lot').over(my_window).alias('CLOSING_VALUE_ON_CP_Lot'))
                stock = stock.withColumn('CLOSING_VALUE_ON_SP_Lot', sumf('CLOSING_VALUE_ON_SP_Lot').over(my_window).alias('CLOSING_VALUE_ON_SP_Lot'))
                stock = stock.drop('Key')
                stock = stock.filter(stock['YearMonth'] == cdm)
                stock = stock.filter(stock['CQTY'] != 0)
                stock.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Stock")
                print("stock Done")
            
            if((int(datetime.today().month)+1 == month) & (int(datetime.today().year) == Start_Year)):
                print(datetime.now())            
                stock = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock")
                stock = stock.filter(stock['CQTY'] != 0)
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] <= cdm)
                Date_format_range = Date_format_range.withColumn('date_',when(Date_format_range['Key'] == cdm,datetime.now().date() - timedelta(days = 1)).otherwise(Date_format_range['ED']))
                Date_format_range = Date_format_range.select('date_')
                stock = Date_format_range.join(stock,'date_','left')
                print(datetime.now())
                write_data_sql(stock,"StockI",owmode)
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockI','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(stock.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
                log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
                write_data_sql(log_df,"Logs",mode="append")
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
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
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
                print("check")
                stock = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Stock/YearMonth="+cdm)
                stock = stock.select('date_','Branch_Code','lot_code','net_qty','Godown_Code')
                stock.cache()
                stock = stock.withColumn('YearMonth',yearf(stock['date_'])*100+date_format(stock['date_'],'MM'))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(IntegerType()))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(StringType()))
                
                lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
                lot = lot.select('lot_code','pur_rate_lot','mrp_lot','Item_Det_Code')
                stock = stock.join(lot, 'lot_code', how = 'left')
                stock = stock.withColumn('CLOSING_VALUE_ON_CP_Lot',stock['net_qty'] * stock['pur_rate_lot'])        
                stock = stock.withColumn('CLOSING_VALUE_ON_SP_Lot',stock['net_qty'] * stock['mrp_lot'])
                stock = stock.groupby('date_','Branch_Code','Item_Det_Code','YearMonth','Godown_Code').agg({'net_qty':'sum','CLOSING_VALUE_ON_CP_Lot':'sum','CLOSING_VALUE_ON_SP_Lot':'sum'})
                stock = stock.withColumnRenamed('sum(net_qty)','Stock_Quantity').withColumnRenamed('sum(CLOSING_VALUE_ON_CP_Lot)','CLOSING_VALUE_ON_CP_Lot').withColumnRenamed('sum(CLOSING_VALUE_ON_SP_Lot)','CLOSING_VALUE_ON_SP_Lot')
                stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
                stock = stock.drop('YearMonth')
                if(cdm_back != '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock/YearMonth="+cdm_back)
                    max_date = stock_back_month.agg({"date_": "max"})
                    stock_back_month = stock_back_month.filter(stock_back_month['date_'] == max_date.collect()[0][0])
                    stock_back_month = stock_back_month.withColumn('Stock_Quantity',stock_back_month['CQTY'])
                    stock_back_month = stock_back_month.drop('CQTY')
                    stock = stock.unionByName(stock_back_month)
            except:
                if(cdm_back != '201803'):
                    stock_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock/YearMonth="+cdm_back)
                    max_date = stock_back_month.agg({"date_": "max"})
                    stock_back_month = stock_back_month.filter(stock_back_month['date_'] == max_date.collect()[0][0])
                    stock_back_month = stock_back_month.withColumn('Stock_Quantity',stock_back_month['CQTY'])
                    stock = stock_back_month.drop('CQTY')
                    
            if(stock.count() > 0):
                stock = stock.orderBy('date_','Branch_Code','Item_Det_Code','Godown_Code')
                stock.cache()
                print(stock.count())
                
                stock_distinct = stock.select('Branch_Code','Item_Det_Code','Godown_Code').drop_duplicates()
                stock_distinct = stock_distinct.withColumn('Key',lit('a'))
                
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm)
                SDate = stock.agg({"date_": "min"}).collect()[0][0]
                EDate = Date_format_range.agg({"ED": "max"}).collect()[0][0]
                
                Date_range = pd.DataFrame(pd.date_range(start = SDate,end =EDate, freq='D'),columns = ['date_'])
                Date_range = spark.createDataFrame(Date_range)
                Date_range = Date_range.withColumn("date_",Date_range['date_'].cast(DateType()))
                Date_range = Date_range.withColumn('Key',lit('a'))
                Date_range = Date_range.join(stock_distinct,'Key','left')
                Date_range = Date_range.withColumn('Key' , concat_ws('_',Date_range['date_'],Date_range['Branch_Code'],Date_range['Item_Det_Code'],Date_range['Godown_Code']))
                
                stock = stock.withColumn('Key' , concat_ws('_',stock['date_'],stock['Branch_Code'],stock['Item_Det_Code'],stock['Godown_Code']))
                stock = stock.drop('date_','Branch_Code','Item_Det_Code','Godown_Code')
                
                stock = Date_range.join(stock,'Key', how = 'left')
                stock = stock.withColumn('YearMonth',yearf(stock['date_'])*100+date_format(stock['date_'],'MM'))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(IntegerType()))
                stock = stock.withColumn('YearMonth',stock['YearMonth'].cast(StringType()))
                stock = stock.fillna(0, subset=['Stock_Quantity'])
                stock = stock.fillna(0, subset=['CLOSING_VALUE_ON_CP_Lot'])
                stock = stock.fillna(0, subset=['CLOSING_VALUE_ON_SP_Lot'])
                stock.cache()
                print(stock.count())
                
                my_window = Window.partitionBy('Branch_Code','Item_Det_Code','Godown_Code').orderBy("date_","YearMonth")
                stock = stock.withColumn('CQTY', sumf('Stock_Quantity').over(my_window).alias('CQTY'))
                stock = stock.withColumn('CLOSING_VALUE_ON_CP_Lot', sumf('CLOSING_VALUE_ON_CP_Lot').over(my_window).alias('CLOSING_VALUE_ON_CP_Lot'))
                stock = stock.withColumn('CLOSING_VALUE_ON_SP_Lot', sumf('CLOSING_VALUE_ON_SP_Lot').over(my_window).alias('CLOSING_VALUE_ON_SP_Lot'))
                stock = stock.drop('Key')
                stock = stock.filter(stock['YearMonth'] == cdm)
                stock = stock.filter(stock['CQTY'] != 0)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Stock/YearMonth="+cdm)
                stock.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Stock")
    print(datetime.now())            
    stock = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock")
    stock = stock.filter(stock['CQTY'] != 0)
    Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
    Date_format_range = Date_format_range.filter(Date_format_range['Key'] <= cdm)
    Date_format_range = Date_format_range.withColumn('date_',when(Date_format_range['Key'] == cdm,datetime.now().date() - timedelta(days = 1)).otherwise(Date_format_range['ED']))
    Date_format_range = Date_format_range.select('date_')
    stock = Date_format_range.join(stock,'date_','left')
    print(stock.count())
    stock.cache()
    print(datetime.now())
    write_data_sql(stock,"StockI",owmode)
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockI','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(stock.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    print(datetime.now())
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")