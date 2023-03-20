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
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\POWERBI;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

def write_data_sql(df,name,mode):
    database = "KOCKPIT"
    user = "sa"
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\POWERBI;databaseName="+database
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
conf = SparkConf().setMaster(smaster).setAppName("PendingChallan")\
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
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)
print(datetime.now())

Start_Year = 2018
try:
    #os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Pending_Challan")
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Pending_Challan") !=0):
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
            print(datetime.now())
            try:
                Acc = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Accounts")
                Acc = Acc.select('ST_Branch_Code','act_code').drop_duplicates()
                Acc = Acc.withColumnRenamed('ST_Branch_Code','Branch_Code').withColumnRenamed('act_code','Cust_Code')
                Challan = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Challan/YearMonth="+cdm)
                Challan = Challan.select('vouch_date','code','item_det_code','Org_Qty','Cust_Code')
                Challan = Challan.join(Acc,'Cust_Code','left')
                Challan = Challan.drop('Cust_Code')
                Challan = Challan.groupby('vouch_date','code','item_det_code','Branch_Code').agg({'Org_Qty':'sum'})
                Challan = Challan.withColumnRenamed('sum(Org_Qty)','Tot_Qty')
                Challan = Challan.withColumn("vouch_date",Challan['vouch_date'].cast(DateType()))
                print(Challan.count())
                Challan.cache()
                if(cdm_back != '201803'):
                    Challan_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Pending_Challan/YearMonth="+cdm_back)
                    Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                    Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm_back)
                    max_date = Date_format_range.agg({"ED": "max"}).collect()[0][0]
                    Challan_back_month = Challan_back_month.filter(Challan_back_month['vouch_date'] == max_date)
                    Challan_back_month = Challan_back_month.drop('YearMonth')
                    Challan = Challan.unionByName(Challan_back_month)
                    print(Challan.count())
                    Challan.cache()
            except:
                if(cdm_back != '201803'):
                    Challan_back_month = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Pending_Challan/YearMonth="+cdm_back)
                    Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                    Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm_back)
                    max_date = Date_format_range.agg({"ED": "max"}).collect()[0][0]                
                    Challan = Challan_back_month.filter(Challan_back_month['vouch_date'] == max_date)
                    Challan = Challan.drop('YearMonth')
                    print(Challan.count())
                    Challan.cache()
                 
            if(Challan.count() > 0):
                print(Challan.count())
                Code_Challan = Challan.select('code').drop_duplicates()
                try:
                    sales = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales/YearMonth="+cdm)
                    sales = sales.select('vouch_date','Challan_Code','item_det_code','Tot_Qty','cust_code','Deleted')
                    sales = sales.filter(sales['Deleted'] == 0)
                    sales = sales.join(Acc,'cust_code','left')
                    sales = sales.drop('Deleted','cust_code')
                    sales = sales.withColumnRenamed('Challan_Code','code')
                    sales = Code_Challan.join(sales,'code',how='left')
                    sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
                    sales = sales.withColumn('Tot_Qty',sales['Tot_Qty']*-1)
                    Challan = Challan.unionByName(sales)            
                except:
                    print("jai")
                
                Challan = Challan.groupby('vouch_date','code','item_det_code','Branch_Code').agg({'Tot_Qty':'sum'})
                Challan = Challan.withColumnRenamed('sum(Tot_Qty)','Tot_Qty')
                Challan_distinct = Challan.select('code','item_det_code','Branch_Code').drop_duplicates()
                Challan_distinct = Challan_distinct.withColumn('Key',lit('a'))
                
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] == cdm)
                SDate = Challan.agg({"vouch_date": "min"}).collect()[0][0]
                EDate = Date_format_range.agg({"ED": "max"}).collect()[0][0]
                Date_range = pd.DataFrame(pd.date_range(start = SDate,end =EDate, freq='D'),columns = ['date_'])
                Date_range = spark.createDataFrame(Date_range)
                Date_range = Date_range.withColumn("vouch_date",Date_range['date_'].cast(DateType()))
                Date_range = Date_range.withColumn('Key',lit('a'))
                Date_range = Date_range.join(Challan_distinct,'Key','left')
                Date_range = Date_range.withColumn('Key' , concat_ws('_',Date_range['vouch_date'],Date_range['code'],Date_range['item_det_code'],Date_range['Branch_Code']))
                
                Challan = Challan.withColumn('Key' , concat_ws('_',Challan['vouch_date'],Challan['code'],Challan['item_det_code'],Challan['Branch_Code']))
                Challan = Challan.drop('vouch_date','code','item_det_code','Branch_Code')
                Challan = Date_range.join(Challan,'Key', how = 'left')
                Challan = Challan.drop('Key','date_')
                Challan = Challan.fillna(0, subset=['Tot_Qty'])
                
                my_window = Window.partitionBy('item_Det_Code','code','Branch_Code').orderBy("vouch_date")
                PendingChallan = Challan.withColumn('Tot_Qty', sumf('Tot_Qty').over(my_window).alias('Tot_Qty'))
                PendingChallan = PendingChallan.withColumn('YearMonth',yearf(PendingChallan['vouch_date'])*100+date_format(PendingChallan['vouch_date'],'MM'))
                PendingChallan = PendingChallan.withColumn('YearMonth',PendingChallan['YearMonth'].cast(IntegerType()))
                PendingChallan = PendingChallan.withColumn('YearMonth',PendingChallan['YearMonth'].cast(StringType()))
                PendingChallan = PendingChallan.filter(PendingChallan['YearMonth'] == cdm)
                PendingChallan = PendingChallan.filter(PendingChallan['Tot_Qty'] != 0)
                PendingChallan.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Pending_Challan")
                print("PendingChallan Done")
    
            if((int(datetime.today().month)+1 == month) & (int(datetime.today().year) == Start_Year)):
                print(datetime.now())            
                PendingChallan = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Pending_Challan")
                Date_format_range = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DateFormat")
                Date_format_range = Date_format_range.filter(Date_format_range['Key'] <= cdm)
                Date_format_range = Date_format_range.withColumn('vouch_date',when(Date_format_range['Key'] == cdm,datetime.now().date() - timedelta(days = 1)).otherwise(Date_format_range['ED']))
                Date_format_range = Date_format_range.select('vouch_date')
                PendingChallan = Date_format_range.join(PendingChallan,'vouch_date','left')
                print(datetime.now())
                write_data_sql(PendingChallan,"PendingChallanI",owmode)
                print(datetime.now())
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PendingChallanI','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(PendingChallan.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
                log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
                write_data_sql(log_df,"Logs",mode="append")
                exit()
    
    

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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PendingChallanI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")                