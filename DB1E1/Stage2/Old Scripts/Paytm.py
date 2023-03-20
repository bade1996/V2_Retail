
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import sum as sumf, first as firstf, month as monthf
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




#For reading data from the SQL server
def read_data_sql(table_string):
    database = "LOGICDBS99"
    user = "sa"
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\MARKET99BI;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

def write_data_sql(df,name,mode):
    database = "KOCKPIT"
    user = "sa"
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\MARKET99BI;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df.write.jdbc(url = SQLurl , table = name, properties = SQLprop, mode = mode)

def delete_data(dqry):
    server = '103.248.60.5' 
    database = 'KOCKPIT' 
    username = 'sa' 
    password = 'M99@321' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute(dqry)
    cnxn.commit()

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
conf = SparkConf().setMaster(smaster).setAppName("PaytmEDC").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)

PO = sqlctx.read.parquet(hdfspath+"/Market/Stage1/CCMst")

PO = PO.select('CC_No_Code' ,'CC_Party_Name')

print(datetime.now())
Start_Year = 2018
try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PaytmEdc") !=0):
        SLH22 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Slcc")
        SLH22.cache()
        if(SLH22.count() > 0):
            
            SLH22 = SLH22.join(PO,'CC_No_Code','left' ).drop('CC_No_Code').drop('MPM_Code').drop('vouch_date').drop('Branch_Code')
            #SLH22 =SLH22.filter((SLH22.CC_Party_Name.like('Paytm')) | (SLH22.CC_Party_Name.like('EDC MACHINES')))
                   
            SLH22.show(20)
            #exit()
                
            
            SLH22.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PaytmEdc")
            print("PaytmEdc Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PaytmEdc','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLH22.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
            log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            write_data_sql(log_df,"Logs",mode="append")
    else:
        for i in range(0,3):
            month =  datetime.today().month - i
            if(month <= 3):
                Start_Year = (datetime.today().year) - 1
                End_Year = (datetime.today().year)
            else:
                Start_Year = (datetime.today().year) 
                End_Year = int(datetime.today().year) + 1
            FY = str(Start_Year)+str(End_Year)
            try:
                cdm = str(datetime.today().year)
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                SLH22 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Slcc/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PaytmEdc/YearMonth="+cdm)
                
                SLH22 = SLH22.join(PO,'CC_No_Code','left' ).drop('CC_No_Code').drop('MPM_Code').drop('vouch_date').drop('Branch_Code')
                #SLH22 =SLH22.filter((SLH22.CC_Party_Name.like('Paytm')) | (SLH22.CC_Party_Name.like('EDC MACHINES')))
                SLH22 = SLH22.withColumn('YearMonth',lit(cdm))
                       
                
                
                SLH22.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PaytmEdc")
                delete_data('DELETE FROM KOCKPIT.dbo.PaytmEdc WHERE YearMonth = ' + cdm)
                print("PaytmEdc Done")              
                              
            except Exception as e:
                print(e)
        print(datetime.now())
        SLH22.show()
        SLH22 = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PaytmEdc")
        SLH22 = SLH22.drop("YearMonth")
        write_data_sql(SLH22,"PaytmEdc",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PaytmEdc','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLH22.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
        log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
        write_data_sql(log_df,"Logs",mode="append")
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PaytmEdc','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  





print("\U0001F600")
