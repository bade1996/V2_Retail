
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
    password  = "Market!@999"
    SQLurl = "jdbc:sqlserver://103.234.187.190:24993;databaseName="+database
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

def delete_data(dqry):
    server = '103.234.187.190:2499' 
    database = 'KOCKPIT' 
    username = 'sa' 
    password = 'Market!@999' 
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
conf = SparkConf().setMaster(smaster).setAppName("Damage")\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.executor.cores","3")\
                .set("spark.executor.memory","4g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)


BS = sqlctx.read.parquet(hdfspath+"/Market/Stage2/BillSerial")

BS = BS.select('series_code','Group_Name')




print(datetime.now())
Start_Year = 2018
try:
    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Damage")
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Damage") !=0):
        SL = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
        # SL = SL.filter(SL[''] == 0).filter(SL[''] == 0)
        # SL=SL.filter(SL['Branch_Code']==5004).filter(SL['item_det_code']==72006)\
                    #.filter(SL['vouch_date'].between('2020-12-01','2020-12-31'))
        SL = SL.join(BS,on=['series_code'],how='left')
        SL = SL.withColumn('Tot_Qty',when(SL.Type=='SL',SL.Tot_Qty).otherwise(SL.Tot_Qty*-1))
        SL = SL.withColumn('NET_SALE_VALUE',when(SL.Type=='SL',SL.NET_SALE_VALUE).otherwise(SL.NET_SALE_VALUE*-1))
        SL=SL.withColumn('BI',SL['Tot_Qty'] *SL['Tot_Qty'])
        SL=SL.groupby('vouch_date', 'Branch_Code', 'item_det_code', 'lot_code', 'Group_Name','Stock_Trans','Deleted','YearMonth')\
                .agg(sum('BI').alias('BI'),sum('Tot_Qty').alias('Tot_Qty'),sum('NET_SALE_VALUE').alias('NET_SALE_VALUE'))
        #SL.show()
        SL.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Damage")
        write_data_sql(SL,"Damage",owmode)
        
        
except Exception as e:
    print(e)

# try:
    # if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Damage") !=0):
        # SL = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
        # #SL = SL.select('YearMonth','NET_SALE_VALUE', 'vouch_date', 'Branch_Code', 'item_det_code', 'lot_code', 'Tot_Qty','series_code')
        # SL.cache()
        # if(SL.count() > 0):
        #
            # SL = SL.filter(SL['Stock_Trans'] == 0).filter(SL['Deleted'] == 0)
            # SL = SL.join(BS,on=['series_code'],how='left' )
            #
            #
            # SL=SL.withColumn('BI',SL['Tot_Qty'] *SL['Tot_Qty'])
            # SL=SL.filter(SL['Branch_Code']==5004).filter(SL['item_det_code']==72006)\
                    # .filter(SL['vouch_date'].between('2020-12-01','2020-12-31') )
            # SL=SL.groupby('item_det_code','Group_Name').agg(sum('Tot_Qty').alias('qty'))        
            # # SL=SL.groupby('YearMonth','NET_SALE_VALUE', 'vouch_date', 'Branch_Code', 'item_det_code', 'lot_code', 'Tot_Qty','Group_Name')\
                              # # .agg(sum('BI').alias('BI'),sum('Tot_Qty').alias('Tot_Qty'))
            # SL.show()
            # exit()
            # SL.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Damage")
            # print("Damage Done")
            # print(datetime.now())
            # end_time = datetime.now()
            # endtime = end_time.strftime('%H:%M:%S')
            # etime = str(end_time-start_time)
            # etime = etime.split('.')[0]
            # log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Damage','DB':DB,'EN':Etn,
                # 'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SL.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
            # log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            # write_data_sql(log_df,"Logs",mode="append")
    # else:
        # for i in range(0,3):
            # month =  datetime.today().month - i
            # if(month <= 3):
                # Start_Year = (datetime.today().year) - 1
                # End_Year = (datetime.today().year)
            # else:
                # Start_Year = (datetime.today().year) 
                # End_Year = int(datetime.today().year) + 1
            # FY = str(Start_Year)+str(End_Year)
            # try:
                # cdm = str(datetime.today().year)
                # if(month == 0):
                    # month = 12
                    # cdm = str(datetime.today().year - 1)
                # if(month == -1):
                    # month = 11
                    # cdm = str(datetime.today().year - 1)
                    #
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                # else:
                    # cdm = cdm + str(month)
                # SL = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales/YearMonth="+cdm)
                # os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Damage/YearMonth="+cdm)
                #
                # SL = SL.join(BS,on=['series_code'],how='left' )
                # SL = SL.filter(SL['Stock_Trans'] == 0)#.filter(SL['Deleted'] == 0)   
                #
                # SL=SL.withColumn('BI',SL['Tot_Qty'] *SL['Tot_Qty'] )
                # SL = SL.withColumn('YearMonth',lit(cdm))
                #
                # SL=SL.groupby('YearMonth','NET_SALE_VALUE', 'vouch_date', 'Branch_Code', 'item_det_code', 'lot_code', 'Tot_Qty','Group_Name')\
                              # .agg(sum('BI').alias('BI'))
                              #
                # #SL.show(10)
                # SL.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Damage")
                # delete_data(''' DELETE FROM KOCKPIT.dbo.Damage WHERE YearMonth = ''' + cdm)
                # print("Damage Done")              
                #
            # except Exception as e:
                # print(e)
        # print(datetime.now())
        # #SL.show()
        # SL = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Damage")
        # SL = SL.drop("YearMonth")
        # write_data_sql(SL,"Damage",owmode)
        # end_time = datetime.now()
        # endtime = end_time.strftime('%H:%M:%S')
        # etime = str(end_time-start_time)
        # etime = etime.split('.')[0]
        # log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Damage','DB':DB,'EN':Etn,
            # 'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SL.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
        # log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
        # write_data_sql(log_df,"Logs",mode="append")
# except Exception as ex:
    # print(ex)
    # exc_type,exc_value,exc_traceback=sys.exc_info()
    # print("Error:",ex)
    # print("type - "+str(exc_type))
    # print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    # print("Error Line No. - "+str(exc_traceback.tb_lineno))
    # end_time = datetime.now()
    # endtime = end_time.strftime('%H:%M:%S')
    # etime = str(end_time-start_time)
    # etime = etime.split('.')[0]
    # log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Damage','DB':DB,'EN':Etn,
        # 'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    # log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    # write_data_sql(log_df,"Logs",mode="append")  
print(datetime.now())
print("\U0001F637")