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
# print(_start_time)
# exit()
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
conf = SparkConf().setMaster(smaster).setAppName("FFDGGV")\
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .set("spark.executor.cores","3")\
                .set("spark.executor.memory","4g")\
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
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)
print('Footfall' , datetime.now())


##Footfall
try:
    RF = sqlctx.read.parquet(hdfspath+"/Market/Stage2/RetailFootfall")
    RF = RF.select('Code','Branch_Code','Visit_Time','Visit_Date')
    RF= RF.withColumn('VisitHour',hour('Visit_Time'))
    # RF.show()
    # exit()
    RF = RF.withColumn('TimeInterval',when((RF['VisitHour'] >= 1) & (RF['VisitHour'] < 2),'1-2').
                                    when((RF['VisitHour'] >= 2) & (RF['VisitHour'] < 3),'2-3').
                                    when((RF['VisitHour'] >= 3) & (RF['VisitHour'] < 4),'3-4').
                                    when((RF['VisitHour'] >= 4) & (RF['VisitHour'] < 5),'4-5').
                                    when((RF['VisitHour'] >= 5) & (RF['VisitHour'] < 6),'5-6').
                                    when((RF['VisitHour'] >= 6) & (RF['VisitHour'] < 7),'6-7').
                                    when((RF['VisitHour'] >= 7) & (RF['VisitHour'] < 8),'7-8').
                                    when((RF['VisitHour'] >= 8) & (RF['VisitHour'] < 9),'8-9').
                                    when((RF['VisitHour'] >= 9) & (RF['VisitHour'] < 10),'9-10').
                                    when((RF['VisitHour'] >= 10) & (RF['VisitHour'] < 11),'10-11').
                                    when((RF['VisitHour'] >= 11) & (RF['VisitHour'] < 12),'11-12').
                                    when((RF['VisitHour'] >= 12) & (RF['VisitHour'] < 13),'12-13').
                                    when((RF['VisitHour'] >= 13) & (RF['VisitHour'] < 14),'13-14').
                                    when((RF['VisitHour'] >= 14) & (RF['VisitHour'] < 15),'14-15').
                                    when((RF['VisitHour'] >= 15) & (RF['VisitHour'] < 16),'15-16').
                                    when((RF['VisitHour'] >= 16) & (RF['VisitHour'] < 17),'16-17').
                                    when((RF['VisitHour'] >= 17) & (RF['VisitHour'] < 18),'17-18').
                                    when((RF['VisitHour'] >= 18) & (RF['VisitHour'] < 19),'18-19').
                                    when((RF['VisitHour'] >= 19) & (RF['VisitHour'] < 20),'19-20').
                                    when((RF['VisitHour'] >= 21) & (RF['VisitHour'] < 22),'21-22').
                                    when((RF['VisitHour'] >= 22) & (RF['VisitHour'] < 23),'22-23').
                                    when((RF['VisitHour'] >= 23) & (RF['VisitHour'] < 24),'23-24').otherwise('0-1'))
    
    RF =RF.groupby('Branch_Code','Visit_Date','TimeInterval')\
            .agg(count('Code').alias('Code'))
    
    #RF.show(10,False)
    RF.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/Footfall")
    RF.show()
    # exit()
    write_data_sql(RF,"Footfall",owmode)
    
    print("Footfall Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Footfall','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(RF.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    #log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Footfall','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    #log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")   
    



print('DiscountCoupoun' , datetime.now())
##DiscountCoupoun
try:
    D1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
    D1 = D1.select('vouch_date','Lot_Code','vouch_code','Calc_Adjust_RS')
    DC = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Discount_Coupon_Mst")
    
    DC =DC.select('Discount_Coupon_Num','Sale_Vouch_Code')
    
    DC = DC.join(D1,DC.Sale_Vouch_Code==D1.vouch_code,how='left')
    
    DC =DC.groupby('vouch_date','Lot_Code','Discount_Coupon_Num')\
            .agg(sum('Calc_Adjust_RS').alias('Calc_Adjust_RS'))
    
    #DC.show(10)
    
    
    DC.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/DiscountCoupoun")
    write_data_sql(DC,"DiscountCoupoun",owmode)
    print("DiscountCoupoun Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'DiscountCoupoun','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(DC.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    #log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'DiscountCoupoun','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    #log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")  

try:
    SLMPM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLMPM")
    CCM = sqlctx.read.parquet(hdfspath+"/Market/Stage2/CCMst")
    CCM = CCM.select('CC_No_Code' ,'CC_Party_Name')
    SLMPM = SLMPM.join(CCM,'CC_No_Code','left' )
    SLMPM.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/SLMPM")
    write_data_sql(SLMPM,"SLMPM",owmode)
    print("SLMPM Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLMPM','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(SLMPM.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    #log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLMPM','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    #log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")   
