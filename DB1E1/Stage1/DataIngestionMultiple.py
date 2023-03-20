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
conf = SparkConf().setMaster(smaster).setAppName("Stage1_transaction").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()
sqlctx = SQLContext(sc)

print(datetime.now())
#PUHead1         
Start_Year = 2018
try:   
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/PUHead1") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            PUHead1 = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(ph.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.Vouch_Date)),2) as YearMonth,ph.Vouch_Date,ph1.Vouch_Code as Vouch1,ph1.remarks1,ph1.gr_number,ph1.gr_date,ph1.remarks2,ph1.remarks3,ph1.Transport_Mode from Pur_Head"+FY+" ph left join Pur_Head1"+FY+" ph1 on ph.Vouch_Code = ph1.Vouch_Code ) as pur")
            PUHead1 = PUHead1.where(col("Vouch_Date").isNotNull())
            PUHead1.cache()
            
            PUHead1.show(1)
            if(PUHead1.count() > 0):
                PUHead1 = PUHead1.withColumn("Vouch_Date",PUHead1['Vouch_Date'].cast(DateType()))
                PUHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PUHead1")
                PUHead1.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PUHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(PUHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                PUHead1 =read_data_sql ("(SELECT CONVERT(VARCHAR(4),YEAR(ph.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.Vouch_Date)),2) as YearMonth,ph.Vouch_Date, ph1.Vouch_Code as Vouch1,ph1.remarks1,ph1.gr_number,ph1.gr_date,ph1.remarks2,ph1.remarks3,ph1.Transport_Mode from Pur_Head"+FY+" ph left join Pur_Head1"+FY+" ph1 on ph.Vouch_Code = ph1.Vouch_Code  where month(ph.vouch_date) = "+str(month)+") as pur")
                PUHead1 = PUHead1.where(col("Vouch_Date").isNotNull())
                
                PUHead1.cache()
                if(PUHead1.count() > 0):
                    cdm = str(datetime.today().year)
                    if(month <= 9):
                        cdm = cdm + '0' + str(month)
                    else:
                        cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r hdfs://103.248.60.14:9000/KOCKPIT/Market/Stage1/PUHead1/YearMonth="+cdm)
                    PUHead1 = PUHead1.withColumn("Vouch_Date",PUHead1['Vouch_Date'].cast(DateType()))
                    #PUHead1.show(1)
                    PUHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PUHead1")            
                    #PUHead1.show(1)
                    print('PUHead1')
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PUHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(PUHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PUHead1','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
    
#SLHead1            
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/SLHead1") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            SLHead1 = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Date,sh1.Vouch_Code as Vouch1,sh1.remarks1,sh1.remarks2,sh1.remarks3 from Sl_Head"+FY+" sh left join Sl_Head1"+FY+" sh1 on sh.Vouch_Code = sh1.Vouch_Code ) as pur")
            SLHead1 = SLHead1.where(col("Vouch_Date").isNotNull())
            SLHead1.cache()
            
            SLHead1.show(1)
            if(SLHead1.count() > 0):
                SLHead1 = SLHead1.withColumn("Vouch_Date",SLHead1['Vouch_Date'].cast(DateType()))
                SLHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLHead1")
                SLHead1.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                SLHead1 =read_data_sql ("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Date, sh1.Vouch_Code as Vouch1,sh1.remarks1,sh1.remarks2,sh1.remarks3 from Sl_Head"+FY+" sh left join Sl_Head1"+FY+" sh1 on sh.Vouch_Code = sh1.Vouch_Code  where month(sh.vouch_date) = "+str(month)+" ) as pur")
                SLHead1 = SLHead1.where(col("Vouch_Date").isNotNull())
                
                SLHead1.cache()
                if(SLHead1.count() > 0):
                    cdm = str(datetime.today().year)
                    if(month <= 9):
                        cdm = cdm + '0' + str(month)
                    else:
                        cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r hdfs://103.248.60.14:9000/KOCKPIT/Market/Stage1/SLHead1/YearMonth="+cdm)
                    SLHead1 = SLHead1.withColumn("Vouch_Date",SLHead1['Vouch_Date'].cast(DateType()))
                    #SLHead1.show(1)
                    SLHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLHead1")            
                    #SLHead1.show(1)
                    print('SLHead1')
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLHead1','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
    

    
    
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/Slcc") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            print(FY)
            Slcc = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth,st.MPM_Code,st.Code,st.CC_No_Code, sh.vouch_date, sh.Branch_Code ,\
                      st.vouch_code from Sl_CC_Det"+FY+" st left join sl_head"+FY+" sh on st.vouch_code = sh.vouch_code) as sh")
            SLMPM  = read_data_sql("(SELECT Gift_Vouch_Amount,vouch_code,Cash_Amount ,CC_Amount,Credit_Amount FROM Sl_MPM"+FY+" ) as SLMPM")
            Slcc = Slcc.join(SLMPM,"vouch_code","left")
            
            Slcc = Slcc.where(col("vouch_date").isNotNull())   
            print(Slcc.count())
            Slcc.cache()
            if(Slcc.count() > 0):
                Slcc = Slcc.withColumn("vouch_date",Slcc['vouch_date'].cast(DateType()))
                Slcc.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Slcc")
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Slcc','DB':DB,'EN':Etn,
                        'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(Slcc.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            print(FY)
            try:
                Slcc = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth,st.MPM_Code,st.Code,st.CC_No_Code, sh.vouch_date, sh.Branch_Code ,\
                      st.vouch_code from Sl_CC_Det"+FY+" st left join sl_head"+FY+" sh on st.vouch_code = sh.vouch_code  where month(sh.vouch_date) = "+str(month)+") as sh")
                SLMPM  = read_data_sql("(SELECT Gift_Vouch_Amount,vouch_code,Cash_Amount ,CC_Amount,Credit_Amount FROM Sl_MPM"+FY+" ) as SLMPM")
                Slcc = Slcc.join(SLMPM,"vouch_code","left")
                Slcc = Slcc.where(col("vouch_date").isNotNull())
                Slcc.cache()
                if(Slcc.count() > 0):
                    cdm = str(datetime.today().year)
                    if(month <= 9):
                        cdm = cdm + '0' + str(month)
                    else:
                        cdm = cdm + str(month)
                    #hdfs://103.248.60.14:9000
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/Slcc/YearMonth="+cdm)
                    Slcc = Slcc.withColumn("vouch_date",Slcc['vouch_date'].cast(DateType()))
                    Slcc.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Slcc")
                    print('SLcc')
            except Exception as e:
                print(e)
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Slcc','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(Slcc.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Slcc','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append") 
    print("\U0001F600")


#STRF         
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/STRF") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            STRF = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Date,st.Vouch_Code,st.Godown_Code,st.Rate,sh.Vouch_Num,sh.Cust_Code,st.Lot_Code_To,sh.Branch_Code,sh.Dept_Code,sh.G_Code_From,st.Quantity,sh.G_Code_To,sh.Remarks_1 from STRF_HEAD"+FY+" sh left join STRF_TXN"+FY+" st on sh.Vouch_Code = st.Vouch_Code ) as strf")
            STRF = STRF.where(col("Vouch_Date").isNotNull())
            STRF.cache()
            
            STRF.show(1)
            if(STRF.count() > 0):
                STRF = STRF.withColumn("Vouch_Date",STRF['Vouch_Date'].cast(DateType()))
                STRF.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/STRF")
                STRF.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'STRF','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(STRF.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                STRF = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Date,st.Vouch_Code,sh.Vouch_Num,sh.Cust_Code,st.Godown_Code,st.Rate,sh.Branch_Code,st.Lot_Code_To,sh.Dept_Code,sh.G_Code_From,st.Quantity,sh.G_Code_To,sh.Remarks_1 from STRF_HEAD"+FY+" sh left join STRF_TXN"+FY+" st on sh.Vouch_Code = st.Vouch_Code  where month(sh.vouch_date) = "+str(month)+") as strf")
                STRF = STRF.where(col("Vouch_Date").isNotNull())
                print('1')
                STRF.cache()
                if(STRF.count() > 0):
                    cdm = str(datetime.today().year)
                    if(month <= 9):
                        cdm = cdm + '0' + str(month)
                    else:
                        cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r hdfs://103.248.60.14:9000/KOCKPIT/Market/Stage1/STRF/YearMonth="+cdm)
                    STRF = STRF.withColumn("Vouch_Date",STRF['Vouch_Date'].cast(DateType()))
                    #STRF.show(1)
                    STRF.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/STRF")            
                    #STRF.show(1)
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'STRF','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(STRF.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'STRF','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
    

##SLSCH
Start_Year = 2018
try:   
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/SLSCH") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            print(FY)
            SLSCH = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Code,sh.Vouch_Date,sc.Code_Type,sc.Sch_Det_Code,sc.SL_Txn_Code,sc.CD,sc.TD,sc.CALC_CD,sc.CALC_TD,st.Calc_Gross_Amt,st.Tot_Qty,\
                                    st.Calc_Commission,st.Calc_Net_Amt as NET_SALE_VALUE,st.calc_sp_commission,st.calc_rdf,st.calc_scheme_u,st.calc_scheme_rs,(st.calc_tax_1) as Total_GST_Amount_Paid,st.Calc_Tax_2,st.Calc_Tax_3,st.calc_sur_on_tax3,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,\
                                    sh.Net_Amt,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Freight,st.Calc_Adjust,st.calc_excise_u,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,st.Item_Det_Code,st.Lot_Code,st.Calc_Adjustment_u,\
                                   st.Calc_Adjust_RS,st.Sale_Or_SR as Type,st.Deleted,sh.Cust_Code,sh.Branch_Code,sh.Stock_Trans from SL_Head"+FY+" sh left join SL_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code inner join SL_SCH"+FY+" sc on sc.SL_Txn_Code = st.Code ) as slsch")

            SLSCH = SLSCH.where(col("Vouch_Date").isNotNull())
            print(SLSCH.count())
            SLSCH.cache()
            if(SLSCH.count() > 0):
                SLSCH = SLSCH.withColumn("Vouch_Date",SLSCH['Vouch_Date'].cast(DateType()))
                SLSCH.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLSCH")
                #SLSCH.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLSCH','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLSCH.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                SLSCH = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Code,sh.Vouch_Date,sc.Code_Type,sc.Sch_Det_Code,sc.SL_Txn_Code,sc.CD,sc.TD,sc.CALC_CD,sc.CALC_TD,st.Calc_Gross_Amt,st.Tot_Qty,\
                                    st.Calc_Commission,st.Calc_Net_Amt as NET_SALE_VALUE,st.calc_sp_commission,st.calc_rdf,st.calc_scheme_u,st.calc_scheme_rs,(st.calc_tax_1) as Total_GST_Amount_Paid,st.Calc_Tax_2,st.Calc_Tax_3,st.calc_sur_on_tax3,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,\
                                    sh.Net_Amt,st.Calc_Adjust_RS,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Freight,st.Calc_Adjust,st.calc_excise_u,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,st.Item_Det_Code,st.Lot_Code,st.Calc_Adjustment_u,\
                            st.Sale_Or_SR as Type,st.Deleted,sh.Cust_Code,sh.Branch_Code,sh.Stock_Trans from SL_Head"+FY+" sh left join SL_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code inner join SL_SCH"+FY+" sc on sc.SL_Txn_Code = st.Code where month(sh.vouch_date) = "+str(month)+" ) as slsch")

                SLSCH = SLSCH.where(col("Vouch_Date").isNotNull())
                print('1')
                SLSCH.cache()
                if(SLSCH.count() > 0):
                    cdm = str(datetime.today().year)
                    if(month <= 9):
                        cdm = cdm + '0' + str(month)
                    else:
                        cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r hdfs://103.248.60.14:9000/KOCKPIT/Market/Stage1/SLSCH/YearMonth="+cdm)
                    SLSCH = SLSCH.withColumn("Vouch_Date",SLSCH['Vouch_Date'].cast(DateType()))
                    #SLSCH.show(1)
                    SLSCH.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLSCH")            
                    #SLSCH.show(1)
                    print('SLSCH')
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLSCH','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLSCH.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLSCH','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
print(datetime.now())