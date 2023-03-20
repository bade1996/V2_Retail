'''
Created on 2 Jan 2019
@author: Ashish,Aniket
'''
## FOR TABLE ONLY SL_Head
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import last
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udl
import pandas as pd
from _datetime import date
import subprocess

#from PIL.JpegImagePlugin import COM
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
schema_log = StructType([
        StructField('Date',StringType(),True),
        StructField('Start_Time',StringType(),True),
        StructField('End_Time', StringType(),True),
        StructField('Run_Time',StringType(),True),
        StructField('File_Name',StringType(),True),
        StructField('DB',StringType(),True),
        StructField('EN', StringType(),True),
        StructField('Status',StringType(),True),
        StructField('Log_Status',StringType(),True),
        StructField('ErrorLineNo.',StringType(),True),
        StructField('Rows',IntegerType(),True),
        StructField('Columns',IntegerType(),True),
        StructField('Source',StringType(),True)
        ])

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')
'''
print("Today:"+Datelog)

if Datelog>='2019-04-01' and Datelog<='2020-03-31':
    newFY='20192020'
elif Datelog>='2020-04-01' and Datelog<='2021-03-31':
    newFY='20202021'
elif Datelog>='2021-04-01' and Datelog<='2022-03-31':
    newFY='20212022'
elif Datelog>='2022-04-01' and Datelog<='2023-03-31':
    newFY='20222023'
elif Datelog>='2023-04-01' and Datelog<='2024-03-31':
    newFY='20232024'
elif Datelog>='2024-04-01' and Datelog<='2025-03-31':
    newFY='20242025'
elif Datelog>='2025-04-01' and Datelog<='2026-03-31':
    newFY='20252026'
'''
#present=Datelog
present=datetime.datetime.now()#.strftime('%Y-%m-%d')
if present.month<=3:
    year1=str(present.year-1)
    year2=str(present.year)
    presentFY=year1+year2
else:
    year1=str(present.year)
    year2=str(present.year+1)
    presentFY=year1+year2

    
'''
yr= Datelog.split('-')[0]
oyr=int(yr)-1
newFY=str(oyr)+yr
''
print("CurrentFiscalYr:"+newFY)
'''
rollD=90

previous=present-datetime.timedelta(days=rollD)
if previous.month<=3:
    year1=str(previous.year-1)
    year2=str(previous.year)
    previousFY=year1+year2
else:
    year1=str(previous.year)
    year2=str(previous.year+1)
    previousFY=year1+year2

past=previous.strftime('%Y-%m-%d')
present=present.strftime('%Y-%m-%d')
pastFY=previousFY
# print(past)
# print(pastFY)
# print(present)
# print(presentFY)


try:
    config = os.path.dirname(os.path.realpath(__file__))
    DBET = config[config.rfind("DB"):config.rfind("/")]
    Etn = DBET[DBET.rfind("E"):]
    DB = DBET[:DBET.rfind("E")]
    config_path = config[0:config.rfind("DB")]
    path = config = config[0:config.rfind("DB")]
    path = "file://"+path
    config = pd.read_csv(config+"/Config/conf.csv")
    
    
    for i in range(0,len(config)):
        exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
        
    cdate_old = config.iloc[6]['Val']
    cdate_new = str(datetime.datetime.today().date())
    config = config.replace(cdate_old,cdate_new)
    config.to_csv(config_path+"/Config/conf.csv",index=False)

    conf = SparkConf().setMaster(smaster).setAppName("Stage1:6_DataIngestSL_Head").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:6_DataIngestSL_Head").getOrCreate()

    #AccDt = sqlctx.read.parquet(hdfspath+"/Data/AccessDetails").filter(col('NewDBName')==DB).collect()
    #CmpDt = sqlctx.read.parquet(hdfspath+"/Data/CompanyDetails").filter(col('DBName')==DB).filter(col('EntityName')==Etn)

    #Company = CmpDt.select(CmpDt.EntityName).collect()[0]['EntityName']
    #Sqlurl="jdbc:sqlserver://"+AccDt[0]['serverip']+port+";databaseName="+AccDt[0]['databasename']+";user="+AccDt[0]['userid']+";password=koc@P2019"
    Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    #Sqlurl="jdbc:sqlserver://103.248.60.5:1433;databaseName=LOGICDBS99;user=sa;password=M99@321"
    #Sqlurl="jdbc:sqlserver://103.248.60.5:1433;databaseName=LOGICDBS99;user=Administrator;password=f'GxNpnC0NV4"
    '''
    CalendarFY=sqlctx.read.parquet(hdfspath+"/Stage1/CalendarFY")
    CalendarFY.show()
    Rno=CalendarFY.count()
    oldFY= CalendarFY.select('Fiscal_Year_FULL').collect()[Rno-1]["Fiscal_Year_FULL"]

    #.groupBy('CalendarFY').agg(F.first('CalendarFY'))
    #print(newFY)
    print("OldFiscalYr"+oldFY)
    '''
    
    '''
    #stage1 = sqlctx.read.csv(path+"/nav_13_16.csv",header="true")
    stage2 = sqlctx.read.csv(path+"/Config/stage2.csv",header="true")
    
    ##stage1 = stage2.select('Table_Name').distinct()  #line taken from parquet craetion
    
    stage1_1 = sqlctx.read.csv(path+"/Config/stage1_1.csv",header="true")  #consists name of tables whose FY wiil be checked
    
    stage1_2 = sqlctx.read.csv(path+"/Config/stage1_2.csv",header="true")  #consists name of tables whose FY will not be checked
    #changed above 27 jan
    stage2 = stage2.select(col("Table_Name"),col("Col_Name"),regexp_replace(col("Script_Name"),' ',''))\
            .withColumnRenamed("regexp_replace(Script_Name,  , )","Script_Name")
    stage2 = stage2.select(col("Script_Name"),col("Col_Name"),regexp_replace(col("Table_Name"),' ',''))\
            .withColumnRenamed("regexp_replace(Table_Name,  , )","Table_Name")
    stage2 = stage2.select(col("Script_Name"),col("Table_Name"),regexp_replace(col("Col_Name"),' ',''))\
            .withColumnRenamed("regexp_replace(Col_Name,  , )","Col_Name")
            
    stage1_1.write.mode(owmode).save(hdfspath+"/Stage1/Stage1_1")     #consists name of tables whose data will be refreshed once
    stage1_2.write.mode(owmode).save(hdfspath+"/Stage1/Stage1_2")     #consists name of tables whose data will be refreshed everytime
    
    stage2.write.mode(owmode).save(hdfspath+"/Stage2/Stage2")
    stage2.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/Stage2")
    stage2.show(250)
    print("YuHu")
    '''
    '''
    ##############Fiscal Yr Not Check###################
    df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_2")  #FY Not checked
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    for j in range(0,df.count()):
        start_time = datetime.datetime.now()
        stime = start_time.strftime('%H:%M:%S')
        tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
        tbname1 = tbname.replace(' ','')
        #tbname = TB_Name+tbname
        schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
        schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
        temp = ''
        for i in range(0,len(schema)):
            col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
            if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                col_name_temp = col_name_temp
            elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                col_name_temp = "," + col_name_temp
            if schema[i]['DATA_TYPE']=='sql_variant':
                temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
            else:
                temp = temp + col_name_temp
        print(temp)
        print(tbname)
        tables = "(SELECT "+temp+" FROM ["+tbname+"]) AS data1"
        if tbname1 == 'G_LEntry':
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                lowerBound=0, upperBound=10000000, numPartitions=3).load()
        elif tbname1 == 'ValueEntry':
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                lowerBound=0, upperBound=3000000, numPartitions=3).load()
        else:
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        
        table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
        table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
        table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
        table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
        table.cache()
        table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbname1)
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
                'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
        log_df = sqlctx.createDataFrame(log_dict,schema_log)
        log_df.write.mode(apmode).save(hdfspath+"/Logs")
        
    '''
    ##############Fiscal Year Check###################
    df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_Rolling")  #FY checked
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    for j in range(0,df.count()):
        start_time = datetime.datetime.now()
        stime = start_time.strftime('%H:%M:%S')
        tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
        tbname1 = tbname.replace(' ','')
        
        #tbname1=tbname1+"20172018"
        #################PAST DATE##################
        if tbname1=="SL_Head":
            tbwOyr = tbname1
            #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")
            
            if pastFY==presentFY:
                tbname1=tbname1+str(pastFY)
                tbname=tbname1
    #             print(tbname1)
    #             sys.exit()
                #tbname = TB_Name+tbname
                #changed Below1
                #if tbname1 =='Sl_Txn20192020':
                schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
            
                schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                temp = ''
                for i in range(0,len(schema)):
                    col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                    if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                        col_name_temp = col_name_temp
                    elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                        col_name_temp = "," + col_name_temp
                    if schema[i]['DATA_TYPE']=='sql_variant':
                        temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                    else:
                        temp = temp + col_name_temp
                print(temp)
                print(tbname)
                tables = "(SELECT "+temp+" FROM ["+tbname+"]  WHERE vouch_date >="+past+") AS data1"
                if tbname1 == 'G_LEntry':
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                        lowerBound=0, upperBound=10000000, numPartitions=3).load()
                elif tbname1 == 'ValueEntry':
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                        lowerBound=0, upperBound=3000000, numPartitions=3).load()
                else:
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                
                table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                ############FISCAL YAER ADDED
                table = table.withColumn("FiscalYear",lit(pastFY))     ##changed from newFY
                table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                table.cache()
                #changed below
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr)
                prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")
                #prevTab.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")
                #exit()
                #prevTab.filter(prevTab.vouch_date>="2020-02-18").show()
                prevTab=prevTab.filter(prevTab.vouch_date<past)
                #prevTab=prevTab.filter(prevTab.vouch_date=='2019-11-28')
                #prevTab.cache()
                #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
                table=prevTab.unionByName(table)      ## Union Previous and current Table
                table.cache()
                
                table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")   #Separate Storing location
                ################DELETE COMPLETELY THE CONTENTS OF TABLE READ FROM HDFS########26Feb
                #bashCommand = "hadoop fs -rm /KOCKPITDB1E1/Stage2/shit/*"
                bashCommand = "hadoop fs -rmdir /KOCKPIT/DB1E1/Stage1/"+tbwOyr+"2"
                #print(bashCommand)
                #exit()
                process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                output, error = process.communicate()
                #end
                #####READ FROM THE SEPARATE LOCATION
                table=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")
                #sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")
                
                table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")    # before ......tbname1
                #end
                print("SUCCESSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
                end_time = datetime.datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
                        'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
                log_df = sqlctx.createDataFrame(log_dict,schema_log)
                log_df.write.mode(apmode).save(hdfspath+"/Logs")
                
            else:
                #################################1st part from begin of past date to end of past table###################
                tbname1=tbname1+str(pastFY)
                tbname=tbname1
                tbname2=tbname1+str(presentFY)
    #             print(tbname1)
    #             sys.exit()
                #tbname = TB_Name+tbname
                #changed Below1
                #if tbname1 =='Sl_Txn20192020':
                schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
                schema.cache()
                schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                temp = ''
                for i in range(0,len(schema)):
                    col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                    if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                        col_name_temp = col_name_temp
                    elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                        col_name_temp = "," + col_name_temp
                    if schema[i]['DATA_TYPE']=='sql_variant':
                        temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                    else:
                        temp = temp + col_name_temp
                print(temp)
                print(tbname)
                tables = "(SELECT "+temp+" FROM ["+tbname+"]  WHERE vouch_date >="+past+") AS data1"
                if tbname1 == 'G_LEntry':
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                        lowerBound=0, upperBound=10000000, numPartitions=3).load()
                elif tbname1 == 'ValueEntry':
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                        lowerBound=0, upperBound=3000000, numPartitions=3).load()
                else:
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                
                table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                ############FISCAL YAER ADDED
                table = table.withColumn("FiscalYear",lit(pastFY))     ##changed from newFY
                table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                table.cache()
                #changed below
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr)
                prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")
                prevTab=prevTab.filter(prevTab.vouch_date<past)
                #prevTab.cache()
                #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
                table11=prevTab.unionByName(table)      ## Union Previous and current Table
                table11.cache()
                #table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr)    # before ......tbname1
                ################################2ND part upto present date of present table##########################
                tbname=tbname2
                tbname1=tbname2
    #             print(tbname1)
    #             sys.exit()
                #tbname = TB_Name+tbname
                #changed Below1
                #if tbname1 =='Sl_Txn20192020':
                schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
                schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                temp = ''
                for i in range(0,len(schema)):
                    col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                    if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                        col_name_temp = col_name_temp
                    elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                        col_name_temp = "," + col_name_temp
                    if schema[i]['DATA_TYPE']=='sql_variant':
                        temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                    else:
                        temp = temp + col_name_temp
                print(temp)
                print(tbname)
                tables = "(SELECT "+temp+" FROM ["+tbname+"]  WHERE vouch_date <="+present+") AS data1"
                if tbname1 == 'G_LEntry':
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                        lowerBound=0, upperBound=10000000, numPartitions=3).load()
                elif tbname1 == 'ValueEntry':
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                        lowerBound=0, upperBound=3000000, numPartitions=3).load()
                else:
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                
                table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                ############FISCAL YAER ADDED
                table = table.withColumn("FiscalYear",lit(presentFY))     ##changed from newFY
                table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                table.cache()
                #changed below
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr).filter(prevTab.vouch_date<past)
                #prevTab.cache()
                #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
                table=table11.unionByName(table)
                #table=table.unionByName(prevTab)      ## Union Previous and current Table
                table.cache()
                table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")   #Separate Storing location
                ################DELETE COMPLETELY THE CONTENTS OF TABLE READ FROM HDFS########26Feb
                bashCommand = "hadoop fs -rmdir /KOCKPIT/DB1E1/Stage1/"+tbwOyr+"2"
                process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                output, error = process.communicate()
                #####READ FROM THE SEPARATE LOCATION
                table=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")
                
                table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")    # before ......tbname1
                print("SUCCESSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
                #end
                end_time = datetime.datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
                        'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
                log_df = sqlctx.createDataFrame(log_dict,schema_log)
                log_df.write.mode(apmode).save(hdfspath+"/Logs")

    '''      
    ################DELETE COMPLETELY THE CONTENTS OF TABLE READ FROM HDFS########26Feb
                #bashCommand = "hadoop fs -rm /KOCKPITDB1E1/Stage2/shit/*"
                bashCommand = "hadoop fs -rmdir /KOCKPIT/DB1E1/Stage1/"+tbwOyr+"2"
                #print(bashCommand)
                #exit()
                process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                output, error = process.communicate()
                #end
                #####READ FROM THE SEPARATE LOCATION
                table=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")
    else:
            
        for j in range(0,df.count()):
            start_time = datetime.datetime.now()
            stime = start_time.strftime('%H:%M:%S')
            tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
            tbname1 = tbname.replace(' ','')
            tbwOyr =tbname1
            #tbname1=tbname1+"20172018"
            ################LOAD OLD TABLE#################
            tbname3=tbname1
            tbname1=tbname1+str(oldFY)
            tbname=tbname1
#             print(tbname1)
#             sys.exit()
            #tbname = TB_Name+tbname
            #changed Below1
            #if tbname1 =='Sl_Txn20192020':
            schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
            schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
            temp = ''
            for i in range(0,len(schema)):
                col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                    col_name_temp = col_name_temp
                elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                    col_name_temp = "," + col_name_temp
                if schema[i]['DATA_TYPE']=='sql_variant':
                    temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                else:
                    temp = temp + col_name_temp
            print(temp)
            print(tbname)
            tables = "(SELECT "+temp+" FROM ["+tbname+"]) AS data1"
            if tbname1 == 'G_LEntry':
                table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                    lowerBound=0, upperBound=10000000, numPartitions=3).load()
            elif tbname1 == 'ValueEntry':
                table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                    lowerBound=0, upperBound=3000000, numPartitions=3).load()
            else:
                table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ###### Fiscal Year Added
            table = table.withColumn("FiscalYear",lit(oldFY))

            table.cache()
            ###############LOADS THE OLDFY TABLE AND CONCAT AND WRITE TO PREVIOUS TABLE#############
            ###############changed below
            prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
            prevTab.cache()
            #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table=table.unionByName(prevTab)      ## Union Previous and current Table
            table.cache()
        
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")    # again write to previous table
            
            
            #end
            
            
            ################LOAD NEW TABLE#################
            tbname1=tbname3+str(newFY)
            tbname=tbname1
#             print(tbname1)
#             sys.exit()
            #tbname = TB_Name+tbname
            #changed Below1
            #if tbname1 =='Sl_Txn20192020':
            schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
            schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
            temp = ''
            for i in range(0,len(schema)):
                col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                    col_name_temp = col_name_temp
                elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                    col_name_temp = "," + col_name_temp
                if schema[i]['DATA_TYPE']=='sql_variant':
                    temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                else:
                    temp = temp + col_name_temp
            print(temp)
            print(tbname)
            tables = "(SELECT "+temp+" FROM ["+tbname+"]) AS data1"
            if tbname1 == 'G_LEntry':
                table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                    lowerBound=0, upperBound=10000000, numPartitions=3).load()
            elif tbname1 == 'ValueEntry':
                table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                    lowerBound=0, upperBound=3000000, numPartitions=3).load()
            else:
                table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############# FISCAL YEAR ADDED
            table = table.withColumn("FiscalYear",lit(newFY))
            table.cache()
            #####################LOADS NEWFY TABLE THEN CONCAT WITH PREVIOUS TABLE THEN WRITE TO GEN TABLE DIRECTORY#################
            #changed below
            prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
            prevTab.cache()
            #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table=table.unionByName(prevTab)      ## Union Previous and current Table
            table.cache()
        
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr)    # Final Table
            #end
            
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
                    'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
            log_df = sqlctx.createDataFrame(log_dict,schema_log)
            log_df.write.mode(apmode).save(hdfspath+"/Logs")
            '''
except Exception as ex:
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Stage1','DB':DB,'EN':Etn,'Status':'Failed',\
            'Log_Status':ex,'ErrorLineNo.':str(exc_traceback.tb_lineno),'Rows':0,'Columns':0,'Source':IDEorBatch}]
    log_df = spark.createDataFrame(log_dict,schema_log)
    log_df.write.mode(apmode).save(hdfspath+"/Logs")
