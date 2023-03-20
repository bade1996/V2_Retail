'''
Created on 2 Jan 2019
@author: Ashish,Aniket
'''
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
#from PIL.JpegImagePlugin import COM
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')


stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()#.strftime('%H:%M:%S')

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
yr= Datelog.split('-')[0]
oyr=int(yr)-1
newFY=str(oyr)+yr
'''
print("CurrentFiscalYr:"+newFY)


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
    print(RPASS)
    print(hdfspath)
    exit()    
    cdate_old = config.iloc[6]['Val']
    cdate_new = str(datetime.datetime.today().date())
    config = config.replace(cdate_old,cdate_new)
    config.to_csv(config_path+"/Config/conf.csv",index=False)

    conf = SparkConf().setMaster(smaster).setAppName("Stage1:Data_Ingestion").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:Data_Ingestion").getOrCreate()
    #AccDt = sqlctx.read.parquet(hdfspath+"/Data/AccessDetails").filter(col('NewDBName')==DB).collect()
    #CmpDt = sqlctx.read.parquet(hdfspath+"/Data/CompanyDetails").filter(col('DBName')==DB).filter(col('EntityName')==Etn)

    #Company = CmpDt.select(CmpDt.EntityName).collect()[0]['EntityName']
    #Sqlurl="jdbc:sqlserver://"+AccDt[0]['serverip']+port+";databaseName="+AccDt[0]['databasename']+";user="+AccDt[0]['userid']+";password=koc@P2019"
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS                
    
    CalendarFY=sqlctx.read.parquet(hdfspath+"/Stage1/CalendarFY")
    CalendarFY.show()
    Rno=CalendarFY.count()
    oldFY= CalendarFY.select('Fiscal_Year_FULL').collect()[Rno-1]["Fiscal_Year_FULL"]
    
    #.groupBy('CalendarFY').agg(F.first('CalendarFY'))
    #print(newFY)
    print("OldFiscalYr"+oldFY)
    
    
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
    ##############Fiscal Yr Not Check###################
    
    df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_2")  #FY Not checked
#     df.show()
#     exit()
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

    print("SUCCESSSSSSSSSSSSSSSSSSSFFFFFFFFFFFFFUUUUUUUULLLLLLLLLLLLLLLLLL")
    
    
    ###############################NEW MASTER TABLES ADD APR 13, 2020##########################
    list1 = ["Agents_Brokers","Comm_Calc_Info","Group_Mst","city_","Tax_Regions"]
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    for j in list1:
        start_time = datetime.datetime.now()
        stime = start_time.strftime('%H:%M:%S')
        tbname = j
#         if tbname=="Agents_Brokers":
#             continue
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
        
        tables = "(SELECT "+temp+" FROM ["+tbname+"]) AS data1"
        
        #tables = "(SELECT * FROM ["+tbname+"]) AS data1"
        
        table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        
        table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
        table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
        table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
        table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
        
        table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbname1)
        print("WRITTEN IN PARQUET ",tbname)
        
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        #try:
        #    IDEorBatch = sys.argv[1]
        #except Exception as e :
        #    IDEorBatch = "IDLE"
        #log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
        #        'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
        #log_df = sqlctx.createDataFrame(log_dict,schema_log)
        #log_df.write.mode(apmode).save(hdfspath+"/Logs")
    
    
    print("SUCCESSSSSSSSSSSSSSSSSSSFFFFFFFFFFFFFUUUUUUUULLLLLLLLLLLLLLLLLL")
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'2_dataIngestion','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo.':'NA','Operation':'Full','Rows':'MultipleTables','BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    ###############################NEW MASTER TABLES END###############################
    
    
    '''
    ##############Fiscal Year Check###################
    df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_1")  #FY checked   in Stage1_1 :    Sl_Txn   Pur_Txn    # Discarded  SL_Txn   SL_Sch
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    if oldFY==newFY:
            
        for j in range(0,df.count()):
            start_time = datetime.datetime.now()
            stime = start_time.strftime('%H:%M:%S')
            tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
            tbname1 = tbname.replace(' ','')
            tbwOyr =tbname1
            #tbname1=tbname1+"20172018"
            tbname1=tbname1+str(newFY)
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
            ############FISCAL YAER ADDED
            table = table.withColumn("FiscalYear",lit(newFY))
            table.cache()
            #changed below
            prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
            prevTab.cache()
            #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                                    
            table=table.unionByName(prevTab)      ## Union Previous and current Table
            table.cache()
        
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr)    # before ......tbname1
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'2_dataIngestion','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
#     end_time = datetime.datetime.now()
#     endtime = end_time.strftime('%H:%M:%S')
#     etime = str(end_time-start_time)
#     etime = etime.split('.')[0]
#     try:
#         IDEorBatch = sys.argv[1]
#     except Exception as e :
#         IDEorBatch = "IDLE"
#     log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Stage1','DB':DB,'EN':Etn,'Status':'Failed',\
#             'Log_Status':ex,'ErrorLineNo.':str(exc_traceback.tb_lineno),'Rows':0,'Columns':0,'Source':IDEorBatch}]
#     log_df = spark.createDataFrame(log_dict,schema_log)
#     log_df.write.mode(apmode).save(hdfspath+"/Logs")
