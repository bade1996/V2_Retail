'''
Created on 2 Jan 2019
@author: Ashish,Aniket
'''
#FOT TABLE stk_dtxn   USED IN Stk_Concat snapshot, Single Table, Stk_Concat     DEST. stk_dtxn
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
import dateutil.relativedelta
#from PIL.JpegImagePlugin import COM

#import org.apache.log4j.Logger
#import org.apache.log4j.Level
#Logger.getLogger("org").setLevel(Level.OFF) Logger.getLogger("akka").setLevel(Level.OFF)

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

present=datetime.datetime.now()#.strftime('%Y-%m-%d')   ###### PRESENT DATE
# present='2020-04-01'
# present=datetime.datetime.strptime(present,"%Y-%m-%d")
if present.month<=3:
    year1=str(present.year-1)
    year2=str(present.year)
    presentFY=year1+year2
else:
    year1=str(present.year)
    year2=str(present.year+1)
    presentFY=year1+year2

print(present)
CM=present.month    ## CM = CURRENT MONTH
CM = str(CM)
#print(CM)
'''
yr= Datelog.split('-')[0]
oyr=int(yr)-1
newFY=str(oyr)+yr
''
print("CurrentFiscalYr:"+newFY)
'''
if present.month==3:
    rollD=28
else:
    rollD=30

#previous=present-datetime.timedelta(days=rollD)
previous=present-dateutil.relativedelta.relativedelta(months=1)   ############
#previous=present-datetime.timedelta(months=1)
previous = previous.strftime('%Y-%m-%d')
#print(previous)
previous = previous[:-2]
previous=previous+"01"
previous=datetime.datetime.strptime(previous,"%Y-%m-%d")  ######## PREVIOUS MONTH DAY 1
#print(previous)

PM=previous.month  ## PM = PAST MONTH/ PREVIOUS MONTH
PM = str(PM)
#print(PM)


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

    conf = SparkConf().setMaster(smaster).setAppName("Stage1:7_DataIngeststk_dtxn").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:7_DataIngeststk_dtxn").getOrCreate()



    Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
                    
    
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
        # Sl_Head  Sl_Txn
        if tbname1=="stk_dtxn":
            tbwOyr = tbname1
            
            ####### CONDITION WHERE PARQUET DOES NOT EXIST #######
            #### SLHAEDSL_TXN1
            #if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/stk_dtxn2")==256: # 256 Code Defines PARQUET DOES NOT EXIST
            if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Baaaaal")==256:
                print("RUNNING 20192020  YEARS DATA")
                table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                        +" where schema_name(schema_id) = 'dbo' and name like '%stk_dtxn________') AS data1"
                
                SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                SQLyear.cache()
                SQLyear=SQLyear.collect()[0]["table_name"]
                print("SQLMAX",SQLyear)
                
                #tableAll=sqlctx.createDataFrame(sc.emptyRDD())
                for FY in range(20182019,(int(SQLyear)+100),10001):    # 10001
                    if FY==20192020:
                        print(FY)
                        
                        tbname1=tbname1+str(FY)
                        tbname=tbname1
                        # 29 feb
                        
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
                        
                        print(tbname)
                        
                        if FY == 20182019:
                            tables = "(SELECT "+temp+" FROM ["+tbname+"]) AS data1"
                        else:
                            tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE date_ > (select min(date_) from "+tbname+")) AS data1"
                        print(FY)
                        '''
                        (SELECT [lot_code],[net_qty],[date_],[sl_qty],[sr_qty],[pu_qty],[pr_qty],[sl_gross_val],[sr_gross_val],[sl_net_val],[sr_net_val],
                        [issue_qty],[rec_qty],[net_qty_Kgs],[sl_qty_kgs],[sr_qty_kgs],[pu_qty_kgs],[pr_qty_kgs],[issue_qty_kgs],[rec_qty_kgs],[prod_qty],
                        [prod_kgs],[Sl_Free],[Sl_Repl],[Sl_Free_Kgs],[Sl_Repl_Kgs],[Pu_Free],[Pu_Repl],[Pu_Free_Kgs],[Pu_Repl_Kgs],[Pr_Free],[Pr_Repl],
                        [Pr_Free_Kgs],[Pr_Repl_Kgs],[Sr_Free],[Sr_Repl],[Sr_Free_Kgs],[Sr_Repl_Kgs],[Net_Free_Qty],[Net_Free_Kgs],[Stk_In],[Stk_Out]
                        ,[Stk_In_Kgs],[Stk_Out_Kgs],[Pu_Gross_Val],[Pr_Gross_Val],[Pu_Net_Val],[Pr_Net_Val],[Prod_Issue],[Prod_Issue_Kgs],[Txn_Type],
                        [Sl_Sample],[Sr_Sample],[Pu_Sample],[Pr_Sample],[Net_Sample_Qty],[Branch_Code],[Godown_Code] FROM [stk_dtxn20172018]) AS data1
                        '''
                        ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                        #print(tables)
                        table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                        
                        #table=table.filter()#FILTER OUT 31March Data
                        
                        
                        table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                        table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                        table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                        table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                        ############FISCAL YAER ADDED
                        table = table.withColumn("fiscalyear",lit(FY))     ##changed from newFY
                        ######## YAER MONTH FLAG ADD
                        table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.date_)))
                        
                        table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                        
                        table = table.drop_duplicates()     ####### 14 MAR
                        
                        table.write.partitionBy("yearmonth").mode("append").save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")
                        #tableAll = tableAll.unionByName(table)
                        #tableAll.cache()
                        
                    
                #tableAll.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2") ##
                print("SUCCESSFULLY RAN FULL RELOAD OF MULTIPLE YEARS")
                
                ###########################EINDING#####################
            else:
                ############# IF PARQUET EXISTS #############
                '''
                prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1")
                prevTab.show()
                '''
                print("LOAD LAST TWO MONTHS")
                
                if pastFY==presentFY:
                    ########### SAME FISCAL YEAR  ##########
                    
                    #list = ['20192020']#,'20182019','20192020']
                    #for i in list:
                        #pastFY=i
                        
                    tbname1=tbname1+str(pastFY)
                    tbname=tbname1                  ### Sl_Head
                    # 29 feb
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
                    
                    print(tbname)
                    
                    ############### PREVIOUS MONTH
                    '''
                    tablesPM = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE MONTH(SH.vouch_date) ="+PM+") AS data1"
                    '''
                    tablesPM = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+PM+") AS data1"
                
                    
                    tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    ############FISCAL YAER ADDED
                    tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.date_)))
                    tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
                    
                    tablePM = tablePM.drop_duplicates()     ####### 14 MAR
                    #cnt = tablePM.count()
                    #print("HUBBBBAAA",cnt)
                    #tablePM=tablePM.filter(tablePM.lot_code==3541315).show()
                    #exit()
                    #tablePM.cache()
                    tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2"+"/yearmonth="+pastFY+PM)
                    
                    
                    
                    
                    ################ CURRENT MONTH
                    tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+CM+") AS data1"
                    
                    ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                    
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    
                    table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                    ############FISCAL YAER ADDED
                    table = table.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.date_)))
                    table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                    
                    table = table.drop_duplicates()     ####### 14 MAR
                    
                    #table.cache()
                    table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2"+"/yearmonth="+pastFY+CM)
                    
                    
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
                    ########### DIFFERENT FISCAL YEAR  ##########
                    #################################1st part PREVIOUS MONTH OF PASTFY###################
                    
                    tbname1=tbname1+str(pastFY)
                    tbname=tbname1                  ##
                    # 29 feb
                    
    #                     print(tbname1)
    #                     sys.exit()
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
                    
                    print(tbname)
                    
                    tablesPM = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+PM+") AS data1"
                    
                    tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    ############FISCAL YAER ADDED
                    tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.date_)))
                    tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
                    
                    tablePM = tablePM.drop_duplicates()     ####### 14 MAR
                    #tablePM.cache()
                    tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2"+"/yearmonth="+pastFY+PM)
                    
                    
                    
                    
                    ########################### 2ND PART CURRENT MONTH OF PRESENT FY ################
                    ################ CURRENT MONTH
                    '''
                    table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                        +" where schema_name(schema_id) = 'dbo' and name like '%Sl_Txn________') AS data1"
                    SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    SQLyear=SQLyear.collect()[0]["table_name"]
                    presentFY = SQLyear
                    print(SQLyear)
                    '''
                    tbname1="stk_dtxn"
                    tbname1=tbname1+str(presentFY)
                    tbname=tbname1                  #
                    
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
                    
                    print(tbname)
                    #print(tbname3)
                    
                    #tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+CM+") AS data1"
                    tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE date_ > (select min(date_) from "+tbname+")) AS data1"
                    
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    
                    table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                    ############FISCAL YAER ADDED
                    table = table.withColumn("fiscalyear",lit(presentFY))     ##changed from newFY
                    
                    table = table.drop_duplicates()     ####### 14 MAR
                    
                    ######## YAER MONTH FLAG ADD
                    table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.date_)))
                    table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                    
                    #table.cache()
                    table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2"+"/yearmonth="+presentFY+CM)
                    
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
