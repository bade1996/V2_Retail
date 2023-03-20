'''
Created on 2 Jan 2019
@author: Ashish,Aniket
'''
### JOIN OF SL_HEAD AND SL_TXN then load

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

    conf = SparkConf().setMaster(smaster).setAppName("Stage1:10_DataIngestJOIN").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:10_DataIngestJOIN").getOrCreate()

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
        if tbname1=="Sl_Head":
            tbwOyr = tbname1
            #####29 feb
            tbname3 = "Sl_Txn"   #29 Feb
            tbwOyr3 = tbname3
            
            ##### mar 2
            '''
            pastFY='20172018'
            test=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+pastFY)    #
            test.show()
            print(test.count)
            exit()
            '''
            '''
            ############### CONCAT 20172018 20182019 20192020 THENDUMP TO FILENAME FILE########
            prev1718 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"20172018")
            prev1718.cache()
            prev1819 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"20182019")
            conTab = prev1718.unionByName(prev1819).cache()
            prev1920 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"20192020")
            conTab=conTab.unionByName(prev1920).cache()
            conTab.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3)
            exit()
            '''
            if pastFY==presentFY:
                #list = ['20192020']#,'20182019','20192020']
                #for i in list:
                    #pastFY=i
                    
                tbname1=tbname1+str(pastFY)
                tbname=tbname1
                # 29 feb
                tbname3=tbname3+str(pastFY)

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
                        col_name_temp = "SH."+col_name_temp
                    elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                        col_name_temp = ","+"SH." + col_name_temp
                    if schema[i]['DATA_TYPE']=='sql_variant':
                        temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                    else:
                        temp = temp + col_name_temp
                
                print(tbname)
                
                '''
                ########### 29 Feb
                schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname3+chr(39)+") AS data"
                
                schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                temp1 = ''
                for i in range(0,len(schema)):
                    col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                    if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                        col_name_temp = col_name_temp
                    elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                        col_name_temp = "," + col_name_temp
                    if schema[i]['DATA_TYPE']=='sql_variant':
                        temp1 =temp1 +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                    else:
                        temp1 = temp1 + col_name_temp
                print(temp1)
                '''
                print(tbname3)
                
                
                #tables = "(SELECT "+temp+" FROM ["+tbname+"]  WHERE vouch_date >="+past+") AS data1"
                tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                 +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                 +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                 +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                 +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                 +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                 +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE SH.vouch_date >="+past+") AS data1"
                
                ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                
                print(tables)
                exit()
                
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
                
                ###table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+pastFY)    # mar 1
                
                #changed below
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
                
                prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3)
                #prevTab.show()
                prevTab=prevTab.filter(prevTab.vouch_date<past)
                #prevTab.cache()
                #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
                table=prevTab.unionByName(table)      ## Union Previous and current Table
                table.cache()
                
                table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"5")   #Separate Storing location
                ################DELETE COMPLETELY THE CONTENTS OF TABLE READ FROM HDFS########26Feb
                ###################DUMP TO SEPARATE LOCATION & RENAME  28 Feb
                bashCommand = "hadoop fs -mv /KOCKPIT/DB1E1/Stage1/"+tbwOyr+tbwOyr3+"5"+" "+"/KOCKPIT/DB1E1/Stage1/"+tbwOyr+tbwOyr3
                process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                output, error = process.communicate()
                #####READ FROM THE SEPARATE LOCATION
                #table=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")
                
                #table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")    # before ......tbname1
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
