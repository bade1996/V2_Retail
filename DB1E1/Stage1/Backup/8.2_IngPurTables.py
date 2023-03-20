'''
NOT USED SCRIPT THERE ARE 2 DIFFRENT SCRIPTS FOR PURCHASE POAnalysis & PurchaseAnalysis
Created on 2 Jan 2019
@author: Ashish,Aniket
'''
#FOT TABLE Pur_Head JOIN Pur_Txn  ;  PO_Head JOIN PO_Txn   USED IN purchase script 

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
import dateutil.relativedelta
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

#present=Datelog
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
# print("baal"+past)
# print(PM) 
# print(pastFY)
# print(present)
# print(CM)
# print(presentFY)
# exit()


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

    conf = SparkConf().setMaster(smaster).setAppName("Stage1:11_IngLast2Mth").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:11_IngLast2Mth").getOrCreate()

    #AccDt = sqlctx.read.parquet(hdfspath+"/Data/AccessDetails").filter(col('NewDBName')==DB).collect()
    #CmpDt = sqlctx.read.parquet(hdfspath+"/Data/CompanyDetails").filter(col('DBName')==DB).filter(col('EntityName')==Etn)

    #Company = CmpDt.select(CmpDt.EntityName).collect()[0]['EntityName']
    #Sqlurl="jdbc:sqlserver://"+AccDt[0]['serverip']+port+";databaseName="+AccDt[0]['databasename']+";user="+AccDt[0]['userid']+";password=koc@P2019"
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    
    rowsW=0
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
    #df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_Rolling")  #FY checked
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    
#     for j in range(0,df.count()):
#         start_time = datetime.datetime.now()
#         stime = start_time.strftime('%H:%M:%S')
#         tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
#         tbname1 = tbname.replace(' ','')
        
        #tbname1=tbname1+"20172018"
        #################PAST DATE##################
        # Sl_Head  Sl_Txn
    tbname1="Pur_Head"
    tbwOyr = tbname1
    
    tbname3 = "Pur_Txn"   #29 Feb
    tbwOyr3 = tbname3
    
    #PO_Head
    #PO_Txn

    
    ####### CONDITION WHERE PARQUET DOES NOT EXIST #######
    #### SLHAEDSL_TXN1
    if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Pur_HeadPur_Txn")==256:
        ######### Path does not exist##### PARQUET DOES NOT EXIST
        print("NOT EXIST")
        ############### IF PARQUET DOES NOT EXIST#####################
        print("LOAD FULL PARQUET")
        #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1")
        ########################## MAR 3 TESTING ###############
        '''
        if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn45")==256:
            ######### Path does not exist##### PARQUET DOES NOT EXIST
            print("NOT EXIST")
            print(os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn45"))
        
        '''
        
        table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                +" where schema_name(schema_id) = 'dbo' and name like '%Pur_Txn________') AS data1"
        SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        SQLyear=SQLyear.collect()[0]["table_name"]
        print(SQLyear)
        
        for FY in range(20182019,int(SQLyear+100),10001):
            print(FY)
            FY=str(FY)
            #tbname1=tbname1+str(FY)
            #tbname=tbname1
            # 29 feb
            #tbname3=tbname3+str(FY)
            #print(tbname1)
            #print(tbname3)
            #exit()
            '''
            schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
            
            schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
            
            temp = ''
            for i in range(0,len(schema)):
                col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                    col_name_temp = "PH."+col_name_temp
                elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                    col_name_temp = ","+"PH." + col_name_temp
                if schema[i]['DATA_TYPE']=='sql_variant':
                    temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                else:
                    temp = temp + col_name_temp
            '''
            #print(tbname)
            #print(tbname3)
            '''
            tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
            +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
             +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
             +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
             +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
             +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
             +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
             +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code ) AS data1"
            '''
                    
            tables = "(SELECT PH.vouch_date,PH.vouch_code,PH.Bill_No,PH.Bill_Date,PH.GRN_PreFix,PH.GRN_Number,"\
            +"PT.Order_Item_Code,PT.Item_Det_Code,PT.Tot_Qty,PT.Calc_Net_Amt,PT.Calc_Tax_3,PT.Calc_Sur_On_Tax3 "\
            +"FROM Pur_Head"+FY+" AS PH LEFT JOIN Pur_Txn"+FY+" AS PT ON PH.vouch_code=PT.vouch_code) AS Data1"
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(FY))     ##changed from newFY
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            tableAll1 = tableAll1.unionByName(table)
            tableAll1.cache()
            
            
            
            ###################2ND ONE
            tables = "(SELECT PH.Vouch_Date,PH.Order_Date,PH.Vouch_Code,PH.Branch_Code,PH.Sup_Code,PH.NetOrder_Amount,PH.Order_No,"\
            +"PT.Code,PT.Item_Det_Code AS POTItemDetCode,PT.Tot_Qty AS POTTot_Qty,PT.Rate AS POTRate,PT.Code_Type AS POTCodeType "\
            +"FROM PO_Head"+FY+" AS PH LEFT JOIN PO_Txn"+FY+" AS PT ON PH.vouch_code=PT.vouch_code) AS Data1"
            
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(FY))     ##changed from newFY
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            tableAll2 = tableAll2.unionByName(table)
            tableAll2.cache()
            
        
        tableAll1.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn") ##
        tableAll2.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PO_HeadPO_Txn") ##
        rowsW=rowsW+table.count()
        ###########################EINDING#####################
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
    else:
        ############# IF PARQUET EXISTS #############
        
        print("LOAD LAST TWO MONTHS")
        if pastFY==presentFY:
            #list = ['20192020']#,'20182019','20192020']
            #for i in list:
                #pastFY=i
                
            tbname1=tbname1+str(pastFY)
            tbname=tbname1                  ### Sl_Head
            # 29 feb
            tbname3=tbname3+str(pastFY)     ### Sl_Txn

            '''
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
            '''
            #print(tbname)
            #print(tbname3)
            
            
            ############### PREVIOUS MONTH
            
            tablesPM = "(SELECT PH.vouch_date,PH.vouch_code,PH.Bill_No,PH.Bill_Date,PH.GRN_PreFix,PH.GRN_Number,"\
            +"PT.Order_Item_Code,PT.Item_Det_Code,PT.Tot_Qty,PT.Calc_Net_Amt,PT.Calc_Tax_3,PT.Calc_Sur_On_Tax3 "\
            +"FROM Pur_Head"+pastFY+" AS PH LEFT JOIN Pur_Txn"+pastFY+" AS PT ON PH.vouch_code=PT.vouch_code WHERE MONTH(SH.vouch_date) ="+PM+") AS Data1"
            
            
            tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
            ############FISCAL YAER ADDED
            tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.vouch_date)))
            tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
            
            #tablePM.cache()
            tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+"Pur_HeadPur_Txn"+"/yearmonth="+pastFY+PM)
            
            
            
            ################ CURRENT MONTH
            tables = "(SELECT PH.vouch_date,PH.vouch_code,PH.Bill_No,PH.Bill_Date,PH.GRN_PreFix,PH.GRN_Number,"\
            +"PT.Order_Item_Code,PT.Item_Det_Code,PT.Tot_Qty,PT.Calc_Net_Amt,PT.Calc_Tax_3,PT.Calc_Sur_On_Tax3 "\
            +"FROM Pur_Head"+pastFY+" AS PH LEFT JOIN Pur_Txn"+pastFY+" AS PT ON PH.vouch_code=PT.vouch_code WHERE MONTH(SH.vouch_date) ="+CM+") AS Data1"
            
            ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table.cache()
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn"+"/yearmonth="+pastFY+CM)
            
            
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
            #################################1st part PREVIOUS MONTH OF PASTFY###################
            tbname1=tbname1+str(pastFY)
            tbname=tbname1                  ### Sl_Head
            # 29 feb
            tbname3=tbname3+str(pastFY)
            
            ############### PREVIOUS MONTH
            tablesPM = "(SELECT PH.vouch_date,PH.vouch_code,PH.Bill_No,PH.Bill_Date,PH.GRN_PreFix,PH.GRN_Number,"\
            +"PT.Order_Item_Code,PT.Item_Det_Code,PT.Tot_Qty,PT.Calc_Net_Amt,PT.Calc_Tax_3,PT.Calc_Sur_On_Tax3 "\
            +"FROM Pur_Head"+pastFY+" AS PH LEFT JOIN Pur_Txn"+pastFY+" AS PT ON PH.vouch_code=PT.vouch_code WHERE MONTH(SH.vouch_date) ="+PM+") AS Data1"
            
            
            tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
            ############FISCAL YAER ADDED
            tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.vouch_date)))
            tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
            
            #tablePM.cache()
            tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+"Pur_HeadPur_Txn"+"/yearmonth="+pastFY+PM)
            
            
            
            ########################### 2ND PART CURRENT MONTH OF PRESENT FY ################
            ################ CURRENT MONTH
            
            tables = "(SELECT PH.vouch_date,PH.vouch_code,PH.Bill_No,PH.Bill_Date,PH.GRN_PreFix,PH.GRN_Number,"\
            +"PT.Order_Item_Code,PT.Item_Det_Code,PT.Tot_Qty,PT.Calc_Net_Amt,PT.Calc_Tax_3,PT.Calc_Sur_On_Tax3 "\
            +"FROM Pur_Head"+presentFY+" AS PH LEFT JOIN Pur_Txn"+presentFY+" AS PT ON PH.vouch_code=PT.vouch_code WHERE MONTH(SH.vouch_date) ="+CM+") AS Data1"
            
            ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
            
            
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(presentFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table.cache()
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn"+"/yearmonth="+presentFY+CM)
            
            
            
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
