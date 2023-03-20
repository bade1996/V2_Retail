'''
Created on 2 Jan 2019
@author: Abhishek,Aniket
'''
##PO Analysis Query script   JOIN OF 4 TABLES
## Po_Head   Po_Txn   Pur_Head FY     Pur_TxnFY
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
# schema_log = StructType([
#         StructField('Date',StringType(),True),
#         StructField('Start_Time',StringType(),True),
#         StructField('End_Time', StringType(),True),
#         StructField('Run_Time',StringType(),True),
#         StructField('File_Name',StringType(),True),
#         StructField('DB',StringType(),True),
#         StructField('EN', StringType(),True),
#         StructField('Status',StringType(),True),
#         StructField('Log_Status',StringType(),True),
#         StructField('ErrorLineNo.',StringType(),True),
#         StructField('Rows',IntegerType(),True),
#         StructField('Columns',IntegerType(),True),
#         StructField('Source',StringType(),True)
#         ])

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

    conf = SparkConf().setMaster(smaster).setAppName("IngPOAnalysis1").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("IngPOAnalysis1").getOrCreate()

    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
                    
    rowsW=0
    ##############Fiscal Year Check###################
    #df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_Rolling")  #FY checked
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    
    #for j in range(0,df.count()):
    start_time = datetime.datetime.now()
    stime = start_time.strftime('%H:%M:%S')
    #tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
    #tbname1 = tbname.replace(' ','')
    
    #tbname1=tbname1+"20172018"
    #################PAST DATE##################
    tbname1='Pur_Txn'
    tbname2='Pur_Head'
    
        ####### CONDITION WHERE PARQUET DOES NOT EXIST #######
        #### SLHAEDSL_TXN1
    if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/PohPotPuhPut")==256: # 256 Code Defines PARQUET DOES NOT EXIST
    #if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Po_Txn")==256:
        
        print('FULL LOAD YEARS FROM 1819')
        table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                +" where schema_name(schema_id) = 'dbo' and name like '%Pur_Head________') AS data1"
        
        SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        SQLyear.cache()
        SQLyear=SQLyear.collect()[0]["table_name"]
        print("SQLMAX",SQLyear)
        
        for FY in range(20182019,(int(SQLyear)+100),10001):
            print(FY)
            
            tbname1='Pur_Txn'
            tbname2='Pur_Head'
            #continue
            tbname1=tbname1+str(FY)
            tbname=tbname1
            tbname2=tbname2+str(FY)
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
            
            tables = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                    POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code,
                    (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                    PUT.Tot_Qty AS PurchaseQty,(POT.Tot_Qty * POT.Rate) As OrderAmount, 
                    (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                    (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                    (PUT.Tot_Qty/(CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END))*100 AS FillRateQty, --26Mar
                    PUT.Calc_Tax_3 AS TAX, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TotalTAX, 
                    PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount,PUT.vouch_code
                    FROM Po_Txn AS POT LEFT JOIN """+tbname1+""" AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                    AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                    LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                    INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code) AS data1""" 
            
            ##Pur_Txn20192020
            ##Pur_Head20192020
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(FY))     ##changed from newFY
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.PoOrderDate)))
            
            ######################-PLAIN VALUE DICT ERROR May4#########################
            table=table.withColumn("PoOrderNo",table["PoOrderNo"].cast("string")).withColumn("BillNo",table["BillNo"].cast("string"))
            table=table.withColumn("EntityName",table["EntityName"].cast("string")).withColumn("DBName",table["DBName"].cast("string"))
            table=table.withColumn("yearmonth",table["yearmonth"].cast("string"))
            
            table=table.withColumn("PoOrderDate",table["PoOrderDate"].cast("timestamp")).withColumn("PoValidDate",table["PoValidDate"].cast("timestamp"))
            table=table.withColumn("BillDate",table["BillDate"].cast("timestamp")).withColumn("VoucherDate",table["VoucherDate"].cast("timestamp"))
            
            table=table.withColumn("GrnNo",table["GrnNo"].cast("decimal(19,4)")).withColumn("POQty",table["POQty"].cast("decimal(19,4)"))
            table=table.withColumn("PurchaseQty",table["PurchaseQty"].cast("decimal(19,4)")).withColumn("CancelPOQty",table["CancelPOQty"].cast("decimal(19,4)"))
            table=table.withColumn("PendingPOQty",table["PendingPOQty"].cast("decimal(19,4)")).withColumn("FillRateQty",table["FillRateQty"].cast("decimal(19,4)"))
            table=table.withColumn("TAX",table["TAX"].cast("decimal(19,4)")).withColumn("TotalTAX",table["TotalTAX"].cast("decimal(19,4)"))
            table=table.withColumn("NetAmount",table["NetAmount"].cast("decimal(19,4)")).withColumn("GrossAmount",table["GrossAmount"].cast("decimal(19,4)"))
            
            table=table.withColumn("Branch_Code",table["Branch_Code"].cast("integer")).withColumn("Sup_Code",table["Sup_Code"].cast("integer"))
            table=table.withColumn("Item_Det_Code",table["Item_Det_Code"].cast("integer")).withColumn("Lot_Code",table["Lot_Code"].cast("integer"))
            table=table.withColumn("vouch_code",table["vouch_code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))
            
            table=table.withColumn("OrderAmount",table["OrderAmount"].cast("double")).withColumn("BasicRate",table["BasicRate"].cast("double"))
            table=table.withColumn("POTRate",table["POTRate"].cast("double"))
            
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table = table.drop_duplicates()     ####### 14 MAR
            table.cache()
            table.write.partitionBy("yearmonth").mode("append").save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut")
            rowsW=rowsW+table.count()
            print("WRITTEN",tbname1)
            #tableAll = tableAll.unionByName(table)
            #tableAll.cache()
        print("SUCCESSFULLY RAN FULL YEARS RELOAD")
        cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut").count()
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'IngPOAnalysis1','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo.':'NA','Operation':'Full','Rows':str(rowsW),'BeforeETLRows':'0','AfterETLRows':str(cnt)}]
        log_df = sqlctx.createDataFrame(log_dict,schema_log)
        log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
        #tableAll.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut") ##
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
            print("SAME FISCAL YEAR")
            bcnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut").count()
            tbname1=tbname1+str(pastFY)
            tbname=tbname1                  ### Sl_Head
            tbname2=tbname2+str(pastFY)
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
            tablesPM = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                    POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code,
                    (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                    PUT.Tot_Qty AS PurchaseQty,(POT.Tot_Qty * POT.Rate) As OrderAmount, 
                    (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                    (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                    (PUT.Tot_Qty/(CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END))*100 AS FillRateQty, --26Mar
                    PUT.Calc_Tax_3 AS TAX, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TotalTAX, 
                    PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount,PUT.vouch_code
                    FROM Po_Txn AS POT LEFT JOIN """+tbname1+""" AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                    AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                    LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                    INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code WHERE MONTH(PUH.Vouch_Date) = """+PM+") AS data1"
            
            #tablesPM = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+PM+") AS data1"
        
            
            tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
            ############FISCAL YAER ADDED
            tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.PoOrderDate)))
            
            ######################-PLAIN VALUE DICT ERROR May4#########################
            tablePM=tablePM.withColumn("PoOrderNo",tablePM["PoOrderNo"].cast("string")).withColumn("BillNo",tablePM["BillNo"].cast("string"))
            tablePM=tablePM.withColumn("EntityName",tablePM["EntityName"].cast("string")).withColumn("DBName",tablePM["DBName"].cast("string"))
            tablePM=tablePM.withColumn("yearmonth",tablePM["yearmonth"].cast("string"))
            
            tablePM=tablePM.withColumn("PoOrderDate",tablePM["PoOrderDate"].cast("timestamp")).withColumn("PoValidDate",tablePM["PoValidDate"].cast("timestamp"))
            tablePM=tablePM.withColumn("BillDate",tablePM["BillDate"].cast("timestamp")).withColumn("VoucherDate",tablePM["VoucherDate"].cast("timestamp"))
            
            tablePM=tablePM.withColumn("GrnNo",tablePM["GrnNo"].cast("decimal(19,4)")).withColumn("POQty",tablePM["POQty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("PurchaseQty",tablePM["PurchaseQty"].cast("decimal(19,4)")).withColumn("CancelPOQty",tablePM["CancelPOQty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("PendingPOQty",tablePM["PendingPOQty"].cast("decimal(19,4)")).withColumn("FillRateQty",tablePM["FillRateQty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("TAX",tablePM["TAX"].cast("decimal(19,4)")).withColumn("TotalTAX",tablePM["TotalTAX"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("NetAmount",tablePM["NetAmount"].cast("decimal(19,4)")).withColumn("GrossAmount",tablePM["GrossAmount"].cast("decimal(19,4)"))
            
            tablePM=tablePM.withColumn("Branch_Code",tablePM["Branch_Code"].cast("integer")).withColumn("Sup_Code",tablePM["Sup_Code"].cast("integer"))
            tablePM=tablePM.withColumn("Item_Det_Code",tablePM["Item_Det_Code"].cast("integer")).withColumn("Lot_Code",tablePM["Lot_Code"].cast("integer"))
            tablePM=tablePM.withColumn("vouch_code",tablePM["vouch_code"].cast("integer")).withColumn("fiscalyear",tablePM["fiscalyear"].cast("integer"))
            
            tablePM=tablePM.withColumn("OrderAmount",tablePM["OrderAmount"].cast("double")).withColumn("BasicRate",tablePM["BasicRate"].cast("double"))
            tablePM=tablePM.withColumn("POTRate",tablePM["POTRate"].cast("double"))
            
            
            tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
            
            tablePM = tablePM.drop_duplicates()     ####### 14 MAR
            tablePM.cache()
            tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut/yearmonth="+pastFY+PM)
            
            
            
            ################ CURRENT MONTH
            tables = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                    POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code,
                    (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                    PUT.Tot_Qty AS PurchaseQty,(POT.Tot_Qty * POT.Rate) As OrderAmount, 
                    (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                    (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                    (PUT.Tot_Qty/(CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END))*100 AS FillRateQty, --26Mar
                    PUT.Calc_Tax_3 AS TAX, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TotalTAX, 
                    PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount,PUT.vouch_code
                    FROM Po_Txn AS POT LEFT JOIN """+tbname1+""" AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                    AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                    LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                    INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code WHERE MONTH(PUH.Vouch_Date) = """+CM+") AS data1"
            #tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+CM+") AS data1"
            
            ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.PoOrderDate)))
            
            ######################-PLAIN VALUE DICT ERROR May4#########################
            table=table.withColumn("PoOrderNo",table["PoOrderNo"].cast("string")).withColumn("BillNo",table["BillNo"].cast("string"))
            table=table.withColumn("EntityName",table["EntityName"].cast("string")).withColumn("DBName",table["DBName"].cast("string"))
            table=table.withColumn("yearmonth",table["yearmonth"].cast("string"))
            
            table=table.withColumn("PoOrderDate",table["PoOrderDate"].cast("timestamp")).withColumn("PoValidDate",table["PoValidDate"].cast("timestamp"))
            table=table.withColumn("BillDate",table["BillDate"].cast("timestamp")).withColumn("VoucherDate",table["VoucherDate"].cast("timestamp"))
            
            table=table.withColumn("GrnNo",table["GrnNo"].cast("decimal(19,4)")).withColumn("POQty",table["POQty"].cast("decimal(19,4)"))
            table=table.withColumn("PurchaseQty",table["PurchaseQty"].cast("decimal(19,4)")).withColumn("CancelPOQty",table["CancelPOQty"].cast("decimal(19,4)"))
            table=table.withColumn("PendingPOQty",table["PendingPOQty"].cast("decimal(19,4)")).withColumn("FillRateQty",table["FillRateQty"].cast("decimal(19,4)"))
            table=table.withColumn("TAX",table["TAX"].cast("decimal(19,4)")).withColumn("TotalTAX",table["TotalTAX"].cast("decimal(19,4)"))
            table=table.withColumn("NetAmount",table["NetAmount"].cast("decimal(19,4)")).withColumn("GrossAmount",table["GrossAmount"].cast("decimal(19,4)"))
            
            table=table.withColumn("Branch_Code",table["Branch_Code"].cast("integer")).withColumn("Sup_Code",table["Sup_Code"].cast("integer"))
            table=table.withColumn("Item_Det_Code",table["Item_Det_Code"].cast("integer")).withColumn("Lot_Code",table["Lot_Code"].cast("integer"))
            table=table.withColumn("vouch_code",table["vouch_code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))
            
            table=table.withColumn("OrderAmount",table["OrderAmount"].cast("double")).withColumn("BasicRate",table["BasicRate"].cast("double"))
            table=table.withColumn("POTRate",table["POTRate"].cast("double"))
            
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table = table.drop_duplicates()     ####### 14 MAR
            
            table.cache()
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut/yearmonth="+pastFY+CM)
            
            
            print("SUCCESSSSSSSSSSSSSS  RAN 2 MONTHS INCREMENTAL")
            cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut").count()
            Tcount=tablePM.count()+table.count()
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'IngPOAnalysis','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(Tcount),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
            log_df = sqlctx.createDataFrame(log_dict,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
            
        else:
            ########### DIFFERENT FISCAL YEAR  ##########
            #################################1st part PREVIOUS MONTH OF PASTFY###################
            print("DIFFERENT FISCAL YEAR")
            bcnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut").count()
            tbname1=tbname1+str(pastFY)
            tbname=tbname1                  ##
            tbname2=tbname2+str(pastFY)
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
            
            tablesPM = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                    POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code,
                    (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                    PUT.Tot_Qty AS PurchaseQty,(POT.Tot_Qty * POT.Rate) As OrderAmount, 
                    (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                    (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                    (PUT.Tot_Qty/(CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END))*100 AS FillRateQty, --26Mar
                    PUT.Calc_Tax_3 AS TAX, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TotalTAX, 
                    PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount,PUT.vouch_code
                    FROM Po_Txn AS POT LEFT JOIN """+tbname1+""" AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                    AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                    LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                    INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code WHERE MONTH(PUH.Vouch_Date) = """+PM+") AS data1"
            #tablesPM = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+PM+") AS data1"
            
            tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
            ############FISCAL YAER ADDED
            tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.PoOrderDate)))
            
            ######################-PLAIN VALUE DICT ERROR May4#########################
            tablePM=tablePM.withColumn("PoOrderNo",tablePM["PoOrderNo"].cast("string")).withColumn("BillNo",tablePM["BillNo"].cast("string"))
            tablePM=tablePM.withColumn("EntityName",tablePM["EntityName"].cast("string")).withColumn("DBName",tablePM["DBName"].cast("string"))
            tablePM=tablePM.withColumn("yearmonth",tablePM["yearmonth"].cast("string"))
            
            tablePM=tablePM.withColumn("PoOrderDate",tablePM["PoOrderDate"].cast("timestamp")).withColumn("PoValidDate",tablePM["PoValidDate"].cast("timestamp"))
            tablePM=tablePM.withColumn("BillDate",tablePM["BillDate"].cast("timestamp")).withColumn("VoucherDate",tablePM["VoucherDate"].cast("timestamp"))
            
            tablePM=tablePM.withColumn("GrnNo",tablePM["GrnNo"].cast("decimal(19,4)")).withColumn("POQty",tablePM["POQty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("PurchaseQty",tablePM["PurchaseQty"].cast("decimal(19,4)")).withColumn("CancelPOQty",tablePM["CancelPOQty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("PendingPOQty",tablePM["PendingPOQty"].cast("decimal(19,4)")).withColumn("FillRateQty",tablePM["FillRateQty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("TAX",tablePM["TAX"].cast("decimal(19,4)")).withColumn("TotalTAX",tablePM["TotalTAX"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("NetAmount",tablePM["NetAmount"].cast("decimal(19,4)")).withColumn("GrossAmount",tablePM["GrossAmount"].cast("decimal(19,4)"))
            
            tablePM=tablePM.withColumn("Branch_Code",tablePM["Branch_Code"].cast("integer")).withColumn("Sup_Code",tablePM["Sup_Code"].cast("integer"))
            tablePM=tablePM.withColumn("Item_Det_Code",tablePM["Item_Det_Code"].cast("integer")).withColumn("Lot_Code",tablePM["Lot_Code"].cast("integer"))
            tablePM=tablePM.withColumn("vouch_code",tablePM["vouch_code"].cast("integer")).withColumn("fiscalyear",tablePM["fiscalyear"].cast("integer"))
            
            tablePM=tablePM.withColumn("OrderAmount",tablePM["OrderAmount"].cast("double")).withColumn("BasicRate",tablePM["BasicRate"].cast("double"))
            tablePM=tablePM.withColumn("POTRate",tablePM["POTRate"].cast("double"))
            
            
            tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
            
            tablePM = tablePM.drop_duplicates()     ####### 14 MAR
            tablePM.cache()
            tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut/yearmonth="+pastFY+PM)
            
            
            
            
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
            tbname1='Pur_Txn'
            tbname2='Pur_Head'
            tbname1=tbname1+str(presentFY)
            tbname=tbname1                  #
            tbname2=tbname2+str(presentFY)
            
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
            tables = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                    POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code,
                    (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                    PUT.Tot_Qty AS PurchaseQty,(POT.Tot_Qty * POT.Rate) As OrderAmount, 
                    (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                    (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                    (PUT.Tot_Qty/(CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END))*100 AS FillRateQty, --26Mar
                    PUT.Calc_Tax_3 AS TAX, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TotalTAX, 
                    PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount,PUT.vouch_code
                    FROM Po_Txn AS POT LEFT JOIN """+tbname1+""" AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                    AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                    LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                    INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code WHERE MONTH(PUH.Vouch_Date) = """+CM+") AS data1"
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(presentFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.PoOrderDate)))
            
            ######################-PLAIN VALUE DICT ERROR May4#########################
            table=table.withColumn("PoOrderNo",table["PoOrderNo"].cast("string")).withColumn("BillNo",table["BillNo"].cast("string"))
            table=table.withColumn("EntityName",table["EntityName"].cast("string")).withColumn("DBName",table["DBName"].cast("string"))
            table=table.withColumn("yearmonth",table["yearmonth"].cast("string"))
            
            table=table.withColumn("PoOrderDate",table["PoOrderDate"].cast("timestamp")).withColumn("PoValidDate",table["PoValidDate"].cast("timestamp"))
            table=table.withColumn("BillDate",table["BillDate"].cast("timestamp")).withColumn("VoucherDate",table["VoucherDate"].cast("timestamp"))
            
            table=table.withColumn("GrnNo",table["GrnNo"].cast("decimal(19,4)")).withColumn("POQty",table["POQty"].cast("decimal(19,4)"))
            table=table.withColumn("PurchaseQty",table["PurchaseQty"].cast("decimal(19,4)")).withColumn("CancelPOQty",table["CancelPOQty"].cast("decimal(19,4)"))
            table=table.withColumn("PendingPOQty",table["PendingPOQty"].cast("decimal(19,4)")).withColumn("FillRateQty",table["FillRateQty"].cast("decimal(19,4)"))
            table=table.withColumn("TAX",table["TAX"].cast("decimal(19,4)")).withColumn("TotalTAX",table["TotalTAX"].cast("decimal(19,4)"))
            table=table.withColumn("NetAmount",table["NetAmount"].cast("decimal(19,4)")).withColumn("GrossAmount",table["GrossAmount"].cast("decimal(19,4)"))
            
            table=table.withColumn("Branch_Code",table["Branch_Code"].cast("integer")).withColumn("Sup_Code",table["Sup_Code"].cast("integer"))
            table=table.withColumn("Item_Det_Code",table["Item_Det_Code"].cast("integer")).withColumn("Lot_Code",table["Lot_Code"].cast("integer"))
            table=table.withColumn("vouch_code",table["vouch_code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))
            
            table=table.withColumn("OrderAmount",table["OrderAmount"].cast("double")).withColumn("BasicRate",table["BasicRate"].cast("double"))
            table=table.withColumn("POTRate",table["POTRate"].cast("double"))
            
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table = table.drop_duplicates()     ####### 14 MAR
            
            table.cache()
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut/yearmonth="+presentFY+CM)
            
            print("SUCCESSSSSSSSSSSSSSSSSSSSS RAN TWO MONTHS INCREMENTAL")
            cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut").count()
            Tcount=tablePM.count()+table.count()
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'IngPOAnalysis','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(Tcount),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
            log_df = sqlctx.createDataFrame(log_dict,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
            


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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'IngPOAnalysis','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
