#PO Analysis Query script   JOIN OF 4 TABLES
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,col,month,year,lit,concat,max as max_,min as min_,sum as sum_,datediff,to_date,col,when
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
import udl
import pandas as pd
from numpy.core._multiarray_umath import empty
from dateutil import relativedelta
from pyspark.storagelevel import StorageLevel   
import psycopg2
import dateutil.relativedelta
import pyspark.sql.functions as F
import pyodbc
#import pandas as pd    
#from sqlserverport import lookup
#from Tools.scripts.objgraph import ignore


now = datetime.datetime.now()
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
    path = config = config[0:config.rfind("DB")]
    path = "file://"+path
    config = pd.read_csv(config+"/Config/conf.csv")

    for i in range(0,len(config)):
        exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
        
    conf = SparkConf().setMaster(smaster).setAppName("POAnalysisS2")\
           .set("spark.sql.shuffle.partitions",60)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        #.set("spark.executor.cores",23)
        #.set("spark.driver.maxResultSize","4g")
#        .set("spark.driver.cores",4)\
#         .set("spark.sql.debug.maxToStringFields", 9000)\
#         .set("spark.network.timeout", "6000s")

    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("POAnalysisS2").config("spark.network.timeout", "100000001")\
    .config("spark.executor.heartbeatInterval", "100000000").getOrCreate()
    sqlctx = SQLContext(sc)
    
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    connection_string="Driver="+ODBC13+";Server="+SURLCUR+","+PORTCUR+";Database="+WDB+";uid="+RUSER+";pwd="+RPASS
    
    '''
    tables = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code, --JOINCond
                (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                (POT.Tot_Qty * POT.Rate) As OrderAmount,
                (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                PUT.Calc_Tax_3 AS Tax3, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TAX, 
                PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount
                FROM Po_Txn AS POT LEFT JOIN Pur_Txn20192020 AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                LEFT JOIN Pur_Head20192020 AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code) AS data1"""
                
                    ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                    #print(tables)
    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
    table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
    table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
    table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
    table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
    ############FISCAL YAER ADDED
    table = table.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
    ######## YAER MONTH FLAG ADD
    table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.VoucherDate)))
    
    table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
    
    table = table.drop_duplicates()     ####### 14 MAR
    
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    table.write.jdbc(url=Sqlurlwrite, table="PurchaseOrderAlys", mode="overwrite")
    
    print("POAnalysis_SUCCESSFUUUUUUUUULLLLLLLLLLLLLLLLLLLLLLL")
    '''
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    try:
        cntQuery = "(SELECT COUNT(*) As kalam FROM PurchaseOrderAlys) As Data"
        cnt = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=cntQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        cnt = cnt.collect()[0]['kalam']
        bcnt=cnt
        print("BEFORE ETL ROW COUNT ",cnt)
        #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
        #Sqlkock="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
        #con = pyodbc.connect(connection_string)          
        squery = "(SELECT COUNT(*) AS cock FROM PurchaseOrderAlys ) As Data"
        table = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=squery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        #table.show()
        value = table.collect()[0]["cock"]
        print(value)
        if value>700000:
            print("RUN INCREMENTAL")
            ######################## MAR 4  TAKE LAST TWO RECORDS ##########
            print("LAST TWO MONTHS")
            connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
            con= pyodbc.connect(connection_string)
            #dquery3 = "DELETE FROM PurchaseOrderAlys WHERE voucherdate in (SELECT DISTINCT TOP(2) Link_date FROM StockSnapshot ORDER BY Link_date desc)"
            #dquery2 =  "DELETE FROM StockSnapshot WHERE YEAR(Link_date)="+recyr+" AND MONTH(Link_date)="+recmth
            
            #dquery="DELETE FROM PurchaseOrderAlys WHERE YEAR(voucherdate) ="+pastFY+" AND MONTH(voucherdate) ="+PM
            dquery="DELETE FROM PurchaseOrderAlys WHERE yearmonth = "+pastFY+PM
            dquery1="DELETE FROM PurchaseOrderAlys WHERE yearmonth = "+presentFY+CM
            #print(dquery)
            #print(dquery1)
            
            #dquery1="DELETE FROM PurchaseOrderAlys WHERE YEAR(voucherdate) ="+presentFY+" AND MONTH(voucherdate) ="+CM
            cur=con.cursor()#491
            cur.execute(dquery)
            cur.execute(dquery1)
            con.commit()
            con.close
            
            '''
            poa=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut")
            poa.cache()
            poa1=poa.filter(poa.yearmonth==pastFY+PM)
            poa2=poa.filter(poa.yearmonth==presentFY+CM)
            '''
            
            poa1=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut/yearmonth="+pastFY+PM)
            poa1.write.jdbc(url=Sqlurlwrite, table="PurchaseOrderAlys", mode="append")
            
            if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/PohPotPuhPut/yearmonth="+presentFY+CM)!=256: # 256 Code Defines PARQUET DOES NOT EXIST
                poa2=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut/yearmonth="+presentFY+CM)
                poa2.write.jdbc(url=Sqlurlwrite, table="PurchaseOrderAlys", mode="append")
                
                
            cntQuery = "(SELECT COUNT(*) As kalam FROM PurchaseOrderAlys) As Data"
            cnt = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=cntQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            cnt = cnt.collect()[0]['kalam']
            print("AFTER ETL ROW COUNT ",cnt)
                
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'POAnalysisS2','DB':DB,'EN':Etn,
                         'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(poa1.count()),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
            log_df = sqlctx.createDataFrame(log_dict,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
                
            print("SUCCESSFULLY RAN TWO MONTHS INCREMENTAL")
            
            
        else:
            print("ELSE FULL RELOAD")
            poa=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut")
            poa.write.jdbc(url=Sqlurlwrite, table="PurchaseOrderAlys", mode="overwrite")
            
            cntQuery = "(SELECT COUNT(*) As kalam FROM PurchaseOrderAlys) As Data"
            cnt = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=cntQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            cnt = cnt.collect()[0]['kalam']
            print("AFTER ETL ROW COUNT ",cnt)
            
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'POAnalysisS2','DB':DB,'EN':Etn,
                         'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(poa.count()),'BeforeETLRows':'0','AfterETLRows':str(cnt)}]
            log_df = sqlctx.createDataFrame(log_dict,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
            
            print("SUCCESSFULLY RAN FULL RELOAD")
            
            
    except Exception as ex:
        print("ELSE FULL RELOAD")
        poa=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut")
        poa.write.jdbc(url=Sqlurlwrite, table="PurchaseOrderAlys", mode="overwrite")
        
        cntQuery = "(SELECT COUNT(*) As kalam FROM PurchaseOrderAlys) As Data"
        cnt = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=cntQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        cnt = cnt.collect()[0]['kalam']
        print("AFTER ETL ROW COUNT ",cnt)
        
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'POAnalysisS2','DB':DB,'EN':Etn,
                     'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(poa.count()),'BeforeETLRows':'0','AfterETLRows':str(cnt)}]
        log_df = sqlctx.createDataFrame(log_dict,schema_log)
        log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
        
        print("SUCCESSFULLY RAN FULL RELOAD")


except Exception as ex:
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error",ex)
    print("type - ",str(exc_type))
    print("line "+str(exc_traceback.tb_lineno))
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'POAnalysisS2','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
