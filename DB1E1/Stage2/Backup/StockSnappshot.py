#Snapshot of Stock Query script
#Dependent on Stage1     7.2_IngStk_Dtxn
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
from pyspark.sql.types import DateType
#import pandas as pd
#from sqlserverport import lookup
#from Tools.scripts.objgraph import ignore


now = datetime.datetime.now()
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

# pastStr="'"+past+"'"
# print("baal"+past)
# print(PM) 
# print(pastFY)
# print(present)
# print(CM)
# print(presentFY)
# exit()

#dquery =  """DELETE FROM market99.StockAllYearSnapshot WHERE "Date_" > """+pastStr

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
        
    conf = SparkConf().setMaster(smaster).setAppName("StockSnapshot")\
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
    spark = SparkSession.builder.appName("StockSnapshot").config("spark.network.timeout", "100000001")\
    .config("spark.executor.heartbeatInterval", "100000000").getOrCreate()
    sqlctx = SQLContext(sc)
    
    Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    
    Postgresurl2 = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
    Postgresprop2 = {
        "user":"postgres",
        "password":"sa123",
        "driver": "org.postgresql.Driver" 
    }
    
    Postgresurl = "jdbc:postgresql://103.248.60.5:5432/kockpit"
    Postgresprop= {
        "user":"postgres",
        "password":"sa@123",
        "driver": "org.postgresql.Driver" 
    }
    ## PASSWORD Changed to sa123 for LINUX Postgres
    
    
        
    def last_day_of_month(date):
        #print("date: ",date)
        #if date.day <=15:
            #print("NewDate: ",date.replace(day=15))
        #    return date.replace(day=15)        
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)  
    
    '''table1'''  
    ''' 
    stock="(SELECT *  FROM market99.stockallyear ) AS doc"
    stock=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=stock,
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    '''
    #stock=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/StockAllYear")
    
    ######################## MATERIALS OF Stk_Concat SCRIPT #################  MAR 2 20
    stkTable = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn2")
    #stkTable=stkTable.filter(stkTable.branch_code=='5007').filter(stkTable.lot_code=='3067760')
    #stkTable.show(20)
    #exit()
    
    stk_dtxn=stkTable
    #stk_dtxn.show()
    
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Stk_Concat")
    cols.cache()
    col_sp = cols.filter(cols.Table_Name=="stk_dtxn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    stk_dtxn = stk_dtxn.select(col_sp)
    table1=stk_dtxn
    
    #table1=table1.filter(table1.Lot_Code!=0)                 ## 30 Mar Plain Value Dict
    #table1=table1.filter(table1["Lot_Code"].isNotNull())        ## 30 Mar Plain Value Dict
    
    table1 = table1.filter(table1.Godown_Code ==0)
    #table1.write.mode("overwrite").save(hdfspath+"/"+DBET+"/Stage2/StockAllYear")
    stock = table1
    
    stock = stock.filter(stock['Net_Qty']!=0)
    stock.cache()
    #stock=stock.withColumn('Lot_Code',stock['Lot_Code'].cast('string'))   ## 30 Mar Plain Value Dict
    #stock.show()
    ##stock=stock.withColumn("Date_",stock["Date_"].cast(DateType()))            ## 30 Mar Plain Value Dict
    ##stock=stock.withColumn("Net_Qty",stock["Net_Qty"].cast("decimal(19,4)"))   ## 30 Mar Plain Value Dict
    #stock.printSchema()
    #print(stock.dtypes)
    #stockGrp = stock.groupBy("Lot_Code").agg({"Net_Qty":"sum"},{"Date_":"max"})#agg(max_("Date_"),sum_("Net_Qty"))
    stockGrp = stock.groupBy("Lot_Code").agg(max_("Date_"),sum_("Net_Qty"))
    #stockGrp.show()
    #stockGrp = stock.groupBy("Lot_Code").max("Date_").sum("Net_Qty")
    
    ##########both stock & stockGrp are ok till now
    
    #stockGrp = stockGrp.withColumnRenamed("Lot_Code","Grp_Lot_Code").withColumnRenamed("sum(Net_Qty)","Grp_Net_Qty")
    stockGrp = stockGrp.withColumnRenamed("Lot_Code","GrpLotCode").withColumnRenamed("sum(Net_Qty)","Grp_Net_Qty")
    #stockGrp=stockGrp.withColumn('GrpLotCode',stockGrp['GrpLotCode'].cast('string'))   ## 30 Mar Plain Value Dict
    
    #stockGrp.toPandas().to_csv("/home/hadoopusr/KOCKPIT/DB1E1/Stage2/Backup/stockGrp")
    
    #grp=pd.read_csv("/home/hadoopusr/KOCKPIT/DB1E1/Stage2/Backup/stockGrp")
    #print(grp)
    
    
    #stockGrp=spark.createDataFrame(grp)
    #stockGrp.show()
    #exit()
    '''
    stock.show()
    stock.printSchema()
    stockGrp.show()
    stockGrp.printSchema()
    exit()
    '''
    #sys.exit()
    #stockGrp.cache()
    #cond= [(stock["Lot_Code"]==stockGrp.Grp_Lot_Code)]
    cond= [(stock.Lot_Code==stockGrp.GrpLotCode)]
    
    stock = stock.join(stockGrp,cond,"left")
    
    stock = stock.withColumnRenamed("max(Date_)","EndDate")
    #stock = stock.withColumn("Today",lit(now))
    stock=stock.withColumn("EndDate1", \
                  when(stock["Grp_Net_Qty"] == 0,stock["EndDate"] ).otherwise(now))#reverts the changes to the last date which was previouslt showing last month date of last Date
    stock=stock.drop("Grp_Net_Qty").drop("EndDate")                             ############# MARCH 14
    stock.cache()
    
    #stock.persist(StorageLevel.MEMORY_AND_DISK)
    #stock.show()
    date = stock.select("Date_")#.filter(stock["Date_"]!= '')
    
    date.cache()
    Calendar_Startdate = str(date.agg({"Date_": "min"}).collect()[0][0]).split()[0]
    Calendar_Enddate = str(date.agg({"Date_": "max"}).collect()[0][0]).split()[0]

    #Calendar_Startdate = datetime.datetime.strptime(Calendar_Startdate, '%Y-%m-%d').date()
    Calendar_Enddate = datetime.datetime.strptime(Calendar_Enddate, '%Y-%m-%d').date()
    Calendar_Startdate = Calendar_Enddate - relativedelta.relativedelta(months=6)
    
    print("Calendar_Startdate")
    print(Calendar_Startdate)
    print("Calendar_Enddate")
    print(Calendar_Enddate)
    #stock.show()
    #exit()
    data =[]
    for single_date in daterange(Calendar_Startdate, Calendar_Enddate):
        data.append({'Link_date':last_day_of_month(single_date)})
   
    
    schema = StructType([
        StructField("Link_date", DateType(),True)
    ])
    
    records=spark.createDataFrame(data,schema)#recordes every date btw the startDate and EndDate
    #print("before records")
    #records.persist(StorageLevel.MEMORY_AND_DISK)
    #records.show()
    records=records.select('Link_date').distinct().sort('Link_date')#Contains Last Date of the month for every date btw the range
    #print("after records")
    #records.unpersist()
    
    records=records.withColumn("Link_date", \
                  when(records["Link_date"] == last_day_of_month(Calendar_Enddate), Calendar_Enddate).otherwise(records["Link_date"]))#reverts the changes to the last date which was previouslt showing last month date of last Date
    
    records.cache()
    
    ##########################
    ###########################
    
    
    try:
        #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
        Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
        #con = pyodbc.connect(connection_string)          
        squery = "(SELECT DISTINCT Link_date FROM StockSnapshot) As Data"
        table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=squery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        
        if table.count()>=6:
    
            print("RUN INCREMENTAL")
            ######################## MAR 4  TAKE LAST TWO RECORDS ##########
            print("LAST TWO MONTHS")
            recLast = records.collect()[-1]["Link_date"]
            recyr = str(recLast.year)
            recyrD = recLast.year
            recmth = str(recLast.month)
            recmthD = recLast.month
            
            ############# 2ND LAST RECORD
            rec2Last = records.collect()[-2]["Link_date"]
            rec2yr = str(rec2Last.year)
            rec2yrD = rec2Last.year
            rec2mth = str(rec2Last.month)
            rec2mthD = rec2Last.month
            
            recLast = [{"Link_date":recLast}]
            recLast = spark.createDataFrame(recLast)
            
            rec2Last = [{"Link_date":rec2Last}]
            rec2Last= spark.createDataFrame(rec2Last)
            
            records = rec2Last.unionByName(recLast)
            
            
            cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate1"]>records.Link_date)]
            #print("joint")
            ageing = stock.join(records,cond,"inner")
            
            '''
            lot='(SELECT *  FROM market99.lot ) AS doc'
            lot=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=lot,\
                      user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
            '''
            lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
            #hdfspath+"/"+DBET+"/Stage2/Lot"
            lot = lot.withColumnRenamed("Lot_Code","Code")
            
             
            cond= [(ageing["Lot_Code"]==lot.Code)]
            #print("joint")
            
            ageing = ageing.join(lot,cond,"left")
            #print(type(ageing))
            #sys.exit()
            
            
            ageing=ageing.withColumn("Date_Diff", \
                          when(ageing["pur_date"].isNull(), 0).otherwise(
                              
                              datediff(ageing.Link_date,
                               to_date("pur_date","yyyy/MM/dd"))
                              ))#reverts the changes to the last date which was previouslt showing last month date of last Date
            
            
            ageing.cache()
            
            list=ageing.select('Date_Diff').distinct().collect()
        
            NoOfRows=len(list)
        
            data1=[]
             
            ########Bucket########
            #Bucket = spark.read.csv("E:\Market 99\StockSnapshotBucket.csv",header=True)
            Bucket = spark.read.csv(path+"/Config/StockSnapshotBucket.csv",header=True)
            
            
            Bucket = Bucket.collect()
        
            NoOfBuckets=len(Bucket)
            for i in range(0,NoOfRows):
                n=list[i].Date_Diff
                for j in range(0, NoOfBuckets):
                    if int(Bucket[j].Lower_Limit) <= n <= int(Bucket[j].Upper_Limit):
                        data1.append({'Interval':Bucket[j].Bucket_Name,'Bucket_Sort':Bucket[j].Bucket_Sort,'NOD':n})
                        break
        
            TempBuckets=spark.createDataFrame(data1).distinct()
            #TempBuckets.persist(StorageLevel.MEMORY_AND_DISK)
            #ageing.unpersist()
            #ageing.persist(StorageLevel.MEMORY_AND_DISK)
            cond = [ageing.Date_Diff == TempBuckets.NOD]
            #ageing.unpersist()
            #TempBuckets.unpersist()
            ageing=ageing.join(TempBuckets,cond,'left')
            #ageing.persist(StorageLevel.MEMORY_AND_DISK)
            ############################## REQUIREMENT PRINCE########################
            
            #ageing=ageing.withColumn("linkyearmth",concat(year(ageing.Link_date),month(ageing.Link_date)))
            
            
            ageing.cache()
            
            Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
            
            ###############DELETE FROM SQL
            
            connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
            con= pyodbc.connect(connection_string)
        #             database="kockpit", user="sa", password = "M99@321", \
        #                           host = "sqlserver://MARKET-NINETY3\MARKET99", port = "5432")
            #dquery =  """DELETE FROM market99.StockAllYearSnapshot WHERE "Date_" > """+past
            #dquery =  "DELETE FROM StockSnapshot WHERE YEAR(Link_date)="+rec2yr+" AND MONTH(Link_date)="+rec2mth
            #dquery2 =  "DELETE FROM StockSnapshot WHERE YEAR(Link_date)="+recyr+" AND MONTH(Link_date)="+recmth
            dquery3 = "DELETE FROM StockSnapshot WHERE Link_date in (SELECT DISTINCT TOP(2) Link_date FROM StockSnapshot ORDER BY Link_date desc)"
            cur=con.cursor()#491
            #cur.execute(dquery)
            #cur.execute(dquery2)
            cur.execute(dquery3)
            con.commit()
            con.close
            
            ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot", mode="append")#497
            print("SUCCESSFULLY RAN INCREMENTAL")
            
    
        else:
            print("FULL RELOAD")
            
            cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate1"]>records.Link_date)]
            #print("joint")
            ageing = stock.join(records,cond,"inner")
            
            
            
            
            lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
            #hdfspath+"/"+DBET+"/Stage2/Lot"
            lot = lot.withColumnRenamed("Lot_Code","Code") 
            cond= [(ageing["Lot_Code"]==lot.Code)]
            #print("joint")
            ageing = ageing.join(lot,cond,"left")
            #ageing.unpersist()
            
            ageing=ageing.withColumn("Date_Diff", \
                          when(ageing["pur_date"].isNull(), 0).otherwise(
                              
                              datediff(ageing.Link_date,
                               to_date("pur_date","yyyy/MM/dd"))
                              ))#reverts the changes to the last date which was previouslt showing last month date of last Date
            
            
            ageing.cache()
            list=ageing.select('Date_Diff').distinct().collect()
        
            NoOfRows=len(list)
        
            data1=[]
             
            ########Bucket########
            #Bucket = spark.read.csv("E:\Market 99\StockSnapshotBucket.csv",header=True)
            Bucket = spark.read.csv(path+"/Config/StockSnapshotBucket.csv",header=True)
            
            Bucket = Bucket.collect()
        
            NoOfBuckets=len(Bucket)
            for i in range(0,NoOfRows):
                n=list[i].Date_Diff
                for j in range(0, NoOfBuckets):
                    if int(Bucket[j].Lower_Limit) <= n <= int(Bucket[j].Upper_Limit):
                        data1.append({'Interval':Bucket[j].Bucket_Name,'Bucket_Sort':Bucket[j].Bucket_Sort,'NOD':n})
                        break
        
            TempBuckets=spark.createDataFrame(data1).distinct()
            #TempBuckets.persist(StorageLevel.MEMORY_AND_DISK)
            #ageing.unpersist()
            #ageing.persist(StorageLevel.MEMORY_AND_DISK)
            cond = [ageing.Date_Diff == TempBuckets.NOD]
            #ageing.unpersist()
            #TempBuckets.unpersist()
            ageing=ageing.join(TempBuckets,cond,'left')
            #ageing.persist(StorageLevel.MEMORY_AND_DISK)
            
            
            #ageing=ageing.withColumn("linkyearmth",concat(year(ageing.Link_date),month(ageing.Link_date)))
            #ageing.write.partitionBy("linkyearmth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot")
            Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
            ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot", mode="overwrite")
            print("SUCCESSFULLY RAN FULL RELOAD")
            
        
    except Exception as ex:
        print("FULL RELOAD")
        
        records =records
        cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate1"]>records.Link_date)]
        #print("joint")
        ageing = stock.join(records,cond,"inner")
        
        
        
        lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
        #hdfspath+"/"+DBET+"/Stage2/Lot"
        lot = lot.withColumnRenamed("Lot_Code","Code") 
        cond= [(ageing["Lot_Code"]==lot.Code)]
        #print("joint")
        ageing = ageing.join(lot,cond,"left")
        #ageing.unpersist()
        
        ageing=ageing.withColumn("Date_Diff", \
                      when(ageing["pur_date"].isNull(), 0).otherwise(
                          
                          datediff(ageing.Link_date,
                           to_date("pur_date","yyyy/MM/dd"))
                          ))#reverts the changes to the last date which was previouslt showing last month date of last Date
        
        
        ageing.cache()
        list=ageing.select('Date_Diff').distinct().collect()
    
        NoOfRows=len(list)
    
        data1=[]
         
        ########Bucket########
        #Bucket = spark.read.csv("E:\Market 99\StockSnapshotBucket.csv",header=True)
        Bucket = spark.read.csv(path+"/Config/StockSnapshotBucket.csv",header=True)
        
        Bucket = Bucket.collect()
    
        NoOfBuckets=len(Bucket)
        for i in range(0,NoOfRows):
            n=list[i].Date_Diff
            for j in range(0, NoOfBuckets):
                if int(Bucket[j].Lower_Limit) <= n <= int(Bucket[j].Upper_Limit):
                    data1.append({'Interval':Bucket[j].Bucket_Name,'Bucket_Sort':Bucket[j].Bucket_Sort,'NOD':n})
                    break
    
        TempBuckets=spark.createDataFrame(data1).distinct()
        #TempBuckets.persist(StorageLevel.MEMORY_AND_DISK)
        #ageing.unpersist()
        #ageing.persist(StorageLevel.MEMORY_AND_DISK)
        cond = [ageing.Date_Diff == TempBuckets.NOD]
        #ageing.unpersist()
        #TempBuckets.unpersist()
        ageing=ageing.join(TempBuckets,cond,'left')
        #ageing.persist(StorageLevel.MEMORY_AND_DISK)
        ############################## REQUIREMENT PRINCE########################
        
        Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
        ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot", mode="overwrite")
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
   
