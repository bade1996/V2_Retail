#Snapshot of Stock Query script
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,col,month,year,lit,concat,max as max_,min as min_,sum as sum_,datediff,to_date,col,when
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
#from pyspark.sql.concatenate import *
import pandas as pd
from numpy.core._multiarray_umath import empty
from dateutil import relativedelta
from pyspark.storagelevel import StorageLevel   
import psycopg2
import dateutil.relativedelta
import pyspark.sql.functions as F
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
        
    conf = SparkConf().setMaster(smaster).setAppName("StockConSnapshot")\
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
    spark = SparkSession.builder.appName("StockConSnapshot").config("spark.network.timeout", "100000001")\
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
    '''
    #Kamal Sir
    Postgresurl2 = "jdbc:postgresql://103.248.60.14:5432/kockpit"
    Postgresprop2= {
        "user":"postgres",
        "password":"sa123",
        "batchsize":"1000000", 
        "driver": "org.postgresql.Driver" 
    }
    '''
    '''
    Postgresurl2 = "jdbc:postgresql://103.248.60.14:5432/kockpit"
    Postgresprop2= {
        "user":"postgres",
        "password":"sa123",
        "driver": "org.postgresql.Driver" 
    }
    '''
    '''
    records=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Dump")
    print("SOURCE")
    records.show()
#     records = records.where(F.col('index')>records.count()-2).show()
    recLast = records.collect()[-1]["Link_date"]
    print(recLast)
    rec2Last = records.collect()[-2]["Link_date"]
    rec2yr = datetime.datetime.year(rec2Last)
    print(rec2yr)
    print(type(rec2Last))
    recLast = [{"Link_date":recLast}]
    recLast = spark.createDataFrame(recLast)
    rec2Last = [{"Link_date":rec2Last}]
    rec2Last= spark.createDataFrame(rec2Last)
    
    records = rec2Last.unionByName(recLast)
    records.show()
    exit()
    '''
    '''
    ############################### TESTING GROUND ######################
#     AccDt = sqlctx.read.parquet("hdfs://10.4.0.113:9000/KOCKPIT/Data/AccessDetails").show()
#     exit()

    
    try:
        connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
    
        con = pyodbc.connect(connection_string)          
    
        squery = "(Select * from [kockpit].[dbo].[branch])"
    #squery = "SELECT 0 AS FLAG"      
        cur=con.cursor()
        y=cur.execute(squery)
        #for i in y:
        #    y=i
        #    y=i[0]
        #print("y",y)
        x = pd.read_sql_query(squery,con)
        x = spark.createDataFrame(x)
        x.show(4000,False)
        exit()
        
    except Exception as ex:
        print("Not Available",ex)
    exit()
    '''
#     x = "Select TOP(50) * from [item]"
#     y=cur.execute(x)
#     
#     print(y)
#     con.close()
#     exit()
    
    #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99;Database=kockpit;uid=sa;pwd=M99@321'
    #con = pyodbc.connect(connection_string)
    #cur=con.cursor()
    #x = "Select TOP(50) * from [item]"
    ### Joga 
#     df= pd.read_sql(x, con)
#     print(df)
#     sys.exit()   
 
#     con= pypyodbc.connect(connection_string2,timeout=30000000)
#     cur=con.cursor()
#     x = "Select TOP(50) * from [item]"
#     cur.execute(x)

#             database="kockpit", user="sa", password = "M99@321", \
#                           host = "sqlserver://MARKET-NINETY3\MARKET99", port = "5432")
        
        #dquery =  """DELETE FROM market99.StockAllYearSnapshot WHERE "Date_" > """+past
    #dquery =  "DELETE FROM StockSnapshot WHERE YEAR(Link_date)="+rec2yr+" AND MONTH(Link_date)="+rec2mth
    #dquery2 = "DELETE FROM StockSnapshot WHERE YEAR(Link_date)="+recyr+" AND MONTH(Link_date)="+recmth
    
#     cur=con.cursor()
#     cur.execute(dquery)
#     cur.execute(dquery2)
#     
#     con.commit()
#     con.close
#     exit()
    
    
    
    records=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Dump")
#     Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
#     records.write.jdbc(url=Sqlurlwrite, table="Testing", mode="append")
#     exit()   
    
    print("SOURCE")
    records.show()
#     records = records.where(F.col('index')>records.count()-2).show()
    recLast = records.collect()[-1]["Link_date"]
    print(recLast)
    rec2Last = records.collect()[-2]["Link_date"]
    #rec2yr = datetime.datetime.year(rec2Last)
    rec2yr = str(rec2Last.year)
    rec2mth = str(rec2Last.month)
    print(rec2yr)
    print(rec2mth)
    dquery =  "DELETE FROM StockSnapshot WHERE YEAR(Link_date)="+rec2yr+" AND MONTH(Link_date)="+rec2mth
    print(dquery)
    cur=con.cursor()
    cur.execute(dquery)
    #cur.execute(dquery2)
    
    con.commit()
    con.close
    
    exit()
    print(type(rec2Last))
    recLast = [{"Link_date":recLast}]
    recLast = spark.createDataFrame(recLast)
    rec2Last = [{"Link_date":rec2Last}]
    rec2Last= spark.createDataFrame(rec2Last)
    
    records = rec2Last.unionByName(recLast)
    records.show()
    exit()
    
    ############################### TESTING GROUND ENDS ######################
    '''
    ageing=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/linkyearmth=20201")
        
    ##################### NEED TO WRITE THE TWO PARTIOTINS DATA IN APPEND MODE IN SQL
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot", mode="append")
    
    
#     ageing2=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/linkyearmth=20199")
#         
#     ##################### NEED TO WRITE THE TWO PARTIOTINS DATA IN APPEND MODE IN SQL
#     Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
#     ageing2.write.jdbc(url=Sqlurlwrite, table="StockSnapshot", mode="append")
    
    exit()
    
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
    stkTable = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn")

    stk_dtxn=stkTable
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Stk_Concat")
    col_sp = cols.filter(cols.Table_Name=="stk_dtxn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    stk_dtxn = stk_dtxn.select(col_sp)
    table1=stk_dtxn
    table1 = table1.filter(table1.Godown_Code ==0)
    table1.cache()
    #table1.write.mode("overwrite").save(hdfspath+"/"+DBET+"/Stage2/StockAllYear")
    stock = table1
    
    stock = stock.filter(stock['Net_Qty']!=0)
    #stock.cache()    
    stockGrp = stock.groupBy("Lot_Code").agg(max_("Date_"),sum_("Net_Qty"))
    
    stockGrp = stockGrp.withColumnRenamed("Lot_Code","Grp_Lot_Code").withColumnRenamed("sum(Net_Qty)","Grp_Net_Qty")
    #stockGrp.cache()
    cond= [(stock["Lot_Code"]==stockGrp.Grp_Lot_Code)]
    stock = stock.join(stockGrp,cond,"left")
    
    stock = stock.withColumnRenamed("max(Date_)","EndDate")
    #stock = stock.withColumn("Today",lit(now))
    stock=stock.withColumn("EndDate1", \
                  when(stock["Grp_Net_Qty"] == 0,stock["EndDate"] ).otherwise(now))#reverts the changes to the last date which was previouslt showing last month date of last Date
    #stock.show()
    #exit()
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
    
    
    if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage2/StockSnapshot45")==256:
        records =records
        
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
        '''
        item = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Item")
        item=item.select(item.Item_Det_Code,item.Pur_Rate,item.Sale_Rate)
        
        cond1= [ageing.Item_Det_Code == item.Item_Det_Code]
        ageing = ageing.join(item,cond1,'left')
        ageing = ageing.withColumn("Closing_Stock_CP",lit())
        '''
        ageing=ageing.withColumn("linkyearmth",concat(year(ageing.Link_date),month(ageing.Link_date)))
        
        ageing.cache()
        ageing.write.partitionBy("linkyearmth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot")
        
        
    else:
        ######################## MAR 4  TAKE LAST TWO RECORDS ##########
        #recLast = records.collect()[-1]["Link_date"]
        
        ############# 2ND LAST RECORD
        rec2Last = records.collect()[-2]["Link_date"]
        
        #recLast = [{"Link_date":recLast}]
        #recLast = spark.createDataFrame(recLast)
        
        rec2Last = [{"Link_date":rec2Last}]
        rec2Last= spark.createDataFrame(rec2Last)
        
        #records = rec2Last.unionByName(recLast)
        records = rec2Last
        
    
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
        '''
        item = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Item")
        item=item.select(item.Item_Det_Code,item.Pur_Rate,item.Sale_Rate)
        
        cond1= [ageing.Item_Det_Code == item.Item_Det_Code]
        ageing = ageing.join(item,cond1,'left')
        ageing = ageing.withColumn("Closing_Stock_CP",lit())
        '''
        ageing.cache()
        
        #ageing.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/linkyearmth="+)
        
        
    
 
 
 
    ########################GARBAGE
    '''
    if pastFY==presentFY:
        stkTable = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn/yearmonth="+pastFY+PM)
        ageing=StockSnapshot(stkTable)
        ageing.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/yearmonth="+pastFY+PM)
        
        stkTable = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn/yearmonth="+pastFY+CM)
        ageing=StockSnapshot(stkTable)
        ageing.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/yearmonth="+pastFY+CM)
        
    else:
        stkTable = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn/yearmonth="+pastFY+PM)
        ageing=StockSnapshot(stkTable)
        ageing.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/yearmonth="+pastFY+PM)
        
        stkTable = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn/yearmonth="+presentFY+CM)
        ageing=StockSnapshot(stkTable)
        ageing.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/yearmonth="+presentFY+CM)
    '''
    
    ####save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot")
    
    
    #print(ageing.rdd.getNumPartitions())
    #ageing.show()
    #print(start_time)
    #exit()
    #stock.write.jdbc(url=Postgresurl2, table="market99"+".test", mode="overwrite", properties=Postgresprop2)
    #Stk_All_query = "DROP TABLE IF EXISTS StockAllYearSnapshot;"
    '''
    Stk_All_query = "DELETE FROM StockAllYearSnapshot; ALTER TABLE StockAllYearSnapshot SET UNLOGGED;"
    #Stk_All_query = "select count(*) from market99.stockallyearsnapshot;"
    con= psycopg2.connect(database="kockpit", user="postgres", password = "sa123", \
                          host = "103.248.60.14", port = "5432")
    cur=con.cursor()
    cur.execute(Stk_All_query)
    con.commit()
    con.close
    #print(cur.fetchall())
    '''
    '''
    ### Soln by K Sir
    ageing.limit(1).write.jdbc(url=Postgresurl2, table="market99"+".test", mode="overwrite", properties=Postgresprop2)
    
    ## added new below
    Stk_All_query = "DELETE FROM market99.test; ALTER TABLE market99.test SET UNLOGGED;"
    #Stk_All_query = "select count(*) from market99.stockallyearsnapshot;"
    con= psycopg2.connect(database="kockpit", user="postgres", password = "sa123", \
                          host = "103.248.60.14", port = "5432")
    cur=con.cursor()
    cur.execute(Stk_All_query)
    con.commit()
    con.close
    #print(cur.fetchall())
    ageing.limit(2000000000).write.jdbc(url=Postgresurl2, table="market99"+".test", mode="append",properties=Postgresprop2)
    '''
    '''
    ############## DELETE DATA IN POSTGRES > PAST DATE
    con= psycopg2.connect(database="kockpit", user="postgres", password = "sa@123", \
                          host = "103.248.60.5", port = "5432")
    #dquery =  """DELETE FROM market99.StockAllYearSnapshot WHERE "Date_" > """+past
    dquery =  """DELETE FROM market99.StockAllYearSnapshot WHERE "Date_" >= """+pastStr
    cur=con.cursor()
    cur.execute(dquery)
    con.commit()
    con.close
    '''
    #ageing.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot")
    
    #ageing.write.jdbc(url=Postgresurl2, table="market99"+".StockAllYearSnapshot", mode="overwrite", properties=Postgresprop2)  #LINUX
    
    #ageing.write.jdbc(url=Postgresurl, table="market99"+".StockAllYearSnapshot", mode="overwrite", properties=Postgresprop)  #WINDOWS  OVERWRITE THISONE
    #ageing.write.jdbc(url=Postgresurl, table="market99"+".StockAllYearSnapshot", mode="append", properties=Postgresprop)  #WINDOWS  APPEND
    
    #print("QWerty")
    print("SUCCESSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
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
   
