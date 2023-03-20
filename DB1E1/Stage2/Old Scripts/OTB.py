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

if present.month==3:
    rollD=28
else:
    rollD=30

previous=present-dateutil.relativedelta.relativedelta(months=1)   ############
previous = previous.strftime('%Y-%m-%d')
previous = previous[:-2]
previous=previous+"01"
previous=datetime.datetime.strptime(previous,"%Y-%m-%d")  ######## PREVIOUS MONTH DAY 1

PM=previous.month  ## PM = PAST MONTH/ PREVIOUS MONTH
PM = str(PM)

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
    ageing=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/StockSnapshot/linkyearmth=20201")
        
    ##################### NEED TO WRITE THE TWO PARTIOTINS DATA IN APPEND MODE IN SQL
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot", mode="append")
    
    
    
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
        
        cond = [ageing.Date_Diff == TempBuckets.NOD]
        
        ageing=ageing.join(TempBuckets,cond,'left')
        
        ############################## REQUIREMENT PRINCE########################
        
        ageing=ageing.withColumn("linkyearmth",concat(year(ageing.Link_date),month(ageing.Link_date)))
        
        ageing.cache()
        ageing.write.partitionBy("linkyearmth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/StockSnapshot")
        
        
    else:
        rec2Last = records.collect()[-2]["Link_date"]
                
        rec2Last = [{"Link_date":rec2Last}]
        rec2Last= spark.createDataFrame(rec2Last)
        
        records = rec2Last
        
    
        cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate1"]>records.Link_date)]
        ageing = stock.join(records,cond,"inner")
        
        lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
        lot = lot.withColumnRenamed("Lot_Code","Code") 
        cond= [(ageing["Lot_Code"]==lot.Code)]
        ageing = ageing.join(lot,cond,"left")
        
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
        
        cond = [ageing.Date_Diff == TempBuckets.NOD]
        
        ageing=ageing.join(TempBuckets,cond,'left')
        
        ############################## REQUIREMENT PRINCE########################
        ageing.cache()
        '''
        IMOD = read_data_sql("(Select  Code as Item_O_Det_Code from It_Mst_O_Det  ) as tb")
        IMD = read_data_sql("(Select Item_Hd_Code,Item_O_Det_Code,Item_Det_Code from It_Mst_Det  ) as tb")
        item = IMD.join(IMOD, 'Item_O_Det_Code', how='inner')
        IMH = read_data_sql("(Select Item_Hd_Code,Item_Name_Code  from It_Mst_Hd  ) as tb")
        item = item.join(IMH, 'Item_Hd_Code', how = 'inner')
        IMN = read_data_sql("(Select Item_Name,Item_Name_Code from  It_Mst_Names  ) as tb")
        item = item.join(IMN, 'Item_Name_Code', how = 'inner')
        item = item.drop('Item_Name_Code','Item_Hd_Code','Item_O_Det_Code')
        item.show()
        
        
        '''
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
   
