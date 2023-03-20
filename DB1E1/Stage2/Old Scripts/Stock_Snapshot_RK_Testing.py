import datetime,time, sys,os
from dateutil import relativedelta
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,max as max_,sum as sum_,when
from pyspark.sql.types import DateType
from pyspark.sql.types import *

now = datetime.datetime.now()

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
        
    conf = SparkConf().setMaster(smaster).setAppName("StockSnapshot24Mth").set("spark.sql.shuffle.partitions",60)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")

    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("StockSnapshot24Mth").config("spark.network.timeout", "100000001")\
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
        
    def last_day_of_month(date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)  
                
    ######################## MATERIALS OF Stk_Concat SCRIPT #################  MAR 2 20
    stk_dtxn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn2")
    #cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Stk_Concat")
    #cols.cache()
    #col_sp = cols.filter(cols.Table_Name=="stk_dtxn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    #stk_dtxn = stk_dtxn.select(col_sp)
    stk_dtxn=stk_dtxn.select("Net_Qty","Txn_Type","Lot_Code","Godown_Code","Branch_Code","Date_","yearmonth","sl_qty","sr_qty")
    stk_dtxn = stk_dtxn.filter(stk_dtxn.Godown_Code ==0)
    stk_dtxn = stk_dtxn.filter(stk_dtxn['Net_Qty']!=0)
    stk_dtxn = stk_dtxn.filter(stk_dtxn['Branch_Code'] == 5010)
    stk_dtxn.cache()
    stock=stk_dtxn
    stockGrp = stock.groupBy("Lot_Code").agg(max_("Date_"),sum_("Net_Qty"))
    stockGrp = stockGrp.withColumnRenamed("Lot_Code","GrpLotCode").withColumnRenamed("sum(Net_Qty)","Grp_Net_Qty")
    cond= [(stock.Lot_Code==stockGrp.GrpLotCode)]
    stock = stock.join(stockGrp,cond,"left")
    stock = stock.withColumnRenamed("max(Date_)","EndDate")
    stock=stock.withColumn("EndDate",when(stock["Grp_Net_Qty"] == 0,stock["EndDate"] ).otherwise(now))
    stock.cache()
    date = stock.select("Date_")    
    date.cache()
    Calendar_Startdate = str(date.agg({"Date_": "min"}).collect()[0][0]).split()[0]
    Calendar_Enddate = str(date.agg({"Date_": "max"}).collect()[0][0]).split()[0]
    Calendar_Enddate = datetime.datetime.strptime(Calendar_Enddate, '%Y-%m-%d').date()
    Calendar_Startdate = Calendar_Enddate - relativedelta.relativedelta(months=24)
    data =[]
    for single_date in daterange(Calendar_Startdate, Calendar_Enddate):
        data.append({'Link_date':last_day_of_month(single_date)})
   
    schema = StructType([
        StructField("Link_date", DateType(),True)
    ])
    
    records=spark.createDataFrame(data,schema)
    records=records.select('Link_date').distinct().sort('Link_date')
    records=records.withColumn("Link_date",when(records["Link_date"] == last_day_of_month(Calendar_Enddate), Calendar_Enddate)\
                               .otherwise(records["Link_date"]))
    records.cache()
    records.show()
    try:
        Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
        squery = "(SELECT DISTINCT Link_date FROM StockSnapshot2) As Data"
        table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=squery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        if table.count()>=24:
            print("jai")
            recLast = records.collect()[-1]["Link_date"]
            rec2Last = records.collect()[-3]["Link_date"]
            recLast = [{"Link_date":recLast}]
            recLast = spark.createDataFrame(recLast)
            recLast.show()
            rec2Last = [{"Link_date":rec2Last}]
            rec2Last= spark.createDataFrame(rec2Last)
            rec2Last.show()
            records = rec2Last.unionByName(recLast)
            records.show()
            cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate"]>=records.Link_date)]
            ageing = stock.join(records,cond,"inner")
            Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
            ageing.write.jdbc(url=Sqlurlwrite, table="test_snap", mode="overwrite")
            exit()
            lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
            lot = lot.withColumnRenamed("Lot_Code","Code")
            cond= [(ageing["Lot_Code"]==lot.Code)]
            ageing = ageing.join(lot,cond,"left")
            ageing=ageing.withColumn("Date_Diff", when(ageing["pur_date"].isNull(), 0).otherwise(datediff(ageing.Link_date,\
                               to_date("pur_date","yyyy/MM/dd"))))
            ageing.cache()
            list=ageing.select('Date_Diff').distinct().collect()
            NoOfRows=len(list)
            data1=[]
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
            ageing.cache()
            Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
            connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
            con= pyodbc.connect(connection_string)
            dquery3 = "DELETE FROM StockSnapshot2 WHERE Link_date in (SELECT DISTINCT TOP(2) Link_date FROM StockSnapshot2 ORDER BY Link_date desc)"
            cur=con.cursor()
            con.commit()
            con.close
            ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot2", mode="append")#497
        else:
            print("jai2")
            cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate1"]>=records.Link_date)]
            ageing = stock.join(records,cond,"inner")
            lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
            lot = lot.withColumnRenamed("Lot_Code","Code") 
            cond= [(ageing["Lot_Code"]==lot.Code)]
            ageing = ageing.join(lot,cond,"left")
            ageing=ageing.withColumn("Date_Diff", when(ageing["pur_date"].isNull(), 0).otherwise(\
                              datediff(ageing.Link_date,to_date("pur_date","yyyy/MM/dd"))))            
            ageing.cache()
            list=ageing.select('Date_Diff').distinct().collect()
            NoOfRows=len(list)
            data1=[]
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
            Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
            ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot2", mode="overwrite")
            print("SUCCESSFULLY RAN FULL RELOAD")        
    except Exception as ex:
        records =records
        cond= [(stock["Date_"]<=records.Link_date) &(stock["EndDate1"]>=records.Link_date)]
        ageing = stock.join(records,cond,"inner")
        lot=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Lot")         
        lot = lot.withColumnRenamed("Lot_Code","Code") 
        cond= [(ageing["Lot_Code"]==lot.Code)]
        ageing = ageing.join(lot,cond,"left")
        ageing=ageing.withColumn("Date_Diff",when(ageing["pur_date"].isNull(), 0).otherwise(datediff(ageing.Link_date,\
                           to_date("pur_date","yyyy/MM/dd"))))
        ageing.cache()
        list=ageing.select('Date_Diff').distinct().collect()
        NoOfRows=len(list)
        data1=[]
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
        Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
        ageing.write.jdbc(url=Sqlurlwrite, table="StockSnapshot2", mode="overwrite")
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