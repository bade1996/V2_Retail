####### NOT NEEDED TO RUN SCRIPT

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat,col
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
#from pyspark.sql.concatenate import *
import pandas as pd
from numpy.core._multiarray_umath import empty

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')

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
    
    conf = SparkConf().setMaster(smaster).setAppName("StockConcat")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")
        #.set("spark.driver.cores",8)
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("StockConcat").getOrCreate()
    sqlctx = SQLContext(sc)
    
    Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    
    Postgresurl = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
    
    
    Postgresprop= {
        "user":"postgres",
        "password":"sa123",
        "driver": "org.postgresql.Driver" 
    }
    
    Postgresurl2 = "jdbc:postgresql://103.248.60.5:5432/kockpit"
    Postgresprop2= {
        "user":"postgres",
        "password":"sa@123",
        "driver": "org.postgresql.Driver" 
    }
    ## PASSWORD Changed to sa123 for LINUX Postgres
    '''table1'''  
    '''
    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from  stk_dtxn20192020   ) as tb"
    
    
    TXN = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from stk_dtxn20182019   ) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(TXN,TXN1,spark,sqlctx)
    table1.cache()
    '''
    '''Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from  stk_dtxn20172018   ) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    
    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from  stk_dtxn20162017   ) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from  stk_dtxn20152016   ) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from  stk_dtxn20142015   ) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_,Sl_Qty,Sl_Free,Sl_Repl,Sl_Sample,Sr_Qty,Sr_Free,\
    Sr_Repl,Sr_Sample,Stk_In,Stk_Out,rec_qty,Prod_Qty,Pu_Qty from stk_dtxn20132014   ) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    table1=concatenate(table1,TXN1,spark,sqlctx)'''
    ###################CHANGED SOURCE  26 FEB
    stk_dtxn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn2")
#     print(stk_dtxn.count())
#     exit()
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Stk_Concat")
    col_sp = cols.filter(cols.Table_Name=="stk_dtxn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    stk_dtxn = stk_dtxn.select(col_sp)
    table1=stk_dtxn
    table1 = table1.filter(table1.Godown_Code ==0)
    table1.cache()
    table1.write.mode("overwrite").save(hdfspath+"/"+DBET+"/Stage2/StockAllYear")
    #table1.write.jdbc(url=Postgresurl, table="market99"+".StockAllYear", mode="overwrite", properties=Postgresprop)
    table1.write.jdbc(url=Postgresurl2, table="market99"+".StockAllYear", mode="overwrite", properties=Postgresprop2) #WINDOWS

    print("Final")
    stime = stime - time.time()
    print(stime)
    print("YaHoooooooooooooooooooooooooooooooooooooooo")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
   
