#### NO NEED TO RUN 

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
        
    conf = SparkConf().setMaster(smaster).setAppName("SL_Concat")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")
        #.set("spark.driver.cores",8)
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("SL_Concat").getOrCreate()
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
    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20192020) as tb"
    TXN = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20182019) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(TXN,TXN1,spark,sqlctx)
    
    
    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20172018) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    
    
    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20162017) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    ''Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20152016) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20142015) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20132014) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    table1.show()'''
    ###################CHANGED SOURCE  26 FEB
    sl_head = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Head2")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="SL_Concat")
    col_sp = cols.filter(cols.Table_Name=="SL_Head").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    sl_head = sl_head.select(col_sp)
    sl_head.cache()
    #sl_head.write.jdbc(url=Postgresurl, table="market99"+".SL_HEADAllYear", mode="overwrite", properties=Postgresprop)
    sl_head.write.jdbc(url=Postgresurl2, table="market99"+".SL_HEADAllYear", mode="overwrite", properties=Postgresprop2)
    
    print("Sale Head done")
    '''
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20192020) as tb"
    TXN = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20182019) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(TXN,TXN1,spark,sqlctx)
    
    table1.cache()
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20172018) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20162017) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    table1.cache()
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20152016) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    table1.cache()
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20142015) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    table1.cache()
    table1=concatenate(table1,TXN1,spark,sqlctx)
    '''
    '''Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt from SL_Txn20132014) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    table1.cache()
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20122013) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    table1=concatenate(table1,TXN1,spark,sqlctx)
    table1.cache()
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20112012) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    table1.cache()
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20102011) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    table1.cache()
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20092010) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20082009) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20072008) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20062007) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code,Calc_Net_Amt,Sale_Or_Sr,Tot_Qty from SL_Txn20052006) as tb"
    TXN1 = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()    
    table1=concatenate(table1,TXN1,spark,sqlctx)
    print("Final")'''
    #table1.show()
    SL_Txn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Txn")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="SL_Concat")
    col_sp = cols.filter(cols.Table_Name=="SL_Txn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    SL_Txn = SL_Txn.select(col_sp)
    SL_Txn.cache()
    #SL_Txn.write.jdbc(url=Postgresurl, table="market99"+".SL_TXNAllYear", mode="overwrite", properties=Postgresprop)
    SL_Txn.write.jdbc(url=Postgresurl2, table="market99"+".SL_TXNAllYear", mode="overwrite", properties=Postgresprop2)

    
    stime = stime - time.time()
    print(stime)
    print("SL_TxnALLYEAR  Doneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
except Exception as ex:
    print(ex)
    print("Qwerty")
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
   
