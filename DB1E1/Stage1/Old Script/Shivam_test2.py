#print("Shivam")


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import last
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udl
import pandas as pd
from ntpath import join


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
    
    conf = SparkConf().setMaster(smaster).setAppName("Stage1:Data_Ingestion").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:Data_Ingestion").getOrCreate()
   
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    
    Po_Head = "(SELECT Order_No As PoOrderNo, Order_Date As PoOrderDate, Valid_Date AS PoValidDate, Branch_Code, Sup_Code, Order_Type, vouch_code, Tot_Tax AS TAX from Po_Head) as head"
    Po_Head = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Po_Head,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
    Pur_Head20192020 = "(SELECT Bill_No AS BillNo, Bill_Date AS BillDate, Grn_Number As GrnNo, Vouch_Date AS VoucherDate, vouch_code from Pur_Head20192020) as Pur"
    Pur_Head20192020 = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Pur_Head20192020,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
   
    Po_Txn = "(SELECT Item_Det_Code, (Tot_Qty * Rate) As OrderAmount, Tot_Qty, Cancel_Item, Pend_Qty, Code, Rate AS POTRate from Po_Txn) as Txn"
    Po_Txn = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Po_Txn,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
   
    Pur_Txn20192020 = "(SELECT Lot_Code, Order_Item_Code, Challan_code, vouch_code, Calc_Tax_3 As Tax3, Rate As BasicRate, Calc_Net_Amt as NetAmount, Calc_Gross_Amt as GrossAmount  from Pur_Txn20192020 ) as Txn1"
    Pur_Txn20192020 = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Pur_Txn20192020,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
    print(Po_Txn.columns)
    print(Po_Head.columns)
    
    Pur_Txn20192020 = Pur_Txn20192020.filter((Pur_Txn20192020.Order_Item_Code > 0) & (Pur_Txn20192020.Challan_code == 0))
    
    Po_Txn = Po_Txn.join(Pur_Txn20192020, Po_Txn.Code == Pur_Txn20192020.Order_Item_Code, how = 'left' )
    
    Po_Txn  = Po_Txn.join(Pur_Head20192020, Pur_Txn20192020.vouch_code == Pur_Head20192020.vouch_code, how = 'left')
#print(Po_Txn.columns)
  #  print(Po_Head.columns)
    Po_Txn  = Po_Txn.join(Po_Head, on = ['vouch_code'], how = 'inner')
    
   # Po_Txn = Po_Txn.join(Po_Head, Po_Txn.vouch_code == Po_Head.vouch_code, how = 'left')
    
    Po_Txn = Po_Txn.withColumn("POQty",when((Po_Txn["Order_Type"] == 'PI'), "0").otherwise(Po_Txn.Tot_Qty))
    
    Po_Txn = Po_Txn.withColumn("CancelPOQty",when((Po_Txn["Order_Type"] == 'PI') | (Po_Txn["Cancel_Item"] == 0), "0").otherwise(Po_Txn['Pend_Qty']))
    
    
    Po_Txn = Po_Txn.withColumn("PendingPOQty",when((Po_Txn["Cancel_Item"] == 0), Po_Txn["Pend_Qty"]).otherwise("0"))
    
    print("yes")
    Po_Txn.show()
    print(Po_Txn.columns)
   
    
    #Pur_Txn20192020 = Pur_Txn20192020.select("Calc_Tax_3 As Tax3, Rate As BasicRate")
    
   # Po_Txn = Po_Txn.select("Rate AS POTRate")
  #  Po_Head
   # Po_Head = Po_Head.select("Tot_Tax AS TAX") 
    
    #Pur_Txn20192020 = Pur_Txn20192020.select("Calc_Net_Amt as NetAmount, Calc_Gross_Amt as GrossAmount")
    
   # exit()
    
   # Pur_Txn20192020 = Pur_Txn20192020.withColumn("Key", (Pur_Txn20192020[Order_Item_Code]), (Pur_Txn20192020[Order_itrm_code] > 0), (Pur_Txn20192020[Challan_Code] = 0)

#filter
#left join
#left join
#inner join

    
    
   
    
     
   # Po_Txn.show()
    print("Ho gaya")
    print(Po_Txn.count())
    
except Exception as ex:
    print(ex)
    print("test")    
