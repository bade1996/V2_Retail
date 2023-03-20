#Stock Query runtime:21 Mins
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat
import re,keyring,os,datetime
import time,sys
from pyspark.sql.types import *
from pyspark.sql.concatenate import *
import pandas as pd
from numpy.core._multiarray_umath import empty

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')

try:
    conf = SparkConf().setMaster('local[8]').setAppName("Market99")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Market99").getOrCreate()
    sqlctx = SQLContext(sc)
    
    Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=LOGICDBS99;user=sa;password=sa@123"
    Postgresurl = "jdbc:postgresql://localhost:5432/kockpit"
    
    
    Postgresprop= {
        "user":"postgres",
        "password":"sa@123",
        "driver": "org.postgresql.Driver" 
    }

    '''table1'''  
    lot="(SELECT *  FROM market99.lot) AS doc"
    lot=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=lot,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    

    item="(SELECT *  FROM market99.item) AS doc"
    item=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=item,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    item = item.drop("Comm_It_Code").drop("Comm_It_Desc_Code").drop("Color_Code")\
    .drop("Item_Name_Code").drop("Group_Code").drop("Group_Code1").drop("Group_Code2")\
   
    
#     table1=concatenate(lot,item,spark,sqlctx)
#     print("done")
#     table1.show()
#     exit()
    item = item.withColumnRenamed("Item_Det_Code","Item_Det_Code1")
    cond= [(lot["Item_Det_Code"]==item.Item_Det_Code1)]
    lot = lot.join(item,cond,"inner")
    
    
    txn="(SELECT *  FROM market99.txn) AS doc"
    txn=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=txn,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    
    txn = txn.withColumnRenamed("Lot_Code","Lot_Code1")
    cond= [(lot["Lot_Code"]==txn.Lot_Code1)]
    lot = lot.join(txn,cond,"inner")
  
    godown="(SELECT *  FROM market99.godown) AS doc"
    godown=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=godown,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    godown = godown.withColumnRenamed("Godown_Code","Godown_Code1")
    cond= [(lot["Godown_Code"]==godown.Godown_Code1)]
    lot = lot.join(godown,cond,"inner")
   
    pack="(SELECT *  FROM market99.pack) AS doc"
    pack=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=pack,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    pack = pack.withColumnRenamed("Pack_Code","Pack_Code1")
    cond= [(lot["Pack_Code"]==pack.Pack_Code1)]
    lot = lot.join(pack,cond,"inner")
    
    comp="(SELECT *  FROM market99.comp) AS doc"
    comp=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=comp,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    comp = comp.withColumnRenamed("Comp_Code","Comp_Code1")
    cond= [(lot["Comp_Code"]==comp.Comp_Code1)]
    lot = lot.join(comp,cond,"inner")
    
    gm="(SELECT *  FROM market99.gmhierarchy) AS doc"
    gm=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=gm,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    cond= [(lot["Item_Hd_Code"]==gm.item_hd_code)]
    lot = lot.join(gm,cond,"inner")
    lot = lot.drop("Group_Code1")
    
    branch="(SELECT *  FROM market99.branch) AS doc"
    branch=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=branch,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    branch = branch.withColumnRenamed("Branch_Code","Branch_Code1")
    cond= [(lot["Branch_Code"]==branch.Branch_Code1)]
    lot = lot.join(branch,cond,"inner")
    
    table1 = lot
    table1 = table1.withColumnRenamed("Date_","Vouch_Date") 
    table1.show()
    print("table1")
    table1.cache()
    table3 = table1
    table3.cache()
    table1 = table1.withColumn("Value_Type",lit('OP'))
    table3 = table3.withColumn("Value_Type",lit('CL'))
    table2 = table1.filter((table1.Txn_Type=='NO') | (table1.Show_ST_As_Sale==True)  )
    table2.cache()
    table2.show()
    table2 = table2.withColumn("Value_Type",lit('SLPU'))
    print("table2")
    table1 = table1.drop("Comp_Code").drop("Group_Code").drop("Godown_Code")\
    .drop('item_hd_code').drop("Lot_Code").drop("Pack_Code").drop("Branch_Code")
    table2 = table2.drop("Comp_Code").drop("Group_Code").drop("Godown_Code")\
    .drop('item_hd_code').drop("Lot_Code").drop("Pack_Code").drop("Branch_Code")
    table1=concatenate(table2,table1,spark,sqlctx)
    table3 = table3.drop("Comp_Code").drop("Group_Code").drop("Godown_Code")\
    .drop('item_hd_code').drop("Lot_Code").drop("Pack_Code").drop("Branch_Code")
   
    table1=concatenate(table3,table1,spark,sqlctx)
    table1.show()
    print("table11")

    '''Table 4'''
    
    lot="(SELECT *  FROM market99.lot) AS doc"
    lot=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=lot,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
     
    item="(SELECT *  FROM market99.item) AS doc"
    item=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=item,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    item = item.drop("Comm_It_Code").drop("Comm_It_Desc_Code").drop("Color_Code")\
    .drop("Item_Name_Code").drop("Group_Code").drop("Group_Code1").drop("Group_Code2")\
       
#   table1=concatenate(lot,item,spark,sqlctx)
#   print("done")
#   table1.show()
#   exit()
    item = item.withColumnRenamed("Item_Det_Code","Item_Det_Code1")
    cond= [(lot["Item_Det_Code"]==item.Item_Det_Code1)]
    lot = lot.join(item,cond,"inner")
    
    Table1 = "(Select Godown_Code,Lot_Code,Item_Det_Code,Vouch_Code from SL_Txn20192020) as tb"
    txn = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        
    Table1 = "(Select Branch_Code,Stock_Trans,Deleted,Vouch_Date,Vouch_Code from SL_Head20192020) as tb"
    hd = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    hd =hd.filter(hd.Stock_Trans==0)
    hd = hd.filter(hd.Deleted==0)
    
    txn = txn.withColumnRenamed("Lot_Code","Lot_Code1")
    cond= [(lot["Lot_Code"]==txn.Lot_Code1)]
    lot = lot.join(txn,cond,"inner")
  
    godown="(SELECT *  FROM market99.godown) AS doc"
    godown=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=godown,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    godown = godown.withColumnRenamed("Godown_Code","Godown_Code1")
    cond= [(lot["Godown_Code"]==godown.Godown_Code1)]
    lot = lot.join(godown,cond,"inner")
   
    pack="(SELECT *  FROM market99.pack) AS doc"
    pack=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=pack,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    pack = pack.withColumnRenamed("Pack_Code","Pack_Code1")
    cond= [(lot["Pack_Code"]==pack.Pack_Code1)]
    lot = lot.join(pack,cond,"inner")
    
    comp="(SELECT *  FROM market99.comp) AS doc"
    comp=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=comp,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    comp = comp.withColumnRenamed("Comp_Code","Comp_Code1")
    cond= [(lot["Comp_Code"]==comp.Comp_Code1)]
    lot = lot.join(comp,cond,"inner")
    
    gm="(SELECT *  FROM market99.gmhierarchy) AS doc"
    gm=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=gm,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    cond= [(lot["Item_Hd_Code"]==gm.item_hd_code)]
    lot = lot.join(gm,cond,"inner")
    lot = lot.drop("Group_Code1")
        
    hd = hd.withColumnRenamed("Vouch_Code","Vouch_Code1")
    cond= [(lot["Vouch_Code"]==hd.Vouch_Code1)]
    lot = lot.join(hd,cond,"inner")
    
    branch="(SELECT *  FROM market99.branch) AS doc"
    branch=sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=branch,\
              user="postgres",password="sa@123", driver= "org.postgresql.Driver").load()
    branch = branch.withColumnRenamed("Branch_Code","Branch_Code1")
    cond= [(lot["Branch_Code"]==branch.Branch_Code1)]
    lot = lot.join(branch,cond,"inner")
    
    lot.show()
    table4 = lot
    table4 = table4.withColumn("Value_Type",lit('SALE'))
    table4 = table4.drop("Comp_Code").drop("Group_Code").drop("Godown_Code")\
    .drop('item_hd_code').drop("Lot_Code").drop("Pack_Code").drop("Branch_Code")\
    .drop('Item_Det_Code').drop("Deleted").drop("Stock_Trans")
    table1 = table1.drop('Item_Det_Code')
    
    
    table1=concatenate(table4,table1,spark,sqlctx)
    table1 = table1.withColumnRenamed("group_name","DEPARTMENT").withColumnRenamed("group_name2","CATEGORY").withColumnRenamed("group_name3","SUB CATEGORY")\
    .withColumnRenamed("group_name11","ACTIVE / INACTIVE").withColumnRenamed("group_name4","MATERIAL").withColumnRenamed("group_name12","IMP / LOCAL")\
    .withColumnRenamed("group_name16","LAUNCH MONTH").withColumnRenamed("group_name20","NEW COMPANY NAME")\
    .withColumnRenamed("User_Code","ITEM CODE").withColumnRenamed("Item_Desc_M","ITEM DESCRIPTION")

    table1.write.jdbc(url=Postgresurl, table="market99"+".Stock", mode="overwrite", properties=Postgresprop)
    print("Final")
    stime = stime - time.time()
    print(stime)
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
   
    