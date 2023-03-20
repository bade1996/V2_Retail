from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import when,lit,concat_ws,concat,to_date,round,col
from pyspark.sql.types import *
import re,os,datetime#,keyring
import time,sys
import pandas as pd

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
        
    conf = SparkConf().setMaster(smaster).setAppName('Sales')\
        .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")
    sc = SparkContext(conf=conf)
    sqlctx = SQLContext(sc)
    #Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=logic;user=sa;password=sa@123"
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
    #     test=  sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2")
    #     test.show()
    #     exit()
    #1 added below###########  12 feb
    #############CHANGED SOURCE 26FEB
    sl_head = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_Head2")
    
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Sales")
    col_sp = cols.filter(cols.Table_Name=="Sl_Head").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    sl_head = sl_head.select(col_sp)
    ######13 Feb
    #sl_head=sl_head.filter(sl_head.Vouch_Date=='2020-01-01').filter(sl_head.Branch_Code=='5002')
    #sl_head.filter(sl_head.Vouch_Code=='2267598').show()
    #endd
    sl_head = sl_head.filter(sl_head.Deleted==0).filter(sl_head.Stock_Trans==0).drop('Deleted')
    ###### BELOW changed avg to sum
    '''
    sl_head = sl_head.groupBy('Vouch_Code','Vouch_Date','Stock_Trans','Branch_Code','Act_Code_For_Txn_X','Series_Code')\
                        .agg({'Net_Amt':'avg'})\
                        .withColumnRenamed('avg(Net_Amt)','Net_Bill_Amount')
    #sl_head=sl_head.filter(sl_head.Vouch_Date>='2020-01-01').filter(sl_head.Vouch_Date<='2020-01-31').filter(sl_head.Branch_Code=='5002')
    #sl_head=sl_head.groupby('Branch_Code').agg({'Net_Bill_Amount','sum'})#.show(50)
    '''
    sl_head = sl_head.withColumn('Vouch_Date',to_date(sl_head.Vouch_Date)).withColumnRenamed('Net_Amt','Net_Bill_Amount')
    sl_head = sl_head.withColumnRenamed('Vouch_Date','Voucher_Date')\
                    .withColumnRenamed('Vouch_Code','Voucher_Code')

    
    ########### Changed 12 FEB
    txn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_Txn")
    #txn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_Txn20192020")
    
    #txn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_TxnPrevious")#####PREVIOUS
    
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Sales")
    col_sp = cols.filter(cols.Table_Name=="Sl_Txn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    txn = txn.select(col_sp)
    #txn = txn.filter(txn['Deleted']==0)
    
    txn = txn.filter(txn['sltxndeleted']==0) ###### changed  mar 2
    #end
    #txn.toDF(*[c.lower() for c in txn.columns]).show()
    txn=txn.withColumnRenamed("Sale_Or_SR","Type").withColumnRenamed("Tot_Qty","Qty").withColumnRenamed("Calc_Gross_Amt","Gross_Amt")\
           .withColumnRenamed("Calc_Net_Amt","Net_Amt")\
           .withColumnRenamed("Calc_Commission","Comm")\
           .withColumnRenamed("Calc_Sp_Commission","Sp_Comm")\
           .withColumnRenamed("Vouch_Code","Voucher_Code_Drop")\
           .withColumnRenamed("Deleted","Del")\
           .withColumnRenamed("Code","Code_Drop")
           #.withColumnRenamed("Gross_Amt.","GrossA")
    
    txn=txn.withColumn("udamt",(0 + txn.Gross_Amt + txn.Comm + txn.Sp_Comm +\
                    txn.calc_rdf + txn.calc_scheme_u + txn.calc_scheme_rs +\
                    txn.Calc_Tax_1 + txn.Calc_Tax_2 + txn.Calc_Tax_3 + txn.calc_sur_on_tax3 +\
                    txn.calc_mfees + txn.calc_excise_u + txn.Calc_adjustment_u + txn.Calc_adjust_rs +\
                    txn.Calc_freight + txn.calc_adjust + txn.Calc_Spdisc + txn.Calc_DN + txn.Calc_CN +\
                    txn.Calc_Display + txn.Calc_Handling + txn.calc_Postage + txn.calc_round + txn.calc_labour))
    
    txn=txn.drop("calc_rdf","calc_scheme_u","calc_scheme_rs","Calc_Tax_1",\
                 "Calc_Tax_2","Calc_Tax_3","calc_sur_on_tax3","calc_mfees","calc_excise_u","Calc_adjustment_u","Calc_adjust_rs",\
                 "Calc_freight","calc_adjust","Calc_Spdisc","Calc_DN","Calc_CN","Calc_Display","Calc_Handling","calc_Postage","calc_round") 
    
    txn = txn.withColumnRenamed("FiscalYear","FiscalYear2")  
#     txn = txn.filter(txn.Voucher_Code_Drop=='2265081').show()
#     exit()
    '''
    #3 added below
    ################33 CHANGED 12 FEBs
    ss = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Sch")
    #ss = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Sch20192020")
    
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Sales")
    col_sp = cols.filter(cols.Table_Name=="SL_Sch").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    ss = ss.select(col_sp)
    #end
    '''
    
    hd_txn_cond = [sl_head.Voucher_Code==txn.Voucher_Code_Drop,sl_head.FiscalYear == txn.FiscalYear2]
    hd_txn = txn.join(sl_head,hd_txn_cond,'left').drop('Voucher_Code_Drop').drop("FiscalYear2")
#     hd_txn.filter(hd_txn.Voucher_Code=='2265081').show()
#     exit()
    #hd_txn = hd_txn.filter(hd_txn['Branch_Code']==5002).filter(hd_txn['Voucher_Date']=='2020-01-01').filter(hd_txn['Voucher_Code']=='2265081')
    
    
    sales_damages = hd_txn.filter(hd_txn.Stock_Trans==1)\
                        .filter(hd_txn.Sa_Subs_Lot==0)\
                        .filter(hd_txn.Del==0)\
                        .withColumnRenamed('Qty','Sale_Quantity')\
                        .withColumnRenamed('Gross_Amt','Gross_Amount')\
                        .withColumnRenamed('Net_Amt','Net_Amount')\
                        .withColumnRenamed('Comm','CD')\
                        .withColumnRenamed('Sp_Comm','TD')\
                        .withColumnRenamed('udamt','UD_Net_Amount2')
    
    hd_txn = hd_txn.drop('Stock_Trans','Sa_Subs_Lot','Del')
    #hd_txn_ss_cond = [ss.SL_Txn_Code==hd_txn.Code_Drop]
    sales=hd_txn
    #sales = ss.join(hd_txn,hd_txn_ss_cond,'left').drop('Code_Drop')
    #sales = sales.withColumn('Bill_No',lit('NA'))
    
    #sales = sales.filter(sales.Code_Type=='SCHCMP').drop('Code_Type')
    sales = sales.withColumn('Sale_Quantity',when(sales.Type=='SL',sales.Qty).otherwise(sales.Qty*-1))\
            .withColumn('Gross_Amount',when(sales.Type=='SL',sales.Gross_Amt).otherwise(sales.Gross_Amt*-1))\
            .withColumn('Net_Amount',when(sales.Type=='SL',sales.Net_Amt).otherwise(sales.Net_Amt*-1))\
            .withColumn('CD',when(sales.Type=='SL',sales.Comm).otherwise(sales.Comm*-1))\
            .withColumn('TD',when(sales.Type=='SL',sales.Sp_Comm).otherwise(sales.Sp_Comm*-1))\
            .withColumn('UD_Net_Amount2',when(sales.Type=='SL',sales.udamt).otherwise(sales.udamt*-1))\
            .drop('Qty','Gross_Amt','Net_Amt','Comm','Sp_Comm','udamt')
    
    
    
    salesRaw=sales
    #salesRaw.write.jdbc(url=Postgresurl, table="market99"+".salesRaw", mode="overwrite", properties=Postgresprop)
    #sales=sales.groupBy("Voucher_Date","Net_Bill_Amount","Voucher_Code","Branch_Code","Series_Code","Type",)
    
    sales=sales.groupBy("Type","Item_Det_Code","Lot_Code","Voucher_Date","FiscalYear","Branch_Code")\
            .agg({'UD_Net_Amount2':'sum',"Net_Bill_Amount":"sum","Sale_Quantity":"sum","Gross_Amount":"sum","Net_Amount":"sum","CD":"sum","TD":"sum"})
        
    
    sales = sales.withColumnRenamed("sum(UD_Net_Amount2)","Calc_Net_Amt")
    sales.cache()
    '''
    sales.filter(sales.Voucher_Date=='2020-01-01').filter(sales.Branch_Code=='5002').show()
    sales.filter(sales.Voucher_Date=='2020-01-02').filter(sales.Branch_Code=='5002').show()
    sales.filter(sales.Voucher_Date=='2020-01-03').filter(sales.Branch_Code=='5002').show()
    sales.filter(sales.Voucher_Date=='2020-01-04').filter(sales.Branch_Code=='5002').show()
    exit()
    '''
    #.filter(sales.Voucher_Code=='2265081')
    #sales=sales.filter(sales.Voucher_Date=='2020-01-01').filter(sales.Branch_Code=='5002').show()
    #sales.groupBy().agg({'UD_Net_Amount2':'sum'}).show()
    #sl_head=sl_head.groupby('Branch_Code').agg({'Net_Bill_Amount','sum'})#.show(50)
    
    #sales.write.jdbc(url=Postgresurl, table="market99"+".sales", mode="overwrite", properties=Postgresprop)     ##LINUX
    sales.write.jdbc(url=Postgresurl2, table="market99"+".sales", mode="overwrite", properties=Postgresprop2)   ##WINDOWS
    #sales_damages.cache()
    #sales_damages.write.jdbc(url=Postgresurl, table="market99"+".sales_damage", mode="overwrite", properties=Postgresprop)
    print("YaHoooooooooooooooooooooooooooo")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
