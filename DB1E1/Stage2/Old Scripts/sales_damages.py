##  Dependent on 11_IngLastTwoMth.py
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import when,lit,concat_ws,concat,to_date,round,col
from pyspark.sql.types import *
import re,os,datetime#,keyring
import time,sys
import pandas as pd

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
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    connection_string="Driver="+ODBC13+";Server="+SURLCUR+","+PORTCUR+";Database="+WDB+";uid="+RUSER+";pwd="+RPASS
    
    
    #Postgresurl = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
    
    #Postgresprop= {
    #        "user":"postgres",
    #        "password":"sa123",
    #        "driver": "org.postgresql.Driver" 
    #        }
    
    #############CHANGED SOURCE 26FEB
    sales = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")
    
#     sales = sales.filter(sales.vouch_date=='2020-03-14')
    
    sales = sales.filter(sales.stock_trans==1)#.filter(sales.Deleted==0)  #.drop('Deleted')
    ###### BELOW changed avg to sum
    
    sales = sales.withColumn('Vouch_Date',to_date(sales.vouch_date)).withColumnRenamed('net_amt','Net_Bill_Amount')
    sales = sales.withColumnRenamed('Vouch_Date','Voucher_Date')\
                    .withColumnRenamed('vouch_code','Voucher_Code')
                    
    ########### Changed 12 FEB
    sd = sales.filter(sales['sltxndeleted']==0) ###### changed  mar 2
    
    sd=sd.withColumnRenamed("Sale_Or_SR","Type").withColumnRenamed("Tot_Qty","Qty").withColumnRenamed("Calc_Net_Amt","Net_Amt")\
           .withColumnRenamed("Calc_Commission","Comm")\
           .withColumnRenamed("Calc_Sp_Commission","Sp_Comm")\
           .withColumnRenamed("Vouch_Code","Voucher_Code_Drop")\
           .withColumnRenamed("Deleted","Del")\
           .withColumnRenamed("Code","Code_Drop")#.withColumnRenamed("Calc_Gross_Amt","Gross_Amt")
    
    ################33 CHANGED 12 FEBs
    sd=sd.filter(sd.sa_subs_lot==0).filter(sd.Del==0)\
            .withColumnRenamed('Qty','Sale_Quantity')\
            .withColumnRenamed('Gross_Amt','Gross_Amount')\
            .withColumnRenamed('Net_Amt','Net_Amount')\
            .withColumnRenamed('Comm','CD')\
            .withColumnRenamed('Sp_Comm','TD')\
            .withColumnRenamed('udamt','UD_Net_Amount2')
            
    sd=sd.select("Type","Item_Det_Code","Lot_Code","Voucher_Date","FiscalYear","Branch_Code","cust_code","series_code",\
                 "UD_Net_Amount2","Net_Bill_Amount","Sale_Quantity","Gross_Amount","Net_Amount","CD","TD")
    
    #sd = sd.drop('Stock_Trans','Sa_Subs_Lot','Del','Voucher_Code_Drop')
    '''
    sales = sales.withColumn('Sale_Quantity',when(sales.Type=='SL',sales.Qty).otherwise(sales.Qty*-1))\
            .withColumn('Gross_Amount',when(sales.Type=='SL',sales.calc_gross_amt).otherwise(sales.calc_gross_amt*-1))\
            .withColumn('Net_Amount',when(sales.Type=='SL',sales.Net_Amt).otherwise(sales.Net_Amt*-1))\
            .withColumn('CD',when(sales.Type=='SL',sales.Comm).otherwise(sales.Comm*-1))\
            .withColumn('TD',when(sales.Type=='SL',sales.Sp_Comm).otherwise(sales.Sp_Comm*-1))\
            .withColumn('UD_Net_Amount2',when(sales.Type=='SL',sales.udamt).otherwise(sales.udamt*-1))\
            .drop('Qty','calc_gross_amt','Net_Amt','Comm','Sp_Comm','udamt')
            
    
    sales=sales.groupBy("Type","Item_Det_Code","Lot_Code","Voucher_Date","FiscalYear","Branch_Code","cust_code","series_code")\
            .agg({'UD_Net_Amount2':'sum',"Net_Bill_Amount":"sum","Sale_Quantity":"sum","Gross_Amount":"sum","Net_Amount":"sum","CD":"sum","TD":"sum"})
    
    
    sales = sales.withColumnRenamed("sum(UD_Net_Amount2)","Calc_Net_Amt").\
    withColumnRenamed("sum(Gross_Amount)","Gross_Amount").withColumnRenamed("sum(Sale_Quantity)","Sale_Quantity").\
    withColumnRenamed("sum(Net_Amount)","Net_Amount").withColumnRenamed("sum(CD)","CD").withColumnRenamed("sum(TD)","TD").\
    withColumnRenamed("sum(Net_Bill_Amount)","Net_Bill_Amount")
    '''
    sd.cache()
    #sd.show()
    #sd.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/salesdamages")
    sd.write.jdbc(url=Sqlurlwrite, table="salesdamages", mode="overwrite")#, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'sales_damages','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(sd.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print("SALES SUCCESSFULLLLLLLLLLLLLLLLL")
    
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'sales_damages','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")