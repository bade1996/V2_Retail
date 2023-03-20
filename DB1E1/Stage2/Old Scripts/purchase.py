from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,lit,concat_ws,concat,to_date,round,col
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
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
        
    conf = SparkConf().setMaster(smaster).setAppName('Purchase')
    sc = SparkContext(conf=conf)
    sqlctx = SQLContext(sc)
    
    #Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=logic;user=sa;password=sa@123"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    connection_string="Driver="+ODBC13+";Server="+SURLCUR+","+PORTCUR+";Database="+WDB+";uid="+RUSER+";pwd="+RPASS
    Postgresurl = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
        
    Postgresprop= {
            "user":"postgres",
            "password":"sa123",
            "driver": "org.postgresql.Driver" 
            }
    ## PASSWORD Changed to sa123 for LINUX Postgres
    Postgresurl2 = "jdbc:postgresql://103.248.60.5:5432/kockpit"
    Postgresprop2= {
        "user":"postgres",
        "password":"sa@123",
        "driver": "org.postgresql.Driver" 
    }
    '''
    pur_head_query = "(SELECT vouch_date AS Voucher_Date,\
                    vouch_code AS Voucher_Code,\
                    Bill_No,\
                    Bill_Date,\
                    GRN_PreFix,\
                    GRN_Number \
                    FROM Pur_Head20192020) AS pur_head"
    pur_headsql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=pur_head_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #1 added below
    '''
    #############CHANGED SOURCE 26FEB
    pur_head = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_Head2")
    
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="purchase")
    #cols.show()
    col_sp = cols.filter(cols.Table_Name=="Pur_Head").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    
    pur_head = pur_head.select(col_sp)
    
    
    pur_head=pur_head.withColumnRenamed("vouch_date","Voucher_Date").withColumnRenamed("vouch_code","Voucher_Code")
    #end
    #pur_head.show()
    
    pur_head = pur_head.withColumn('Voucher_Date',to_date(pur_head.Voucher_Date))\
                        .withColumn('GRN_Number',pur_head.GRN_Number.cast('int'))\
                        .withColumn('Bill_Date',to_date(pur_head.Bill_Date))
    pur_head = pur_head.withColumn('GRN_No',concat('GRN_PreFix',lit('-'),'GRN_Number')).drop('GRN_PreFix','GRN_Number')
    # pur_head.show(2)
    '''
    pur_txn_query = "(SELECT vouch_code AS Voucher_Code,\
                    Order_Item_Code,\
                    Item_Det_Code,\
                    Tot_Qty AS Total_Qty,\
                    Calc_Net_Amt AS Net_Amount,\
                    Calc_Tax_3,\
                    Calc_Sur_On_Tax3 \
                    FROM Pur_Txn20192020 \
                    WHERE Order_Item_Code!=0) AS pur_txn"
    pur_txn = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=pur_txn_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    pur_txnsql = pur_txn.withColumn('Tax_3',pur_txn.Calc_Tax_3+pur_txn.Calc_Sur_On_Tax3).drop('Calc_Tax_3','Calc_Sur_On_Tax3')
    '''
    #2 added below
    pur_txn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_Txn")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="purchase")
    col_sp = cols.filter(cols.Table_Name=="Pur_Txn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    
    pur_txn = pur_txn.select(col_sp).filter(pur_txn.Order_Item_Code!=0)
    pur_txn=pur_txn.withColumnRenamed("vouch_code","Voucher_Code").withColumnRenamed("Tot_Qty","Total_Qty").withColumnRenamed("Calc_Net_Amt","Net_Amount")
    pur_txn=pur_txn.withColumn('Tax_3',pur_txn.Calc_Tax_3+pur_txn.Calc_Sur_On_Tax3).drop('Calc_Tax_3','Calc_Sur_On_Tax3')
    #end
    # pur_txn.show(2)
    '''
    po_head_query = "(SELECT Vouch_Code AS Voucher_Code,\
                    Branch_Code,\
                    Sup_Code,\
                    NetOrder_Amount AS PO_Amount,\
                    Order_Date AS PO_Order_Date,\
                    Order_No AS PO_Order_No \
                    FROM PO_Head) AS po_head"
    po_headsql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=po_head_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #2 added below
    po_head = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/PO_Head")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="purchase")
    col_sp = cols.filter(cols.Table_Name=="PO_Head").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    po_head = po_head.select(col_sp)
    #end
    po_head=po_head.withColumnRenamed("Vouch_Code","Voucher_Code").withColumnRenamed("NetOrder_Amount","PO_Amount").withColumnRenamed("Order_Date","PO_Order_Date")\
                .withColumnRenamed("Order_No","PO_Order_No")
    
    po_head = po_head.withColumn('PO_Order_Date',to_date(po_head.PO_Order_Date))
    # po_head.show(2)
    '''
    po_txn_query = "(SELECT Vouch_Code AS Voucher_Code,\
                    Code \
                    FROM Po_Txn) AS po_txn"
    po_txnsql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=po_txn_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #2 added below
    po_txn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Po_Txn")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="purchase")
    col_sp = cols.filter(cols.Table_Name=="Po_Txn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    po_txn = po_txn.select(col_sp)
    po_txn=po_txn.withColumnRenamed("Vouch_Code","Voucher_Code")
    #end
    # po_txn.show(2)
    
    po_txn = po_txn.withColumnRenamed('Voucher_Code','Voucher_Code_Drop')
    po_cond = [po_head.Voucher_Code==po_txn.Voucher_Code_Drop]
    po_head = po_head.join(po_txn,po_cond,'left').drop('Voucher_Code_Drop')
    # po_head.show(2)
    
    pur_txn = pur_txn.withColumnRenamed('Voucher_Code','Voucher_Code_Drop')
    pur_cond = [pur_head.Voucher_Code==pur_txn.Voucher_Code_Drop]
    pur_head = pur_head.join(pur_txn,pur_cond,'left').drop('Voucher_Code_Drop')
    # pur_head.show(2)
    
    pur_head = pur_head.withColumnRenamed('Voucher_Code','Voucher_Code_Drop')
    po_pur_cond = [po_head.Code==pur_head.Order_Item_Code]
    purchase = po_head.join(pur_head,po_pur_cond,'left').drop('Voucher_Code','Voucher_Code_Drop','Code','Order_Item_Code')
    '''
    purchase = purchase.groupBy('Branch_Code','Sup_Code','Item_Det_Code','PO_Order_Date','PO_Order_No','Voucher_Date','Bill_No','Bill_Date','GRN_No')\
                        .agg({'PO_Amount':'max','Total_Qty':'sum','Net_Amount':'sum','Tax_3':'sum'})\
                        .withColumnRenamed('max(PO_Amount)','PO_Amount')\
                        .withColumnRenamed('sum(Total_Qty)','Total_Qty')\
                        .withColumnRenamed('sum(Net_Amount)','Net_Amount')\
                        .withColumnRenamed('sum(Tax_3)','Tax_3')
    '''
    purchase.cache()
    #purchase.write.jdbc(url=Postgresurl, table="market99"+".purchase", mode="overwrite", properties=Postgresprop)
    #purchase.write.jdbc(url=Postgresurl2, table="market99"+".purchase", mode="overwrite", properties=Postgresprop2)  #WINDOWS
    
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    purchase.write.jdbc(url=Sqlurlwrite, table="purchase", mode="overwrite")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Purchase','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(purchase.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print("Successsssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
    
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Purchase','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")