'''
Dependent on Stage1: 11_IngLast2Mth.py
Sales Incremental
Aniket, 11May,20
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,col,month,year,lit,concat,max,min,sum,datediff,to_date,col,when
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
import udl
import pandas as pd
from numpy.core._multiarray_umath import empty
from dateutil import relativedelta
from pyspark.storagelevel import StorageLevel   
import psycopg2
import dateutil.relativedelta
import pyspark.sql.functions as F
import pyodbc

now = datetime.datetime.now()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()#.strftime('%H:%M:%S')


present=datetime.datetime.now()#.strftime('%Y-%m-%d')   ###### PRESENT DATE
present=present-dateutil.relativedelta.relativedelta(months=2)   ## Done Only for testing May 12
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
CM1=CM
CM = str(CM)
##------Added May 4 FiscalM------##
if CM1>3:
    FM=CM1-3
else:
    FM=CM1+9
Mcount = 24+FM

if present.month==3:
    rollD=28
else:
    rollD=30

previous=present-dateutil.relativedelta.relativedelta(months=1)   
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

##------Added Apr 28 for Logs-------###
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
    
    #print(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")
    sales=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")
    print(sales.count())
    exit()
    
    try:
        ###################ROW COUNT################
        cntQuery = "(SELECT COUNT(*) As kalam FROM sales) As Data"
        cnt = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=cntQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        cnt = cnt.collect()[0]['kalam']
        bcnt=cnt
        print("BEFORE ETL ROW COUNT ",cnt)
        
        squery = "(SELECT DISTINCT (yearmonth) as littleboy FROM sales) As Data"
        table = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=squery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        value=table.count()
        print(value)
        
        if value>=22:#Mcount  #24
            print("RUN INCREMENTAL")
            ######################## MAR 4  TAKE LAST TWO RECORDS ##########
            print("LAST TWO MONTHS")
            #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=103.248.60.5\MARKET99,1433;Database=kockpit;uid=sa;pwd=M99@321' #connection_string = 'Driver={ODBC Driver 13 for SQL Server};Server=10.4.0.194,1433;Database=SnapshotDB1BIKO;uid=sa;pwd=koc@P2019
            con= pyodbc.connect(connection_string)
            
            dquery="DELETE FROM sales WHERE yearmonth = "+pastFY+PM
            dquery1="DELETE FROM sales WHERE yearmonth = "+presentFY+CM
            #dquery1="DELETE FROM PurchaseOrderAlys WHERE YEAR(voucherdate) ="+presentFY+" AND MONTH(voucherdate) ="+CM
            cur=con.cursor()#491
            cur.execute(dquery)
            
            
            ############ Previous Month ###########
            sales=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1/yearmonth="+pastFY+PM)
            sales=sales.withColumn("yearmonth",lit(pastFY+PM))
            #sales = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")
            #sales.printSchema()
            
            if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn1/yearmonth="+presentFY+CM)!=256:   #Not equals 256 i.e checks whether exists
                sales2=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1/yearmonth="+presentFY+CM)
                sales2=sales2.withColumn("yearmonth",lit(presentFY+CM))
                sales=sales.unionByName(sales2)
                sales.cache()
                cur.execute(dquery1)
                print("Query Successful")
            else:
                sales=sales
            
            con.commit()
            con.close
            
            ##################Old Sales2 Script ######################
            sales = sales.filter(sales.stock_trans==0).filter(sales.deleted==0)  #.drop('Deleted')
            ###### BELOW changed avg to sum
            
            sales = sales.withColumn('Vouch_Date',to_date(sales.vouch_date)).withColumnRenamed('net_amt','Net_Bill_Amount')
            sales = sales.withColumnRenamed('Vouch_Date','Voucher_Date')\
                            .withColumnRenamed('vouch_code','Voucher_Code')
                            
            ########### Changed 12 FEB
            sales = sales.filter(sales['sltxndeleted']==0) ###### changed  mar 2
            
            sales=sales.withColumnRenamed("Sale_Or_SR","Type").withColumnRenamed("Tot_Qty","Qty").withColumnRenamed("Calc_Net_Amt","Net_Amt")\
                   .withColumnRenamed("Calc_Commission","Comm")\
                   .withColumnRenamed("Calc_Sp_Commission","Sp_Comm")\
                   .withColumnRenamed("Vouch_Code","Voucher_Code_Drop")\
                   .withColumnRenamed("Deleted","Del")\
                   .withColumnRenamed("Code","Code_Drop")#.withColumnRenamed("Calc_Gross_Amt","Gross_Amt")
            
            ################33 CHANGED 12 FEBs
            sales = sales.drop('Del','Voucher_Code_Drop')##Excluded 'Stock_Trans','Sa_Subs_Lot',
            
            sales = sales.withColumn('Sale_Quantity',when(sales.Type=='SL',sales.Qty).otherwise(sales.Qty*-1))\
                    .withColumn('Gross_Amount',when(sales.Type=='SL',sales.calc_gross_amt).otherwise(sales.calc_gross_amt*-1))\
                    .withColumn('Net_Amount',when(sales.Type=='SL',sales.Net_Amt).otherwise(sales.Net_Amt*-1))\
                    .withColumn('CD',when(sales.Type=='SL',sales.Comm).otherwise(sales.Comm*-1))\
                    .withColumn('TD',when(sales.Type=='SL',sales.Sp_Comm).otherwise(sales.Sp_Comm*-1))\
                    .withColumn('UD_Net_Amount2',when(sales.Type=='SL',sales.udamt).otherwise(sales.udamt*-1))\
                    .drop('Qty','calc_gross_amt','Net_Amt','Comm','Sp_Comm','udamt')
            
            
            
            sales=sales.groupBy("Type","Item_Det_Code","Lot_Code","Voucher_Date","FiscalYear","Branch_Code","cust_code","series_code","Code_Drop","yearmonth")\
                    .agg({'UD_Net_Amount2':'sum',"Net_Bill_Amount":"sum","Sale_Quantity":"sum","Gross_Amount":"sum","Net_Amount":"sum","CD":"sum","TD":"sum"})
            
            
            sales = sales.withColumnRenamed("sum(UD_Net_Amount2)","Calc_Net_Amt").\
            withColumnRenamed("sum(Gross_Amount)","Gross_Amount").withColumnRenamed("sum(Sale_Quantity)","Sale_Quantity").\
            withColumnRenamed("sum(Net_Amount)","Net_Amount").withColumnRenamed("sum(CD)","CD").withColumnRenamed("sum(TD)","TD").\
            withColumnRenamed("sum(Net_Bill_Amount)","Net_Bill_Amount")
            
            sales.cache()
            
            print("before SL_Sch ",sales.count())   #######################!!!!!!
            sch= sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Sch")
            sch=sch.drop_duplicates()                 #Added new 12 May
            sch=sch.filter(sch.code_type=='SCHCMP')   #Added New 12 May
            sch=sch.withColumnRenamed("fiscalyear","fiscalyeardrop")
        
            cond=[sales.Code_Drop==sch.sl_txn_code,sales.FiscalYear==sch.fiscalyeardrop]
            sales = sales.join(sch,cond,'left')
            sales=sales.drop('fiscalyeardrop','sl_txn_code','sch_grp_code','sch_grp_code_type','sch_det_free_code','code_type','entityname','dbname')
            
            sales.cache()
            print("AFTER SL_Sch ",sales.count())   #######################!!!!!!
            sales.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/sales")
            sales.write.jdbc(url=Sqlurlwrite, table="sales", mode="append")#, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")
            
            cntQuery = "(SELECT COUNT(*) As kalam FROM sales) As Data"
            cnt = sqlctx.read.format("jdbc").options(url=Sqlurlwrite,dbtable=cntQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            cnt = cnt.collect()[0]['kalam']
            print("AFTER ETL ROW COUNT ",cnt)
            
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'sales2','DB':DB,'EN':Etn,
                         'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(sales.count()),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
            log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
            
            print("SALES LAST TWO MTHS SUCCESSFULLLLLLLLLLLLLLLLL")
            
        else:
            print("ELSE FULL RELOAD")
            sales = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")
    
            exit()
            sales = sales.filter(sales.stock_trans==0).filter(sales.deleted==0)  #.drop('Deleted')
            ###### BELOW changed avg to sum
            
            sales = sales.withColumn('Vouch_Date',to_date(sales.vouch_date)).withColumnRenamed('net_amt','Net_Bill_Amount')
            sales = sales.withColumnRenamed('Vouch_Date','Voucher_Date')\
                            .withColumnRenamed('vouch_code','Voucher_Code')
                            
            ########### Changed 12 FEB
            sales = sales.filter(sales['sltxndeleted']==0) ###### changed  mar 2
            
            sales=sales.withColumnRenamed("Sale_Or_SR","Type").withColumnRenamed("Tot_Qty","Qty").withColumnRenamed("Calc_Net_Amt","Net_Amt")\
                   .withColumnRenamed("Calc_Commission","Comm")\
                   .withColumnRenamed("Calc_Sp_Commission","Sp_Comm")\
                   .withColumnRenamed("Vouch_Code","Voucher_Code_Drop")\
                   .withColumnRenamed("Deleted","Del")\
                   .withColumnRenamed("Code","Code_Drop")#.withColumnRenamed("Calc_Gross_Amt","Gross_Amt")
            
            ################33 CHANGED 12 FEBs
            sales = sales.drop('Del','Voucher_Code_Drop')##Excluded 'Stock_Trans','Sa_Subs_Lot',
            
            sales = sales.withColumn('Sale_Quantity',when(sales.Type=='SL',sales.Qty).otherwise(sales.Qty*-1))\
                    .withColumn('Gross_Amount',when(sales.Type=='SL',sales.calc_gross_amt).otherwise(sales.calc_gross_amt*-1))\
                    .withColumn('Net_Amount',when(sales.Type=='SL',sales.Net_Amt).otherwise(sales.Net_Amt*-1))\
                    .withColumn('CD',when(sales.Type=='SL',sales.Comm).otherwise(sales.Comm*-1))\
                    .withColumn('TD',when(sales.Type=='SL',sales.Sp_Comm).otherwise(sales.Sp_Comm*-1))\
                    .withColumn('UD_Net_Amount2',when(sales.Type=='SL',sales.udamt).otherwise(sales.udamt*-1))\
                    .drop('Qty','calc_gross_amt','Net_Amt','Comm','Sp_Comm','udamt')
            
            
            
            sales=sales.groupBy("Type","Item_Det_Code","Lot_Code","Voucher_Date","FiscalYear","Branch_Code","cust_code","series_code","Code_Drop","yearmonth")\
                    .agg({'UD_Net_Amount2':'sum',"Net_Bill_Amount":"sum","Sale_Quantity":"sum","Gross_Amount":"sum","Net_Amount":"sum","CD":"sum","TD":"sum"})
            
            
            sales = sales.withColumnRenamed("sum(UD_Net_Amount2)","Calc_Net_Amt").\
            withColumnRenamed("sum(Gross_Amount)","Gross_Amount").withColumnRenamed("sum(Sale_Quantity)","Sale_Quantity").\
            withColumnRenamed("sum(Net_Amount)","Net_Amount").withColumnRenamed("sum(CD)","CD").withColumnRenamed("sum(TD)","TD").\
            withColumnRenamed("sum(Net_Bill_Amount)","Net_Bill_Amount")
            
            sales.cache()
            sch= sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Sch")
            sch=sch.drop_duplicates()                 #Added new 12 May
            sch=sch.filter(sch.code_type=='SCHCMP')   #Added New 12 May
            sch=sch.withColumnRenamed("fiscalyear","fiscalyeardrop")
        
            cond=[sales.Code_Drop==sch.sl_txn_code,sales.FiscalYear==sch.fiscalyeardrop]
            sales = sales.join(sch,cond,'left')
            sales=sales.drop('fiscalyeardrop','sl_txn_code','sch_grp_code','sch_grp_code_type','sch_det_free_code','code_type','entityname','dbname')
            
            sales.cache()
            sales.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/sales")
            sales.write.jdbc(url=Sqlurlwrite, table="sales", mode="overwrite")#, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")
            
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'sales2','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(sales.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
            log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
            
            print("SALES FULL RELOAD SUCCESSFULLLLLLLLLLLLLLLLL")
            
            
    except Exception as ex:
        print("EXCEPT FULL RELOAD")
        sales = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")

        exit()
        sales = sales.filter(sales.stock_trans==0).filter(sales.deleted==0)  #.drop('Deleted')
        ###### BELOW changed avg to sum
        
        sales = sales.withColumn('Vouch_Date',to_date(sales.vouch_date)).withColumnRenamed('net_amt','Net_Bill_Amount')
        sales = sales.withColumnRenamed('Vouch_Date','Voucher_Date')\
                        .withColumnRenamed('vouch_code','Voucher_Code')
                        
        ########### Changed 12 FEB
        sales = sales.filter(sales['sltxndeleted']==0) ###### changed  mar 2
        
        sales=sales.withColumnRenamed("Sale_Or_SR","Type").withColumnRenamed("Tot_Qty","Qty").withColumnRenamed("Calc_Net_Amt","Net_Amt")\
               .withColumnRenamed("Calc_Commission","Comm")\
               .withColumnRenamed("Calc_Sp_Commission","Sp_Comm")\
               .withColumnRenamed("Vouch_Code","Voucher_Code_Drop")\
               .withColumnRenamed("Deleted","Del")\
               .withColumnRenamed("Code","Code_Drop")#.withColumnRenamed("Calc_Gross_Amt","Gross_Amt")
        
        ################33 CHANGED 12 FEBs
        sales = sales.drop('Del','Voucher_Code_Drop')##Excluded 'Stock_Trans','Sa_Subs_Lot',
        
        sales = sales.withColumn('Sale_Quantity',when(sales.Type=='SL',sales.Qty).otherwise(sales.Qty*-1))\
                .withColumn('Gross_Amount',when(sales.Type=='SL',sales.calc_gross_amt).otherwise(sales.calc_gross_amt*-1))\
                .withColumn('Net_Amount',when(sales.Type=='SL',sales.Net_Amt).otherwise(sales.Net_Amt*-1))\
                .withColumn('CD',when(sales.Type=='SL',sales.Comm).otherwise(sales.Comm*-1))\
                .withColumn('TD',when(sales.Type=='SL',sales.Sp_Comm).otherwise(sales.Sp_Comm*-1))\
                .withColumn('UD_Net_Amount2',when(sales.Type=='SL',sales.udamt).otherwise(sales.udamt*-1))\
                .drop('Qty','calc_gross_amt','Net_Amt','Comm','Sp_Comm','udamt')
        
        
        
        sales=sales.groupBy("Type","Item_Det_Code","Lot_Code","Voucher_Date","FiscalYear","Branch_Code","cust_code","series_code","Code_Drop","yearmonth")\
                .agg({'UD_Net_Amount2':'sum',"Net_Bill_Amount":"sum","Sale_Quantity":"sum","Gross_Amount":"sum","Net_Amount":"sum","CD":"sum","TD":"sum"})
        
        
        sales = sales.withColumnRenamed("sum(UD_Net_Amount2)","Calc_Net_Amt").\
        withColumnRenamed("sum(Gross_Amount)","Gross_Amount").withColumnRenamed("sum(Sale_Quantity)","Sale_Quantity").\
        withColumnRenamed("sum(Net_Amount)","Net_Amount").withColumnRenamed("sum(CD)","CD").withColumnRenamed("sum(TD)","TD").\
        withColumnRenamed("sum(Net_Bill_Amount)","Net_Bill_Amount")
        
        sales.cache()
        sch= sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/SL_Sch")
        sch=sch.drop_duplicates()                 #Added new 12 May
        sch=sch.filter(sch.code_type=='SCHCMP')   #Added New 12 May
        sch=sch.withColumnRenamed("fiscalyear","fiscalyeardrop")
    
        cond=[sales.Code_Drop==sch.sl_txn_code,sales.FiscalYear==sch.fiscalyeardrop]
        sales = sales.join(sch,cond,'left')
        sales=sales.drop('fiscalyeardrop','sl_txn_code','sch_grp_code','sch_grp_code_type','sch_det_free_code','code_type','entityname','dbname')
        
        sales.cache()
        sales.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/sales")
        sales.write.jdbc(url=Sqlurlwrite, table="sales", mode="overwrite")#, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")
        
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'sales2','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(sales.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
        log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
        log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
        
        print("SALES FULL RELOAD SUCCESSFULLLLLLLLLLLLLLLLL")
        
        
        
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
    ########----------------ADDED LOGS APR 27 -----------####
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'sales2','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")