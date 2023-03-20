##Done 24 Jan
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat,when,col
import re,os,datetime
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
    
    #added above
    conf = SparkConf().setMaster('local[8]').setAppName("M99_Branch")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)

    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Market99_Branch").getOrCreate()
    sqlctx = SQLContext(sc)
    
    #Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=LOGICDBS99;user=sa;password=sa@123"
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    
    #Postgresurl = "jdbc:postgresql://localhost:5432/kockpit"
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
    ## password changed from sa@123  24 feb
    '''
    Table1 = "(Select Branch_Name,Show_ST_As_Sale,Branch_Code, Branch_Code As BranchCode,Order_ As B_Order,Group_Code1,Group_Code7 from Branch_Mst) as tb"
    BM_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    BM_sql.show()
    '''
    #1 added below
    BM = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Branch_Mst")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Branch")
    col_sp = cols.filter(cols.Table_Name=="Branch_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    BM = BM.select(col_sp)
    BM= BM.withColumnRenamed("Branch_Code","BranchCode").withColumnRenamed("Order_","B_Order")
    BM = BM.withColumn("Branch_Code",BM["BranchCode"])
    BM.show()
    #exit()
    
    '''
    Table = "(Select Group_Code from Branch_Groups) as tb"
    BG_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    BG_sql.show()
    '''
    #1 added below
    BG = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Branch_Groups")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Branch")
    col_sp = cols.filter(cols.Table_Name=="Branch_Groups").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    BG = BG.select(col_sp)
    
    
    cond = [BM.Group_Code1==BG.Group_Code]
    
    bm = BM.join(BG,cond,'inner')
    #bm.show()
    #bm = bm.withColumn("WH/All Stores",when(col("Branch_Code")==5001,lit("Warehouse")).otherwise(lit("All Store")))
    #bm = bm.withColumn("flag",bm.Branch_Code/5000)
    #bm = bm.withColumn("market99/old Stores",when(col("Branch_Code")>=5000,lit("Market99 Store")).otherwise(lit("Old Store/nngr")))
    bm = bm.filter(bm.Branch_Code>=0)
    bm = bm.withColumn("Market99_Store/OLD/NNGR",when(col("Branch_Code")>=5000,lit("Market99 Store")).otherwise(when(col("Group_Code7")==63,lit("NNGR Stores"))\
                       .otherwise(when(col("Group_Code7")==61,lit("Old Stores")).otherwise("Market99 Store")))  )
    
    bm = bm.withColumn("Store_Sort_Order",when(col("Branch_Code")>=5000,lit(0)).otherwise(when(col("Group_Code7")==63,lit(2))\
                       .otherwise(when(col("Group_Code7")==61,lit(1)).otherwise(0)))) 
    bm.cache()
    
    bm.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/Branch")

    bm.write.jdbc(url=Sqlurlwrite, table="Branch", mode="overwrite")
    ########----------------ADDED LOGS APR 27 -----------####
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Branch','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(bm.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print("*******************************************"+"Success")
except Exception as ex:
    print(ex)
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Branch','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
