
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat,col
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
    
    conf = SparkConf().setMaster('local[15]').setAppName("Party")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)

    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Party").getOrCreate()
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
    Table1 = "(Select act_name as Party_Name,\
                act_type as Party_type,\
                act_code as Party_Code,\
                City_Act_Name as Party_Detail,\
                Address_1 as Party_Address,\
                Print_Act_Name as Print_Name \
                from Accounts) as tb"
    PMsql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #1 added below
    PM = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Accounts")
    PM=PM.select("act_name","act_type","act_code","City_Act_Name","Address_1","Print_Act_Name","Remarks_1","Remarks_2","Remarks_3","grp_code","City_Code",\
                 "agent_code","User_Code","Map_Code","Sp_Discount","Branch_Code")
    
    #cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="party")
    #col_sp = cols.filter(cols.Table_Name=="Accounts").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    #PM = PM.select(col_sp)
    PM=PM.withColumnRenamed("act_name","Party_Name").withColumnRenamed("act_type","Party_type").withColumnRenamed("act_code","Party_Code")\
            .withColumnRenamed("City_Act_Name","Party_Detail").withColumnRenamed("Address_1","Party_Address").withColumnRenamed("Print_Act_Name","Print_Name")
    #end
    PM.cache()
    #PM.write.jdbc(url=Postgresurl, table="market99"+".party", mode="overwrite", properties=Postgresprop)
    #PM.write.jdbc(url=Postgresurl2, table="market99"+".party", mode="overwrite", properties=Postgresprop2)  #WINDOWS
    
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    PM.write.jdbc(url=Sqlurlwrite, table="party", mode="overwrite")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Party','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(PM.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print("SUCCESSSSSSSSSSSSSSSSSSSSSSSSSSS")
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Party','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
   
    