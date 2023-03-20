#### NOT NEEDED MAR 2

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,lit,concat_ws,concat,to_date,round
from pyspark.sql.types import *
import re,os,datetime
import time,sys
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import last_day,month,year,lit,concat,when,col

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
            
    conf = SparkConf().setMaster('local[15]').setAppName('Damages')\
        .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")
        
    sc = SparkContext(conf=conf)
    sqlctx = SQLContext(sc)
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    connection_string="Driver="+ODBC13+";Server="+SURLCUR+","+PORTCUR+";Database="+WDB+";uid="+RUSER+";pwd="+RPASS
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
    '''
    ser_query = "(SELECT series AS Series,\
                number AS Number,\
                series_code AS Series_Code,\
                type AS Type,\
                visible AS Visible,\
                Branch_Code,\
                Store_Code,\
                Group_Code1,\
                Group_Code2,\
                Group_Code3,\
                Group_Code4,\
                Group_Code5 \
                FROM bill_ser) AS bill_ser"
    ser_sql = sqlctx.read.format("jdbc")\
            .options(url=Sqlurl, dbtable=ser_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .load()
    ser_sql.show()
    '''
    #1 added below
    ser = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/bill_ser")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Damages")
    col_sp = cols.filter(cols.Table_Name=="bill_ser").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    ser = ser.select(col_sp)
    ser=ser.withColumnRenamed("series","Series").withColumnRenamed("number","Number").withColumnRenamed("series_code","Series_Code")\
                .withColumnRenamed("type","Type").withColumnRenamed("visible","Visible").withColumnRenamed("Store_code","Store_Code")
    
    #end
    '''
    ser_grp_query = "(SELECT Group_Code,\
                    Group_Name,\
                    Level_V,\
                    Level_H,\
                    HGroup_Code \
                    FROM Bill_Ser_Groups) AS ser_grp"
    ser_grp_sql = sqlctx.read.format("jdbc")\
                .options(url=Sqlurl, dbtable=ser_grp_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                .load()
    ser_grp_sql.show()
    '''
    #2 added below
    ser_grp = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Bill_Ser_Groups")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Damages")
    col_sp = cols.filter(cols.Table_Name=="Bill_Ser_Groups").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    ser_grp = ser_grp.select(col_sp)
    
    #end
    join_cond = [ser.Group_Code1==ser_grp.Group_Code]
    damages = ser.join(ser_grp,join_cond,'left').drop('Group_Code')
    
    damages.cache()
    #damages.write.jdbc(url=Postgresurl, table="market99"+".damages", mode="overwrite", properties=Postgresprop)
    #damages.write.jdbc(url=Postgresurl2, table="market99"+".damages", mode="overwrite", properties=Postgresprop2)  #WINDOWS
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    damages.write.jdbc(url=Sqlurlwrite, table="damages", mode="overwrite")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'damages','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(damages.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print('jaiHO')
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'damages','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
   