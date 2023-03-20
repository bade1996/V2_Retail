from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import when,lit,concat_ws,concat,to_date,round,col
from pyspark.sql.types import *
import re,os,datetime#,keyring
import time,sys
import pandas as pd

now = datetime.datetime.now()
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

    conf = SparkConf().setMaster('local[15]').setAppName('Scheme')
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
    sch_query = "(SELECT Scheme_Campaign_Name,\
                Code AS Scheme_Campaign_Code\
                FROM Scheme_Campaign_Mst) AS sch"
    sch = sqlctx.read.format("jdbc")\
            .options(url=Sqlurl, dbtable=sch_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .load()
    '''
    sch = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Scheme_Campaign_Mst")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="scheme")
    col_sp = cols.filter(cols.Table_Name=="Scheme_Campaign_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    sch = sch.select(col_sp)
    sch=sch.withColumnRenamed("Code","Scheme_Campaign_Code")
    #end            
    '''       
    scgm_query = "(SELECT Scheme_Group_Name,\
                Code AS Scheme_Campaign_Grp_Code,\
                Scheme_Campaign_Code AS Scheme_Campaign_Code_Drop\
                FROM Scheme_Campaign_Group_Mst) AS scgm"
    scgm = sqlctx.read.format("jdbc")\
            .options(url=Sqlurl, dbtable=scgm_query,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .load()
    '''
    scgm=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Scheme_Campaign_Group_Mst")
    
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="scheme")
    col_sp = cols.filter(cols.Table_Name=="Scheme_Campaign_Group_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    scgm = scgm.select(col_sp)
    scgm=scgm.withColumnRenamed("Code","Scheme_Campaign_Grp_Code").withColumnRenamed("Scheme_Campaign_Code","Scheme_Campaign_Code_Drop")
    #end            
    sch_scgm_cond = [sch.Scheme_Campaign_Code==scgm.Scheme_Campaign_Code_Drop]
    scheme = sch.join(scgm,sch_scgm_cond,'left')\
                    .drop('Scheme_Campaign_Code_Drop')
#     scheme.show(5)
#     exit()
    scheme.cache()
    
    scheme.write.jdbc(url=Sqlurlwrite, table="scheme", mode="overwrite")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'scheme','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(scheme.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print('jaiHooooooooooooooooooooooooo')
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'scheme','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
