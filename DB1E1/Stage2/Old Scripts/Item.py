#  GrpMstHierarchy new version
  #  RUUNS SIN LOCAL

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat,when,col
import re,os,datetime #,keyring,
import time,sys
from pyspark.sql.types import *
import pandas as pd
from numpy.core._multiarray_umath import empty

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
        
    conf = SparkConf().setMaster(smaster).setAppName("Market99Item")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)     #changed from 'local[8]'
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Market99Item").getOrCreate()
    sqlctx = SQLContext(sc)
    #print(sqlctx)
    #exit()
    
    #Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=LOGICDBS99;user=sa;password=sa@123"
    #Postgresurl = "jdbc:postgresql://localhost:5432/kockpit"
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
    
    
    data = [("0-9", 0),
        ("10-19", 1),
        ("20-29", 2),
        ("30-39", 3),
        ("40-49", 4),
        ("50-59", 5),
        ("60-69", 6),
        ("70-79", 7),
        ("80-89", 8),
        ("90-99", 9),
        ("100-149", 10),
        ("150-199", 11),
        ("200-249", 12),
        ("250-299", 13),
        ("300-349", 14),
        ('350-399', 15),
        ('400-449', 16),
        ('450-499', 17),
        ('500-549', 18),
        ('550-599', 19),
        ('600-649', 20),
        ('650-699', 21),
        ('700-749', 22),
        ('750-799', 23),
        ('800-849', 24),
        ('850-899', 25),
        ('900-999', 26),
        ('1000+', 27)]

# Create a schema for the dataframe
    schema = StructType([
    StructField('Bucket', StringType(), True),
    StructField('Sort', IntegerType(), True)    ])

# Convert list to RDD
    rdd = spark.sparkContext.parallelize(data)

# Create data frame
    SORT = spark.createDataFrame(rdd,schema)
    
    
    '''
    Table1 = "(Select  Code from It_Mst_O_Det  ) as tb"
    IMOD_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #1 added below
    IMOD = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/It_Mst_O_Det")
    IMOD.show()
    #exit()
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Item")
    col_sp = cols.filter(cols.Table_Name=="It_Mst_O_Det").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    IMOD = IMOD.select(col_sp)
    #end
    #IMOD.show()
    '''
    Table1 = "(Select Comm_It_Desc_Code,Item_Hd_Code,Comp_Code,Color_Code,Group_Code,Item_Name_Code  from It_Mst_Hd  ) as tb"
    IMH_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #2 added below
    IMH = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/It_Mst_Hd")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Item")
    col_sp = cols.filter(cols.Table_Name=="It_Mst_Hd").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    IMH = IMH.select(col_sp)
    #end
    '''
    Table1 = "(Select User_Code,Item_Hd_Code,Item_O_Det_Code,Pack_Code,Item_Det_Code,Pur_Rate,Mrp , Cf_1 ,Sale_Rate ,Basic_Rate  from It_Mst_Det  ) as tb"
    IMD_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #3 added below
    IMD = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/It_Mst_Det")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Item")
    col_sp = cols.filter(cols.Table_Name=="It_Mst_Det").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    IMD = IMD.select(col_sp)
    #end
    
    '''
    Table1 = "(Select Item_Name,Item_Name_Code from  It_Mst_Names  ) as tb"
    IMN_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #4 added below
    IMN = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/It_Mst_Names")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Item")
    col_sp = cols.filter(cols.Table_Name=="It_Mst_Names").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    IMN = IMN.select(col_sp)
    #end
    '''
    Table1 = "(Select Code,Item_Desc_M from  Comm_It_Desc   ) as tb"
    CIDsql= sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #5 added below
    CID = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Comm_It_Desc")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Item")
    col_sp = cols.filter(cols.Table_Name=="Comm_It_Desc").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    CID = CID.select(col_sp)
    #end
    '''
    Table1 = "(Select Color_Code from  Item_Color_Mst   ) as tb"
    ICMsql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
    #6 added below
    ICM = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Item_Color_Mst")
    #cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Item")
    #col_sp = cols.filter(cols.Table_Name=="Item_Color_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    ICM = ICM.select("Color_Code","Color_Name","Color_Short_Name")
    ICM.cache()
    #ICM = ICM.select(col_sp)
    #end
    
    cond = [IMD.Item_O_Det_Code==IMOD.Code]
    IMD = IMD.join(IMOD,cond,"inner")
    
    IMD = IMD.withColumnRenamed("Item_Hd_Code","Item_Hd_Code1")
    cond = [IMH.Item_Hd_Code==IMD.Item_Hd_Code1]
    IMH = IMH.join(IMD,cond,"inner")
    
    IMN = IMN.withColumnRenamed("Item_Name_Code","Item_Name_Code1")
    cond = [IMH.Item_Name_Code==IMN.Item_Name_Code1]
    IMH = IMH.join(IMN,cond,"inner")
    
    CID = CID.withColumnRenamed("Code","Code1")
    cond = [IMH.Comm_It_Desc_Code==CID.Code1]
    IMH = IMH.join(CID,cond,"inner")
    
   
    IMH = IMH.withColumn("Pur_Bucket100",when(col("Pur_Rate")<100,lit(100)).otherwise(lit(500)))
    IMH = IMH.withColumn("Pur_Bucket", when( col("Pur_Bucket100")==100,concat(( (col("Pur_Rate")/10).cast(IntegerType())*10).cast(StringType()),lit("-") ,((((col("Pur_Rate")/10)+1).cast(IntegerType())*10)-1).cast(StringType()) ) )\
                         .otherwise(
                             when( col("Pur_Rate")>150,
                                   when( col("Pur_Rate")> 200,
                                         when( col("Pur_Rate")>250,
                                               when( col("Pur_Rate")>300,
                                                     when( col("Pur_Rate")>350,
                                                           when( col("Pur_Rate")>400,
                                                                 when( col("Pur_Rate")>450,
                                                                       when( col("Pur_Rate")>500,
                                                                             when( col("Pur_Rate")>550,
                                                                                   when( col("Pur_Rate")>600,
                                                                                         when( col("Pur_Rate")>650,
                                                                                               when( col("Pur_Rate")>700,
                                                                                                     when( col("Pur_Rate")>750,
                                                                                                           when(col("Pur_Rate")>800,
                                                                                                                when(col("Pur_Rate")>850,
                                                                                                                     when(col("Pur_Rate")>900,
                                                                                                                          when(col("Pur_Rate")>950,
                                                                                                                                when(col("Pur_Rate")>1000,
                                                                                                                                     lit("1000+")
                                                                                                                                     ).otherwise(lit("950-9999"))#lit("1000+")
                                                                                                                               ).otherwise(lit("900-949"))
                                                                                                                          ).otherwise(lit("850-899"))
                                                                                                                     ).otherwise(lit("800-849"))
                                                                                                                ).otherwise(lit("750-799"))
                                                                                                           ).otherwise(lit("700-749"))
                                                                                                     ).otherwise(lit("650-699"))
                                                                                               ).otherwise(lit("600-649"))
                                                                                         ).otherwise(lit("550-599"))
                                                                                   ).otherwise(lit("500-549"))
                                                                             ).otherwise(lit("450-499"))
                                                                       ).otherwise(lit("400-449"))
                                                                 ).otherwise(lit("350-399"))
                                                           ).otherwise(lit("300-349"))
                                                   ).otherwise(lit("250-299"))
                                               ).otherwise(lit("200-249"))
                                         ).otherwise(lit("150-199")) 
                                   ).otherwise(lit("100-149"))
                             
                             
                             ) )
    
    #IM.filter(IMD.Pur_Rate>=500).show()
    #IMD.show(1000,False)
    #exit()
    
    cond = [IMH.Pur_Bucket==SORT.Bucket]
    IMH = IMH.join(SORT,cond,"left")
    
    ##-----------------Join with Item_Color_Mst Apr13,20----------##
    #cond1=[IMH.Color_Code==ICM.Color_Code]
    IMH=IMH.join(ICM,"Color_Code","left")
    
    IMH.cache()
    
    ###### IMH Added to HDFS as Item since required in Stage2 as told by Prince
    IMH.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/Item")
    #IMH.write.jdbc(url=Postgresurl, table="market99"+".item", mode="overwrite", properties=Postgresprop)
    #IMH.write.jdbc(url=Postgresurl2, table="market99"+".item", mode="overwrite", properties=Postgresprop2)   ##WINDOWS
    
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    IMH.write.jdbc(url=Sqlurlwrite, table="item", mode="overwrite")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Item','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(IMH.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print("Success")    
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Item','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
   
    
