#  GrpMstHierarchy new version
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat,col
import re,os,datetime#,keyring
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
    #Added above 27-january-2020-TA0081
    conf = SparkConf().setMaster("local[*]").setAppName("Market99GrpMistHierarchy")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Market99GrpMistHierarchy").getOrCreate()
    sqlctx = SQLContext(sc)
    
    #Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=LOGICDBS99;user=sa;password=sa@123"
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"  
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    connection_string="Driver="+ODBC13+";Server="+SURLCUR+","+PORTCUR+";Database="+WDB+";uid="+RUSER+";pwd="+RPASS  

    #Postgresurl = "jdbc:postgresql://localhost:5432/kockpit"
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
    Table1 = "(Select group_code,group_name,level_h from  Group_Mst   ) as tb"
    GM_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    GM_sql.show()
    '''
    #1 added below
    GM = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Group_Mst")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="GrpMistHierarchy")
    col_sp = cols.filter(cols.Table_Name=="Group_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    GM = GM.select(col_sp)
    #end
    '''
    Table1 = "(Select * from  It_Mst_Hd   ) as tb"
    #Table1 = "(Select * from  It_Mst_Hd  where item_hd_code=69214 ) as tb"
    IMH_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    IMH_sql.show()
    '''
    #2 added below
    IMH = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/It_Mst_Hd")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="GrpMistHierarchy")
    col_sp = cols.filter(cols.Table_Name=="It_Mst_Hd").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    #IMH = IMH.select(col_sp)
    #end
    
    IMH = IMH.withColumnRenamed("group_code","Group_Code1")
    IMH.show()
    #exit()
    cond= [(IMH["Group_Code8"]==GM.group_code) | (IMH["Group_Code4"]==GM.group_code)\
            | (IMH.Group_Code1== GM.group_code)\
            | (IMH["Group_Code2"]==GM.group_code)\
            | (IMH["Group_Code3"]==GM.group_code)|\
             (IMH["Group_Code11"]==GM.group_code)\
            | (IMH["Group_Code_12"]==GM.group_code)|\
             (IMH["Group_Code16"]==GM.group_code)|\
              (IMH["Group_Code20"]==GM.group_code)|\
              (IMH["Group_Code21"]==GM.group_code)|\
              (IMH["Group_Code7"]==GM.group_code)\
             ]
    
    
    GM = GM.join(IMH,cond,"inner")
    
#     print("Check1")
#     GM.filter(GM.item_hd_code==69214)#.show()
    
#     exit()
    GM = GM.drop("Group_Code12")
    GM = GM.withColumnRenamed("Group_Code_12","Group_Code12")
    level = GM.select(GM.level_h.cast("integer")).distinct().rdd.flatMap(lambda x:x).collect()
   # print(level)
    code = GM.select(GM.item_hd_code).distinct().rdd.flatMap(lambda x:x).collect()

    level.sort()
    #print(level)
   
 #   print(level)
    
    #gm1 =GM.filter((GM.item_hd_code==11310) & (GM.group_name!="(NIL)"))
    gm1 =GM.filter( (GM.group_name!="(NIL)"))
#     print("Check2")
#     gm1.filter(gm1.item_hd_code==69214).show()
    
#     gm1.filter(gm1.group_code==4594).show()
#     gm1.filter(gm1.item_hd_code==54588).show()
#     gm1.filter(gm1.item_hd_code==63187).show()
#     gm1.filter(gm1.item_hd_code==63399).show()
#     gm1.filter(gm1.item_hd_code==6663).show()
#     exit()
    gm1 = gm1.filter(gm1.level_h==1)
#     print("Check3")
#     gm1.filter(gm1.item_hd_code==69214)#.show()
    
    #gm1.show()
#     gm1.filter(gm1.group_code==4594).show()
#     gm1.filter(gm1.item_hd_code==54588|gm1.item_hd_code==63187|\
#                gm1.item_hd_code==63399|gm1.item_hd_code==6663).show()
#      
#     exit()
#     
#     
  
#     print("Check1")
#     gm1.show()
#      
    
    level=level[1:]
    print(level)
    #exit()
    print("level2",level)
    
    for i in level:
        gm2 = GM.filter(GM.level_h==i).select("group_name","item_hd_code")
        #gm2 =gm2.filter(gm2.item_hd_code==11310) 
        print("group_name"+str(int(i)))
#         if int(i)==3:
#             gm2.filter(gm2.group_name=='BABY WIPES').show()
#             exit()
        gm2 = gm2.withColumnRenamed("group_name","group_name"+str(int(i))).withColumnRenamed("item_hd_code","item_hd_code1")
        #gm2.show()
        #print(gm2.head(1))
        #print(type(gm2.head(1)))
        if gm2.head(1):
            #gm1 = gm1.join(gm2,[gm1["item_hd_code"]==gm2["item_hd_code1"]],"inner")
            gm1 = gm1.join(gm2,[gm1["item_hd_code"]==gm2["item_hd_code1"]],"left") 
            gm1 = gm1.drop("item_hd_code1")
            #gm1.show()
            print("In Loop")
            gm1.filter(gm1.item_hd_code==69214)#.show()
            print(i)
        
        if not gm2.head(1):
            print("fail")
      
    #gm1.show()
#     gm1.filter(gm1.item_hd_code==63187).show()
#     print("DOne")
    #exit()
#     print("Check2")
#     gm1.show()
#     exit()
#     print("Check4")
#     gm1.filter(gm1.item_hd_code==69214)#.show()
    
    gm1.cache()
    #gm1.show()
    
    gm1 = gm1.withColumnRenamed("group_name","DEPARTMENT").withColumnRenamed("group_name2","CATEGORY").withColumnRenamed("group_name3","SUB CATEGORY")\
    .withColumnRenamed("group_name11","ACTIVE / INACTIVE").withColumnRenamed("group_name4","MATERIAL").withColumnRenamed("group_name12","IMP / LOCAL")\
    .withColumnRenamed("group_name16","LAUNCH MONTH").withColumnRenamed("group_name20","NEW COMPANY NAME")\
    .withColumnRenamed("User_Code","ITEM CODE").withColumnRenamed("Item_Desc_M","ITEM DESCRIPTION").withColumnRenamed("group_name8","GST CATEGORY")\
    .withColumnRenamed("group_name21","SUB CLASS").withColumnRenamed("group_name7","Price Range")
    
#     print("Check5")
#     gm1.filter(gm1.item_hd_code==69214)#.show()
    
    #gm1.write.jdbc(url=Postgresurl, table="market99"+".GMHierarchy", mode="overwrite", properties=Postgresprop)
    #gm1.write.jdbc(url=Postgresurl2, table="market99"+".GMHierarchy", mode="overwrite", properties=Postgresprop2)
    #gm1.show()
    #exit()
    gm1.cache()
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    gm1.write.jdbc(url=Sqlurlwrite, table="GMHierarchy", mode="overwrite")
    
#     gm1=gm1.withColumnRenamed("SUB CATEGORY","SUBCATEGORY")
#     gm1.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/GMHierarchy")
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'GrpMstHierarchy','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(gm1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")

    print("Success")
#     gm1.show()

    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'GrpMstHierarchy','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
