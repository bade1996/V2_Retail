from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat,col
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
import pandas as pd
from distutils.command.check import check


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
    
    conf = SparkConf().setMaster(smaster).setAppName("Single_Table")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        #.set("spark.driver.cores",8)
        
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Single_Table").getOrCreate()
    sqlctx = SQLContext(sc)
    
    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    #Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
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
    Table1 = "(Select Net_Qty,Txn_Type,Lot_Code,Godown_Code,Branch_Code,Date_ from  stk_dtxn20192020   ) as tb"
    TXN_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
#   BM.show()
    #1 added below
    ###################CHANGED SOURCE  26 FEB
    '''
    TXN = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn2")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Single_Table")
    col_sp = cols.filter(cols.Table_Name=="stk_dtxn").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    TXN = TXN.select(col_sp)
    #end
    TXN.cache()
    #TXN.write.jdbc(url=Postgresurl, table="market99"+".TXN", mode="overwrite", properties=Postgresprop)
    TXN.write.jdbc(url=Postgresurl2, table="market99"+".TXN", mode="overwrite", properties=Postgresprop2)  #WINDOWS
    print("TXN")
    '''
    '''
    Table1 = "(Select Godown_Code from  GoDown_Mst   ) as tb"
    GM_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    '''
#   BM.show()
    #1 added below
    
    GM = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/GoDown_Mst")
    GM=GM.select(GM.godown_code,GM.godown_name,GM.Godown_Type,GM.Map_Code,GM.Excise_Nature,GM.Group_Code1,GM.Group_Code2,GM.Group_Code3,GM.Group_Code4)
    
    #cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Single_Table")
    #col_sp = cols.filter(cols.Table_Name=="GoDown_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    #GM = GM.select(col_sp)
    #end
    #GM.cache()
    #GM.write.jdbc(url=Postgresurl, table="market99"+".GoDown", mode="overwrite", properties=Postgresprop)
    #GM.write.jdbc(url=Postgresurl2, table="market99"+".GoDown", mode="overwrite", properties=Postgresprop2)
    
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    GM.write.jdbc(url=Sqlurlwrite, table="GoDown", mode="overwrite")
    
    print("GM")
    
    #Table1 = "(Select Lot_Code,sp_rate2,Pur_Rate,Sale_Rate,Item_Det_Code,Puv_Code,pur_date,Exp_dmg from  Lot_Mst   ) as tb"
    #Lot_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
#   BM.show()
    #1 added below
    Lot = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Lot_Mst")
    
    #cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Single_Table")
    #col_sp = cols.filter(cols.Table_Name=="Lot_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    #colist=[Lot_Code,Pur_Rate,Sale_Rate,Item_Det_Code,Puv_Code,pur_date,Exp_dmg,lot_number,basic_rate,mrp,sp_rate1,sp_rate2,sp_rate3,sp_rate4,sp_rate5]
    #Lot = Lot.select(col_sp)
    Lot=Lot.select("Lot_Code","pur_rate","sale_rate","Item_Det_Code","Puv_Code","pur_date","Exp_dmg","lot_number","basic_rate",\
                   "mrp","sp_rate1","sp_rate2","sp_rate3","sp_rate4","sp_rate5","Expiry","Mfg_Date","cf_lot")
    
    #######Req for PO Ana
    Lot=Lot.withColumnRenamed("basic_rate","lot_basic_rate")
    Lot=Lot.withColumnRenamed("mrp","lot_mrp")
    #end
    lot_damage = Lot
    #Lot = Lot.drop('Pur_Rate','Sale_Rate')
    Lot = Lot.filter(~(Lot.Puv_Code.like('CS%')))
    Lot = Lot.filter(Lot.Exp_dmg=='N')
    
    #lot_damage.cache()
    Lot.cache()
    
    #lot_damage.write.jdbc(url=Postgresurl, table="market99"+".lot_damage", mode="overwrite", properties=Postgresprop)
    #lot_damage.write.jdbc(url=Postgresurl2, table="market99"+".lot_damage", mode="overwrite", properties=Postgresprop2) #this
    
    Lot.write.mode("overwrite").save(hdfspath+"/"+DBET+"/Stage2/Lot")
    #Lot.write.jdbc(url=Postgresurl, table="market99"+".Lot", mode="overwrite", properties=Postgresprop)
    
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    Lot.write.jdbc(url=Sqlurlwrite, table="Lot", mode="overwrite")
    
    print("Lot")
    
    #Table1 = "(Select Link_Code,Pack_Code from  Pack_Mst   ) as tb"
    #Pack_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
#   BM.show()
#1 added below
    
    Pack = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pack_Mst")
    Pack=Pack.select(Pack.Link_Code,Pack.Pack_code,Pack.Pack_Name,Pack.Size_Type,Pack.Grp_Link_Code)
    
    #cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Single_Table")
    #col_sp = cols.filter(cols.Table_Name=="Pack_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    #Pack = Pack.select(col_sp)
    
    #end
    #Pack.cache()
    #Pack.write.jdbc(url=Postgresurl, table="market99"+".Pack", mode="overwrite", properties=Postgresprop)
    #Pack.write.jdbc(url=Postgresurl2, table="market99"+".Pack", mode="overwrite", properties=Postgresprop2)  #THIS ONE
    
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    Pack.write.jdbc(url=Sqlurlwrite, table="Pack", mode="overwrite")
    
    print("Pack")
    
    #Table1 = "(Select Comp_Code, Comp_Name from  Comp_Mst   ) as tb"
    #Comp_sql = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
#   BM.show()

#1 added below
    Comp = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Comp_Mst")
    cols = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage2/Stage2").filter(col("Script_Name")=="Single_Table")
    col_sp = cols.filter(cols.Table_Name=="Comp_Mst").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    Comp = Comp.select(col_sp)
    #end
    #Comp.write.jdbc(url=Postgresurl, table="market99"+".Comp", mode="overwrite", properties=Postgresprop)
    #Comp.write.jdbc(url=Postgresurl2, table="market99"+".Comp", mode="overwrite", properties=Postgresprop2)  #this one
    
    Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
    Comp.write.jdbc(url=Sqlurlwrite, table="Comp", mode="overwrite")
    print("Comp")
    
    
    #----------------- NEW MASTER TABLES ADD APR 13, 20---------------#
    agents = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Agents_Brokers")
    agents = agents.select("Code","Agent_Name","Comp_Code","Link_Code","Godown_Code","Address_1","Address_2","City_Code","Branch_Code")
    agents.write.jdbc(url=Sqlurlwrite, table="AgentBrokers", mode="overwrite")
    print("AgentBrokers")
    
    comm = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Comm_Calc_Info")
    comm=comm.select("Exchange_Rate","Code","Rate_Per","CommonCode","Commission_P","Labour_u","Tax_1","Adjust_Rs","Agent_Code","Excise_U","Adjustment_U")
    comm.write.jdbc(url=Sqlurlwrite, table="CommCalcInfo", mode="overwrite")
    print("CommCalcInfo")
    
    grp = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Group_Mst")
    grp=grp.select("group_name","group_code","Group_Short_Name","Margin_For_MRP","Parent_Group_Code")
    grp.write.jdbc(url=Sqlurlwrite, table="GroupMst", mode="overwrite")
    print("GroupMst")
    
    city = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/city_")
    city=city.select("city_name","city_code","State_Code","order_","Group_Code1")
    city.write.jdbc(url=Sqlurlwrite, table="City", mode="overwrite")
    print("city_")
    
    tax=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Tax_Regions")
    tax=tax.select("tax_reg_code","tax_reg_name","Exc_Sale_Nature","GST_Type")
    tax.write.jdbc(url=Sqlurlwrite, table="TaxRegions", mode="overwrite")
    print("Tax_Regions")
    
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Single_Table','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':'MultipleTables','BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
    print("WAKKAWAKKKAAAAAAAAAAAAAAAAA Its Time for Africaa")
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Single_Table','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
   
    
