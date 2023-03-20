from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf, trim as trimf, year as yearf
from pyspark.sql.types import *
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os, pandas as pd
from datetime import datetime
import time,sys

#For reading data from the SQL server
def read_data_sql(table_string):
    database = "LOGICDBS99"
    user = "sa"
    password  = "Market!@999"
    SQLurl = "jdbc:sqlserver://103.234.187.190:2499;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

def write_data_sql(df,name,mode):
    database = "KOCKPIT"
    user = "sa"
    password  = "Market!@999"
    SQLurl = "jdbc:sqlserver://103.234.187.190:2499;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df.write.jdbc(url = SQLurl , table = name, properties = SQLprop, mode = mode)
    
stime = time.time()
Datelog = datetime.now().strftime('%Y-%m-%d')
start_time = datetime.now()#.strftime('%H:%M:%S')

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


config = os.path.dirname(os.path.realpath(__file__))
Market = config[config.rfind("/")+1:]
Etn = Market[Market.rfind("E"):]   
DB = Market[:Market.rfind("E")]
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("Master")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()
sqlctx = SQLContext(sc)

#print(datetime.now())


try:
    branch = read_data_sql("(Select bm.Group_Code6 as Region,bm.Group_Code7,bm.Branch_Name,bm.Area_SqFT ,bm.Branch_Code from Branch_Mst as bm inner join Branch_Groups as bg on bm.Group_Code1 = bg.Group_Code where bm.Group_Code7 = 62) as branch")
    branch_ = read_data_sql("(Select bm.Branch_Code, bg.Group_Name from Branch_Mst as bm left join Branch_Groups as bg on bm.Group_Code6 = bg.Group_Code where bm.Group_Code7 = 62) as branch")
    branch = branch.join(branch_, 'Branch_Code', 'left')
    branch = branch.filter(branch.Branch_Code>=0)
    branch = branch.withColumn('Branch_Name',trimf(branch['Branch_Name']))
    branch = branch.withColumn('Branch_Name',regexp_replace(col("Branch_Name"), "[\n\r]", ""))
    branch = branch.withColumn('Branch_Name',regexp_replace(col("Branch_Name"), "[.]", ""))
    branch = branch.withColumn("Market99_Store/OLD/NNGR",when(col("Branch_Code")>=5000,lit("Market99 Store")).otherwise(when(col("Group_Code7")==63,lit("NNGR Stores"))\
                           .otherwise(when(col("Group_Code7")==61,lit("Old Stores")).otherwise("Market99 Store")))  )
    branch = branch.withColumn("Store_Sort_Order",when(col("Branch_Code")>=5000,lit(0)).otherwise(when(col("Group_Code7")==63,lit(2))\
                           .otherwise(when(col("Group_Code7")==61,lit(1)).otherwise(0))))
    branch = branch.drop('Group_Code7')
    branch.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Branch")
    write_data_sql(branch,"Branch",owmode)
    print("Branch Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Branch','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(branch.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Branch','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")


try:
    lot = read_data_sql("(SELECT item_det_code as Bill_No,Item_Det_Code,godown_code,sp_rate1,sp_rate2,lot_code,lot_number,Org_Lot_Code,Mfg_Date,Pur_Date,exp_dmg,CF_Lot,Expiry from Lot_Mst) as lot")
    
    lot_same = read_data_sql("(SELECT DISTINCT Org_Lot_Code, mrp as mrp_lot ,pur_rate as pur_rate_lot ,sale_rate as sale_rate_lot ,basic_rate as basic_rate_lot from Lot_Mst where lot_code = Org_Lot_Code) as lot")
    
    lot = lot.join(lot_same, 'Org_Lot_Code', 'left')
    lot = lot.drop('Org_Lot_Code')
    # lot.show()
    # exit()
    lot.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Lot")
    write_data_sql(lot,"Lot",owmode)
    print("Lot Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Lot','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(lot.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Lot','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")


###item
try:
    item = read_data_sql("(Select  imh.item_hd_name,CID.Item_Desc_M ,imh.Color_Code,imh.item_hd_code,imd.Item_Det_Code,imh.Group_Code_13, imh.Comp_Code,imd.User_Code as Item_Code,imd.Pack_Code, imn.Item_Name, imd.Pur_Rate, imd.Mrp as MRP, imd.Sale_Rate, imd.Basic_Rate from It_Mst_O_Det imod inner join It_Mst_Det imd on imod.Code = imd.Item_O_Det_Code inner join It_Mst_Hd imh on imh.item_hd_code = imd.Item_Hd_Code inner join It_Mst_Names imn on imn.Item_Name_Code = imh.Item_Name_Code inner join Comm_It_Desc CID on imh.Comm_It_Desc_Code = CID.Code) as item ")
    com_mst = read_data_sql("(Select Comp_Code, Comp_Name as Company_Name from  Comp_Mst) as cm")
    item = item.join(com_mst,"Comp_Code","left")
    item = item.drop('Comp_Code')
    
    item = item.withColumn("Pur_Bucket100",when(col("Pur_Rate")<100,lit(100)).otherwise(lit(500)))
    item = item.withColumn("Pur_Bucket", when( col("Pur_Bucket100")==100,concat(( (col("Pur_Rate")/10).cast(IntegerType())*10).cast(StringType()),lit("-") ,((((col("Pur_Rate")/10)+1).cast(IntegerType())*10)-1).cast(StringType()) ) )\
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
        
    
    item = item.drop('Pur_Bucket100')
    
    GM = read_data_sql("(Select group_code,group_name,level_h from  Group_Mst where group_name != '(NIL)') as gm")
    GM = GM.withColumn("level_h",GM['level_h'].cast(IntegerType()))
    
    IMH = read_data_sql("(Select item_hd_code, group_code as Group_Code1, Group_Code2, Group_Code3, Group_Code4,Group_Code5 ,Group_Code7, Group_Code8, Group_Code11, Group_Code_12 as Group_Code12, Group_Code16, Group_Code20, Group_Code21, Group_Code13 from  It_Mst_Hd) as imh")
    cond= [(IMH["Group_Code1"]==GM["group_code"]) | (IMH["Group_Code2"]==GM["group_code"]) | (IMH["Group_Code3"]==GM["group_code"]) | (IMH["Group_Code4"]==GM["group_code"]) | (IMH["Group_Code5"]==GM["group_code"]) | (IMH["Group_Code7"]==GM["group_code"]) | (IMH["Group_Code8"]==GM["group_code"]) | (IMH["Group_Code11"]==GM["group_code"]) | (IMH["Group_Code12"]==GM["group_code"]) | (IMH["Group_Code16"]==GM["group_code"]) | (IMH["Group_Code20"]==GM["group_code"]) | (IMH["Group_Code21"]==GM["group_code"]) | (IMH["Group_Code13"]==GM["group_code"])]
    
    GM = GM.join(IMH,cond,"inner")
    GM = GM.drop("Group_Code1","Group_Code2","Group_Code3","Group_Code4","Group_Code7","Group_Code5","Group_Code8","Group_Code11","Group_Code12","Group_Code16","Group_Code20","Group_Code21","Group_Code13")
    GM = GM.groupBy('item_hd_code').pivot('level_h').agg(firstf("group_name").alias("Group_Name"))###on row
    GM = GM.withColumnRenamed("1","DEPARTMENT").withColumnRenamed("2","CATEGORY").withColumnRenamed("3","SUB_CATEGORY").withColumnRenamed("4","MATERIAL").withColumnRenamed("7","Price_Range").withColumnRenamed("8","GST_CATEGORY")\
                    .withColumnRenamed("11","ACTIVE_INACTIVE").withColumnRenamed("12","IMP_LOCAL").withColumnRenamed("16","LAUNCH_MONTH").withColumnRenamed("20","NEW_COMPANY_NAME")\
                    .withColumnRenamed("21","SUB_CLASS").withColumnRenamed("5","Brand").withColumnRenamed("13","Item_Type")
    
    item = item.join(GM, 'item_hd_code','left')
    item = item.drop('item_hd_code')
    
    Department_Code = read_data_sql("(Select group_code as Department_Code,group_name as DEPARTMENT from  Group_Mst where group_name != '(NIL)' and level_h = 1) as gm")
    item = item.join(Department_Code, 'DEPARTMENT', 'left')
    Pack_Mst= read_data_sql("(Select Pack_Code , Pack_Name , Link_Code,Pack_Short_Name,Order_ from  Pack_Mst )as pm")
    item = item.join(Pack_Mst, 'Pack_Code', 'left')
    Item_Color_Mst = read_data_sql("(Select Color_Name ,Color_Code from  Item_Color_Mst )as ICM")
    item = item.join(Item_Color_Mst, 'Color_Code', 'left')
    
    item.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Item")
    write_data_sql(item,"Item",owmode)
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Item','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(item.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    print("Item Done")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Item','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")


#godown
try:
    Godown = read_data_sql("(SELECT a.godown_code, a.godown_name, a.godown_short_name, a.Godown_Type, b.Group_Name FROM LOGICDBS99.dbo.Godown_Mst a left join LOGICDBS99.dbo.Godown_Groups b on a.Group_Code1 = b.Group_Code) as gd")
    Godown.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Godown")
    write_data_sql(Godown,"Godown",owmode)
    print("Godown Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Godown','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(Godown.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Godown','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")

try:
    Cashier = read_data_sql("(SELECT Code,Cashier_Name FROM LOGICDBS99.dbo.SL_Cashier_Mst) as gd")
    Cashier.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Cashier")
    write_data_sql(Cashier,"Cashier",owmode)
    print("Cashier Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Cashier','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(Cashier.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Cashier','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")

try:
    Accounts= read_data_sql("(SELECT a.ST_Branch_Code,a.act_name,a.act_type,a.act_code,a.City_Act_Name,a.Address_1,a.Print_Act_Name,a.Remarks_1,a.Remarks_2,a.Remarks_3,a.grp_code,\
                             a.City_Code,a.agent_code,a.User_Code,a.Map_Code,a.Sp_Discount,a.Branch_Code,\
                               Ct.city_name FROM Accounts a inner join City_ Ct on a.City_Code = Ct.City_Code  ) as ACT")
    AccountGroups= read_data_sql("(Select Grp_Name,grp_code from  AccountGroups )as ag")
    Accounts = Accounts.join(AccountGroups, 'grp_code', 'left')
    
    Accounts.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Accounts")
    write_data_sql(Accounts,"Accounts",owmode)
    print("Accounts Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Accounts','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(Accounts.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Accounts','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    
 
#Date
try:
    Date_range = pd.DataFrame(pd.date_range(start='1/1/2018', periods=100, freq='MS'),columns = ['SD'])
    SD = spark.createDataFrame(Date_range)
    SD = SD.withColumn("SD",SD['SD'].cast(DateType()))
    SD = SD.withColumn("Key", yearf(SD['SD'])*100+date_format(SD['SD'],'MM'))
    SD = SD.withColumn('Key',SD['Key'].cast(IntegerType()))
    SD = SD.withColumn('Key',SD['Key'].cast(StringType()))
    Date_range = pd.DataFrame(pd.date_range(start='1/1/2018', periods=100, freq='M'),columns = ['ED'])
    ED = spark.createDataFrame(Date_range)
    ED = ED.withColumn("ED",ED['ED'].cast(DateType()))
    ED = ED.withColumn("Key", yearf(ED['ED'])*100+date_format(ED['ED'],'MM'))
    ED = ED.withColumn('Key',ED['Key'].cast(IntegerType()))
    ED = ED.withColumn('Key',ED['Key'].cast(StringType()))       
    SD = SD.join(ED,'Key','left')
    SD.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/DateFormat")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'DateFormat','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(SD.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    print(datetime.now())
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'DateFormat','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")

try:
    BillSerial = read_data_sql("(SELECT a.series,number,a.series_code,a.type,a.visible,a.Branch_Code,a.Store_code,\
                                a.Group_Code2,a.Group_Code3,a.Group_Code4,a.Group_Code5,b.Group_Name,b.Level_V,b.Level_H,\
                                b.HGroup_Code  FROM bill_ser a left join Bill_Ser_Groups b on a.Group_Code1=b.Group_Code) as BS")
    BillSerial.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/BillSerial")
    
    print("BillSerial Done")
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'BillSerial','DB':DB,'EN':Etn,
        'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(BillSerial.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    print(datetime.now())
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'BillSerial','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
    


RetailFootfall =read_data_sql("(SELECT * from Retail_FootFalls) as RF")
RetailFootfall.show()
RetailFootfall.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/RetailFootfall")
print("RetailFootfall Done")   
 
    
Discount_Coupon_Mst = read_data_sql("(SELECT Sale_Fin_Year,Sale_vouch_code,CD_Rs,Discount_Coupon_Prefix,Code,Discount_Coupon_Number,Expiry_Date,CD_P,Discount_Coupon_Barcode,Discount_Coupon_Group_Code,Discount_Coupon_Num from Discount_Coupon_Mst) as CDM")
Discount_Coupon_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Discount_Coupon_Mst")
print("Discount_Coupon_Mst Done")  

Store_Mst =read_data_sql("(SELECT Code from Store_Mst) as SM")
Store_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Store_Mst")
print("Store_Mst Done")  

sup_det=read_data_sql("(SELECT * FROM sup_det ) as SD")
sup_det.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/sup_det")
print("sup_det Done")    
    
    
CCMst=read_data_sql("(SELECT CC_No_Code ,CC_Party_Name FROM CC_No_Mst) as Ccms")
CCMst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/CCMst")
print("CCMst Done")    
    
Tax_Regions = read_data_sql("(SELECT Tax_Reg_Name,Tax_Reg_Code From Tax_Regions) as TR")
Tax_Regions.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Tax_Regions")
print("Tax_Regions Done")
    
Setup_Masters = read_data_sql("(SELECT Mst_Name, Code,General_Text_1  from Setup_Masters) as SM")
Setup_Masters.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Setup_Masters")
print("Setup_Masters Done")  

Scheme_Campaign_Group_Mst = read_data_sql("(SELECT Code,Scheme_Group_Name,Scheme_Campaign_Code  from Scheme_Campaign_Group_Mst) as SCGM")
Scheme_Campaign_Group_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Scheme_Campaign_Group_Mst")
print("Scheme_Campaign_Group_Mst Done")    


Scheme_Campaign_Mst = read_data_sql("(SELECT Code,Scheme_Campaign_Name,Date_From,Date_To  from Scheme_Campaign_Mst) as SCM")
Scheme_Campaign_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Scheme_Campaign_Mst")
print("Scheme_Campaign_Mst Done")  

Comm_Calc_Info= read_data_sql("(SELECT Excise_U,Code,Commission_P,Sp_Commission_P ,Rdf_P,Exchange_Rate,Agent_Code,Sl_Act_Code,STax_Act_Code,Sur_Act_Code,STax_Act_Code_1,\
                        ST_Branch_Code,Tax_1,Tax_2,Tax_3,Scheme_Rs,Scheme_U,Adjust_Rs from Comm_Calc_Info) as CCI")
Comm_Calc_Info.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage2/Comm_Calc_Info")  
print("Comm_Calc_Info Done")   
 
    
print(datetime.now())
