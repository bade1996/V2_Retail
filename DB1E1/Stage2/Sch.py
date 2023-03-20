from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import sum as sumf, first as firstf, month as monthf
from pyspark.sql.types import *
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
import re,os,datetime
import time,sys
from datetime import datetime
import pyodbc



#Datelog = datetime.datetime.now().strftime('%Y-%m-%d')

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
    
def delete_data(dqry):
    server = '103.234.187.190:2499' 
    database = 'KOCKPIT' 
    username = 'sa' 
    password = 'Market!@999' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute(dqry)
    cnxn.commit()

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
conf = SparkConf().setMaster(smaster).setAppName("Scheme")\
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .set("spark.executor.cores","3")\
                .set("spark.executor.memory","4g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").config("spark.driver.maxResultSize", "0")\
                            .config("spark.executor.instances", "20").config("spark.default.parallelism=200").getOrCreate()
sqlctx = SQLContext(sc)


SCH = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Scheme_Campaign_Mst")

SCGM = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Scheme_Campaign_Group_Mst")



def RENAME(df,columns):
    if isinstance(columns, dict):
            for old_name, new_name in columns.items():
                        df = df.withColumnRenamed(old_name, new_name)
            return df
    else:
            raise ValueError("'columns'should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
   




SCH=SCH.select('Code','Scheme_Campaign_Name')

SCGM=SCGM.select('Code','Scheme_Group_Name','Scheme_Campaign_Code')
SCGM=RENAME(SCGM,{"Code":"SCGM_Code"})


 
Start_Year = 2018

print('ReportScheme start: ',datetime.now())
'''
##ReportScheme_I
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/ReportScheme") !=0):
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLSCH")
        SLTXN.cache()
        if(SLTXN.count() > 0):
            
            SLTXN = SLTXN.join(SCGM,SLTXN.Sch_Det_Code==SCGM.SCGM_Code,how='left' )
            SLTXN = SLTXN.join(SCH,SLTXN.Scheme_Campaign_Code==SCH.Code,how='left' )
            
            SLTXN = SLTXN.filter(SLTXN.Deleted== 0)
            
            
            SLTXN=SLTXN.withColumn('SaleQty',when((SLTXN['Type'] == 'SL'),(SLTXN['Tot_Qty'])).otherwise(SLTXN['Tot_Qty']* -1))\
                    .withColumn('Gross_Value',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Gross_Amt'])).otherwise(SLTXN['Calc_Gross_Amt']* -1))\
                    .withColumn('NetSale_Value',when((SLTXN['Type'] == 'SL'),(SLTXN['NET_SALE_VALUE'])).otherwise(SLTXN['NET_SALE_VALUE']* -1))\
                    .withColumn('CD1',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Commission'])).otherwise(SLTXN['Calc_Commission']* -1))\
                    .withColumn('TD1',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Sp_Commission'])).otherwise(SLTXN['Calc_Sp_Commission']* -1))\
                    .withColumn('UserDefNetAmt1',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'])).otherwise('0' + SLTXN['Calc_Gross_Amt']* -1))\
                    .withColumn('UserDefNetAmt2',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_round'])).otherwise(('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_round'])* -1))
                        
            SLTXN=SLTXN.groupby('Vouch_Num','Cust_Code','CD','TD','CALC_CD','CALC_TD','SCGM_Code','Sch_Det_Code','Vouch_Date','vouch_code','YearMonth','Calc_Gross_Amt',\
                                'Item_Det_Code','Branch_Code', 'Scheme_Campaign_Name', 'Scheme_Group_Name','Lot_Code')\
                       .agg(avg('Net_Amt').alias('Avg_BillAmt'),sum('SaleQty').alias('SaleQty'),sum('Gross_Value').alias('Gross_Value'),sum('NetSale_Value').alias('NetSale_Value'),\
                            sum('CD1').alias('CD1'),sum('TD1').alias('TD1'),sum('UserDefNetAmt1').alias('UserDefNetAmt1'),sum('UserDefNetAmt2').alias('UserDefNetAmt2'))
    
    
            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/ReportScheme")
            print("ReportScheme Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ReportScheme','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLTXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
            log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            write_data_sql(log_df,"Logs",mode="append")
    else:
        for i in range(0,3):
            month =  datetime.today().month - i
            if(month <= 3):
                Start_Year = (datetime.today().year) - 1
                End_Year = (datetime.today().year)
            else:
                Start_Year = (datetime.today().year) 
                End_Year = int(datetime.today().year) + 1
            FY = str(Start_Year)+str(End_Year)
            try:
                cdm = str(datetime.today().year)
                if(month == 0):
                    month = 12
                    cdm = str(datetime.today().year - 1)
                if(month == -1):
                    month = 11
                    cdm = str(datetime.today().year - 1)
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                print(cdm)
                SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLSCH/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/ReportScheme/YearMonth="+cdm)
                
                SLTXN = SLTXN.join(SCGM,SLTXN.Sch_Det_Code==SCGM.SCGM_Code,how='left' )
                SLTXN = SLTXN.join(SCH,SLTXN.Scheme_Campaign_Code==SCH.Code,how='left' )
                
                SLTXN = SLTXN.filter(SLTXN.Deleted== 0)
                
                SLTXN = SLTXN.withColumn('YearMonth',lit(cdm)) 
                SLTXN=SLTXN.withColumn('SaleQty',when((SLTXN['Type'] == 'SL'),(SLTXN['Tot_Qty'])).otherwise(SLTXN['Tot_Qty']* -1))\
                        .withColumn('Gross_Value',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Gross_Amt'])).otherwise(SLTXN['Calc_Gross_Amt']* -1))\
                        .withColumn('NetSale_Value',when((SLTXN['Type'] == 'SL'),(SLTXN['NET_SALE_VALUE'])).otherwise(SLTXN['NET_SALE_VALUE']* -1))\
                        .withColumn('CD1',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Commission'])).otherwise(SLTXN['Calc_Commission']* -1))\
                        .withColumn('TD1',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Sp_Commission'])).otherwise(SLTXN['Calc_Sp_Commission']* -1))\
                        .withColumn('UserDefNetAmt1',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'])).otherwise('0' + SLTXN['Calc_Gross_Amt']* -1))\
                        .withColumn('UserDefNetAmt2',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_round'])).otherwise(('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_round'])* -1))
                            
                SLTXN=SLTXN.groupby('Vouch_Num','Cust_Code','CD','TD','CALC_CD','CALC_TD','SCGM_Code','Sch_Det_Code','Vouch_Date','vouch_code','YearMonth','Calc_Gross_Amt',\
                                'Item_Det_Code','Branch_Code', 'Scheme_Campaign_Name', 'Scheme_Group_Name','Lot_Code')\
                       .agg(avg('Net_Amt').alias('Avg_BillAmt'),sum('SaleQty').alias('SaleQty'),sum('Gross_Value').alias('Gross_Value'),sum('NetSale_Value').alias('NetSale_Value'),\
                            sum('CD1').alias('CD1'),sum('TD1').alias('TD1'),sum('UserDefNetAmt1').alias('UserDefNetAmt1'),sum('UserDefNetAmt2').alias('UserDefNetAmt2'))
                #SLTXN.show(19)
                SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/ReportScheme")
                
                
            except Exception as e:
                print(e)
        #SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/ReportScheme_I")
        #SLTXN.show(10,False)
        print(datetime.now())
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/ReportScheme")
        SLTXN = SLTXN.drop("YearMonth")
        write_data_sql(SLTXN,"ReportScheme",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ReportScheme','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLTXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ReportScheme','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  

'''

#pricedropsalereport
print('PriceDropSaleReport start: ',datetime.now())
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PriceDropSaleReport") !=0):
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PriceDrop")
        SLTXN.cache()
        if(SLTXN.count() > 0):
            #SLTXN = SLTXN.join(SS,on = ['Code'],how='left' )
                #SLTXN = SLTXN.join(SS,on = ['Code'],how='left' )
                        
            SLTXN=SLTXN.filter(SLTXN.Stock_Trans == 0)\
                       .filter(SLTXN.Deleted== 0)
                       
                       
            SLTXN=SLTXN.withColumn('SaleQty',when((SLTXN['Type'] == 'SL'),(SLTXN['Tot_Qty'])).otherwise(SLTXN['Tot_Qty']* -1)/1 )
                    
            SLTXN=SLTXN.withColumn('TotQty',when((SLTXN['Type'] == 'SL'),((SLTXN['Tot_Qty'] + SLTXN['Free_Qty'] + SLTXN['Repl_Qty'] + SLTXN['Sample_Qty']))).otherwise(((SLTXN['Tot_Qty'] + SLTXN['Free_Qty'] + SLTXN['Repl_Qty'] + SLTXN['Sample_Qty'])* -1)/1 ))\
                            .withColumn('TD1',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Sp_Commission'])).otherwise(SLTXN['Calc_Sp_Commission']* -1))\
                            .withColumn('Bill_Amt',when((SLTXN['Type'] == 'SL'),(SLTXN['NET_SALE_VALUE'])).otherwise(SLTXN['NET_SALE_VALUE']* -1))\
                            .withColumn('UDNetAmt1',when((SLTXN['Type'] == 'SL'),('0')).otherwise('0') * -1).withColumn('Month',monthf(SLTXN.Vouch_Date))
            SLTXN=SLTXN.withColumn('UDNetAmt2',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour']* -1))\
                            .withColumn('UDNetAmt3',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour']* -1))
                                
            SLTXN=SLTXN.groupby('CALC_TD','Lot_Code','Cust_Code','TD','Vouch_Date','vouch_code','YearMonth','Item_Det_Code','Month','Branch_Code')\
                           .agg(sum('SaleQty').alias('SaleQty'),sum('TotQty').alias('TotQty'),sum('TD1').alias('TD1'),sum('Bill_Amt').alias('Bill_Amt'),\
                                sum('UDNetAmt1').alias('UDNetAmt1'),sum('UDNetAmt2').alias('UDNetAmt2'),sum('UDNetAmt3').alias('UDNetAmt3'))
                
            
            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PriceDropSaleReport")
            print("PriceDropSaleReport Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PriceDropSaleReport','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLTXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
            log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            write_data_sql(log_df,"Logs",mode="append")
    else:
        for i in range(0,3):
            month =  datetime.today().month - i
            if(month <= 3):
                Start_Year = (datetime.today().year) - 1
                End_Year = (datetime.today().year)
            else:
                Start_Year = (datetime.today().year) 
                End_Year = int(datetime.today().year) + 1
            FY = str(Start_Year)+str(End_Year)
            try:
                cdm = str(datetime.today().year)
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PriceDrop/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PriceDropSaleReport/YearMonth="+cdm)
                
                
                #SLTXN = SLTXN.join(SS,on = ['Code'],how='left' )
                #SLTXN = SLTXN.join(SS,on = ['Code'],how='left' )
                        
                SLTXN=SLTXN.filter(SLTXN.Stock_Trans == 0)\
                       .filter(SLTXN.Deleted== 0)
                       
                       
                SLTXN=SLTXN.withColumn('SaleQty',when((SLTXN['Type'] == 'SL'),(SLTXN['Tot_Qty'])).otherwise(SLTXN['Tot_Qty']* -1)/1 )
                    
                SLTXN=SLTXN.withColumn('TotQty',when((SLTXN['Type'] == 'SL'),((SLTXN['Tot_Qty'] + SLTXN['Free_Qty'] + SLTXN['Repl_Qty'] + SLTXN['Sample_Qty']))).otherwise(((SLTXN['Tot_Qty'] + SLTXN['Free_Qty'] + SLTXN['Repl_Qty'] + SLTXN['Sample_Qty'])* -1)/1 ))\
                            .withColumn('TD1',when((SLTXN['Type'] == 'SL'),(SLTXN['Calc_Sp_Commission'])).otherwise(SLTXN['Calc_Sp_Commission']* -1))\
                            .withColumn('Bill_Amt',when((SLTXN['Type'] == 'SL'),(SLTXN['NET_SALE_VALUE'])).otherwise(SLTXN['NET_SALE_VALUE']* -1))\
                            .withColumn('UDNetAmt1',when((SLTXN['Type'] == 'SL'),('0')).otherwise('0') * -1).withColumn('Month',monthf(SLTXN.Vouch_Date))
                SLTXN=SLTXN.withColumn('UDNetAmt2',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour']* -1))\
                            .withColumn('UDNetAmt3',when((SLTXN['Type'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Total_GST_Amount_Paid'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour']* -1))
                            
                SLTXN = SLTXN.withColumn('YearMonth',lit(cdm))  
                
                SLTXN=SLTXN.groupby('CALC_TD','Lot_Code','Cust_Code','TD','Vouch_Date','vouch_code','YearMonth','Item_Det_Code','Month','Branch_Code')\
                           .agg(sum('SaleQty').alias('SaleQty'),sum('TotQty').alias('TotQty'),sum('TD1').alias('TD1'),sum('Bill_Amt').alias('Bill_Amt'),\
                                sum('UDNetAmt1').alias('UDNetAmt1'),sum('UDNetAmt2').alias('UDNetAmt2'),sum('UDNetAmt3').alias('UDNetAmt3'))
                
                SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PriceDropSaleReport")
                
                print("PriceDropSaleReport Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PriceDropSaleReport")
        SLTXN = SLTXN.drop("YearMonth")
        #SLTXN.show(1)
        write_data_sql(SLTXN,"PriceDropSaleReport",owmode)
        print('PriceDropSaleReport end: ', datetime.now())
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PriceDropSaleReport','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLTXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PriceDropSaleReport','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  



