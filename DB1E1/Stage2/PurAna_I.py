
####Script of IngPOanalysis and POAnalysisS2
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
    server = '103.234.187.190,2499' 
    database = 'KOCKPIT' 
    username = 'sa' 
    password = 'Market!@999' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
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
conf = SparkConf().setMaster(smaster).setAppName("PurAnalysis")\
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
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)


PO = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseOrder")
POPULink = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUPOLinks")
  
POPULink=POPULink.select('Code','Qty_Adjust')

PO = PO.select('Item_Det_Code', 'Tot_Qty','Pend_Qty','Rate','Cancel_Item','Branch_Code','Order_No','Order_Date','Valid_Date','Tot_Tax','Order_Type','Code',\
                    'Tax','Extra_Percent_1')
PO = PO.withColumnRenamed("Tot_Qty",'TotQty').withColumnRenamed("Rate",'Rate2').withColumnRenamed("Branch_Code",'BranchCode').withColumnRenamed("Item_Det_Code",'ItemDetCode').withColumnRenamed("Tot_Tax",'TotTax')
#PO = PO.withColumn('b',when((PO['Order_Type'] == 'PI'),'0').otherwise(PO['TotQty']))
#PUH1= PUH1.select('gr_date','gr_number','vouch_code')


PO = PO.join(POPULink,on=['Code'],how='left' )

print(datetime.now())
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PurAnalysis") !=0):
        PurOrder = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase")
        PurOrder.cache()
        if(PurOrder.count() > 0):
            
            PurOrder = PurOrder.join(PO,PurOrder.Order_Item_Code==PO.Code,how='left' )
            
            
            PurOrder=PurOrder.filter((PurOrder.Order_Item_Code > 0)&(PurOrder.Challan_Code == 0))\
                                .filter(PurOrder.Deleted_== 0)
                                
            # PurOrder=PurOrder.select('Pend_Qty','Item_Det_Code',)
            
            # PurOrder=PurOrder.withColumn('PendingPOQty',when((PurOrder['Cancel_Item']==0),(PurOrder['Pend_Qty'])).otherwise(0))
            # PurOrder=PurOrder.groupby('Pend_Qty','Item_Det_Code').agg(sum('PendingPOQty').alias('Pendingqty'))
            # PurOrder.show()
            # exit()       
            #
            PurOrder=PurOrder.withColumn('POQty',when((PurOrder['Order_Type'] == 'PI'),0).otherwise(PurOrder['TotQty']))\
                                .withColumn('PendPOQTY',when((PurOrder['Cancel_Item']==0),(PurOrder['TotQty']-PurOrder['Qty_Adjust'])).otherwise(0))\
                                .withColumn('OrderAmount',PurOrder['TotQty'] * PurOrder['Rate2'])\
                                .withColumn('PendingPOQty',when((PurOrder['Cancel_Item']==0),(PurOrder['Pend_Qty'])).otherwise(0))\
                                .withColumn('CancelPOQty',when(((PurOrder['Order_Type'] == 'PI') | (PurOrder['Cancel_Item']== 0)),0).otherwise(PurOrder['Pend_Qty']))\
                                .withColumn('FillRateQty',((PurOrder['Tot_Qty'])/(when((PurOrder['Order_Type'] == 'PI'),0).otherwise(PurOrder['TotQty'])))*100)\
                                .withColumn('userDefinedNetAmount','0' + PurOrder['Calc_Gross_Amt'] + PurOrder['Calc_commission'] + PurOrder['calc_sp_commission'] + PurOrder['calc_rdf'] + PurOrder['calc_scheme_u'] + PurOrder['calc_scheme_rs'] + PurOrder['Calc_Tax_1'] + PurOrder['Calc_Tax_2'] + PurOrder['Calc_Tax_3'] + PurOrder['calc_sur_on_tax3'] + PurOrder['calc_mfees'] + PurOrder['calc_excise_u'] + PurOrder['Calc_adjustment_u'] + PurOrder['Calc_adjust_rs'] + PurOrder['Calc_freight'] + PurOrder['calc_adjust'] + PurOrder['Calc_Spdisc'] + PurOrder['Calc_DN'] + PurOrder['Calc_CN'] + PurOrder['Calc_Display'] + PurOrder['Calc_Handling'] + PurOrder['calc_Postage'] + PurOrder['calc_round'])  
                                
                                
            PurOrder=PurOrder.groupby('Qty_Adjust','YearMonth','Vouch_Date','Order_No' ,'Order_Date','Valid_Date','Bill_No','Bill_Date','GRN_Number','BranchCode',\
                                      'Cust_Code','Tax','Extra_Percent_1','Net_Amt','GRN_PreFix','Item_Det_Code','Lot_Code','TotQty','Calc_Tax_3','Calc_Tax_1',\
                                      'Rate','TotTax','Calc_Net_Amt','Calc_Gross_Amt','vouch_code')\
                              .agg(sum('FillRateQty').alias('FillRateQty'),sum('POQty').alias('POQty'),sum('OrderAmount').alias('OrderAmount'),\
                                sum('PendingPOQty').alias('PendingPOQty'),sum('CancelPOQty').alias('CancelPOQty'),sum('PendPOQTY').alias('PendPOQTY'),sum('userDefinedNetAmount').alias('userDefinedNetAmount'))
            #PurOrder.show(10)PendingPOQty
            #exit()
            
            PurOrder.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurAnalysis")
            print("PurAnalysis Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurAnalysis','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(PurOrder.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                PurOrder = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PurAnalysis/YearMonth="+cdm)
                
                PurOrder = PurOrder.join(PO,PurOrder.Order_Item_Code==PO.Code,how='left' )
            
                PurOrder=PurOrder.filter((PurOrder.Order_Item_Code> 0)&(PurOrder.Challan_Code == 0)).filter(PurOrder.Deleted_== 0)
                PurOrder = PurOrder.withColumn('YearMonth',lit(cdm))
                
                PurOrder=PurOrder.withColumn('POQty',when((PurOrder['Order_Type'] == 'PI'),0).otherwise(PurOrder['TotQty']))\
                                .withColumn('PendPOQTY',when((PurOrder['Cancel_Item']==0),(PurOrder['TotQty']-PurOrder['Qty_Adjust'])).otherwise(0))\
                                .withColumn('OrderAmount',PurOrder['TotQty'] * PurOrder['Rate2'])\
                                .withColumn('PendingPOQty',when((PurOrder['Cancel_Item']==0),(PurOrder['Pend_Qty'])).otherwise(0))\
                                .withColumn('CancelPOQty',when(((PurOrder['Order_Type'] == 'PI') | (PurOrder['Cancel_Item']== 0)),0).otherwise(PurOrder['Pend_Qty']))\
                                .withColumn('FillRateQty',((PurOrder['Tot_Qty'])/(when((PurOrder['Order_Type'] == 'PI'),0).otherwise(PurOrder['TotQty'])))*100)\
                                .withColumn('userDefinedNetAmount','0' + PurOrder['Calc_Gross_Amt'] + PurOrder['Calc_commission'] + PurOrder['calc_sp_commission'] + PurOrder['calc_rdf'] + PurOrder['calc_scheme_u'] + PurOrder['calc_scheme_rs'] + PurOrder['Calc_Tax_1'] + PurOrder['Calc_Tax_2'] + PurOrder['Calc_Tax_3'] + PurOrder['calc_sur_on_tax3'] + PurOrder['calc_mfees'] + PurOrder['calc_excise_u'] + PurOrder['Calc_adjustment_u'] + PurOrder['Calc_adjust_rs'] + PurOrder['Calc_freight'] + PurOrder['calc_adjust'] + PurOrder['Calc_Spdisc'] + PurOrder['Calc_DN'] + PurOrder['Calc_CN'] + PurOrder['Calc_Display'] + PurOrder['Calc_Handling'] + PurOrder['calc_Postage'] + PurOrder['calc_round'])  
                                #.withColumn('PendingPOQty',when((PurOrder['Cancel_Item']==0),(PurOrder['Pend_Qty'])).otherwise(0))\
                                
                                    
                PurOrder=PurOrder.groupby('Qty_Adjust','YearMonth','Vouch_Date','Order_No' ,'Order_Date','Valid_Date','Bill_No','Bill_Date','GRN_Number','BranchCode',\
                                      'Cust_Code','Tax','Extra_Percent_1','Net_Amt','GRN_PreFix','Item_Det_Code','Lot_Code','TotQty','Calc_Tax_3','Calc_Tax_1',\
                                      'Rate','TotTax','Calc_Net_Amt','Calc_Gross_Amt','vouch_code')\
                              .agg(sum('FillRateQty').alias('FillRateQty'),sum('POQty').alias('POQty'),sum('OrderAmount').alias('OrderAmount'),\
                                sum('PendingPOQty').alias('PendingPOQty'),sum('CancelPOQty').alias('CancelPOQty'),sum('PendPOQTY').alias('PendPOQTY'),sum('userDefinedNetAmount').alias('userDefinedNetAmount'))
                
                
                #PurOrder.show(10)
                PurOrder.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurAnalysis")
                delete_data(''' DELETE FROM KOCKPIT.dbo.PurAnalysis WHERE YearMonth = ''' + cdm)
                print("PurAnalysis Done")             
                              
            except Exception as e:
                print(e)
        print(datetime.now())
        PurOrder.show()
        PurOrder = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PurAnalysis")
        PurOrder = PurOrder.drop("YearMonth")
        write_data_sql(PurOrder,"PurAnalysis",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurAnalysis','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(PurOrder.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurAnalysis','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  

print("\U0001F637")