from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf,year as yearf , month as monthf,when, substring as suf
from pyspark.sql.types import *
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime, timedelta
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
conf = SparkConf().setMaster(smaster).setAppName("PO")\
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
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)

print(datetime.now())
try:
    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PO")
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PO") !=0):
        PO = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseOrder")
        PO = PO.select('Item_Det_Code', 'Tot_Qty','Pend_Qty','Rate','Cancel_Item','Branch_Code','Order_No','Order_Date','Valid_Date','Tot_Tax','Order_Type','Code',\
                    'Tax','Extra_Percent_1','Cancel_Order','PO_MRP')
        print (PO.count())
        #PO = PO.filter(PO['Cancel_Item'] != 'true')
        # PO = PO.filter(PO['Code']== 492735)
        # PO.show()
        # exit()
        PO = PO.groupby('Item_Det_Code','Pend_Qty','Rate','Branch_Code','Order_No','Cancel_Item','Order_Date','Valid_Date','Tot_Tax','Order_Type','Code',\
                    'Tax','Extra_Percent_1','Cancel_Order','PO_MRP').agg({'Tot_Qty':'sum'})
        PO = PO.withColumnRenamed('sum(Tot_Qty)','P_Qty')
        # PO.filter(PO['Code']== 494024).show()
        # exit()
        
        POLinks = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUPOLinks")
        POLinks = POLinks.select('Code','Qty_Adjust')
        # POLinks = POLinks.filter(POLinks['Code']== 492650)
        # POLinks.show()
        # exit()
        POLinks = POLinks.groupby('Code').agg({'Qty_Adjust':'sum'})
        POLinks = POLinks.withColumnRenamed('sum(Qty_Adjust)','R_Qty')
        PO = PO.join(POLinks,'Code','left')
        PO = PO.fillna(0, subset=['R_Qty'])
        PO = PO.withColumn('QTY',when(PO['Cancel_Item']==0,(PO['P_Qty'] - PO['R_Qty'])).otherwise(0))
        #PO.filter(PO['Code']== 494024).filter(PO['Order_No'] == 'POM-21192').show()
        # exit()serializable
                
        
        Pur = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase")
        #Pur.filter(Pur['Order_Item_Code']== 494024).filter(Pur['vouch_code'].isin(9050,9102)).show()
        #exit()
        Pur = Pur.select('Bill_No','Bill_Date','GRN_Number','Lot_Code','Calc_Tax_3','Calc_Tax_1','Challan_Code','Deleted_',\
                         'Cust_Code','Net_Amt','GRN_PreFix','Calc_Net_Amt','Calc_Gross_Amt','Order_Item_Code','Calc_commission',\
                         'calc_rdf','calc_scheme_u','calc_scheme_rs','Calc_Tax_2','calc_sur_on_tax3','calc_mfees','calc_excise_u',\
                          'Calc_adjustment_u','Calc_adjust_rs','Calc_freight','calc_adjust','Calc_Spdisc','Calc_DN','Calc_CN',\
                          'Calc_Display','Calc_Handling','calc_Postage','calc_round','calc_sp_commission','vouch_code','Tot_Qty')  
        Pur=Pur.filter((Pur.Order_Item_Code > 0)&(Pur.Challan_Code == 0))\
                                .filter(Pur.Deleted_== 0)
                                
        #Pur=Pur.withColumn('NET',PO['calc_sp_commission']*PO['Calc_Gross_Amt'])
        Pur= Pur.groupby('Bill_No','Bill_Date','GRN_Number','Lot_Code','Calc_Tax_3','Calc_Tax_1','Challan_Code','Deleted_',\
                         'Cust_Code','GRN_PreFix','Calc_Net_Amt','Calc_Gross_Amt','Order_Item_Code','Calc_commission',\
                         'calc_rdf','calc_scheme_u','calc_scheme_rs','Calc_Tax_2','calc_sur_on_tax3','calc_mfees','calc_excise_u',\
                          'Calc_adjustment_u','Calc_adjust_rs','Calc_freight','calc_adjust','Calc_Spdisc','Calc_DN','Calc_CN',\
                          'Calc_Display','Calc_Handling','calc_Postage','calc_round','calc_sp_commission','vouch_code','Tot_Qty')\
                .agg(sum('Net_Amt').alias('Net_Amt'))
                         
        # Pur.filter(Pur['Order_Item_Code']== 494024).filter(Pur['vouch_code'].isin(9050,9102)).show()
        # exit().withColumn('CancelPOQty',when((PO['Cancel_Item']== 1),0).otherwise(PO['Pend_Qty']))\
        PO = PO.join(Pur,PO.Code==Pur.Order_Item_Code,how='left')
        # PO.filter(PO['Order_Item_Code']== 494024).filter(PO['vouch_code'].isin(9050,9102)).show()
        # exit()'POQty',when((PO['Order_Type'] == 'PI'),0).otherwise(PO['Tot_Qty'])
        PO=PO.withColumn('POQty',when((PO['Order_Type'] == 'PO'),PO['P_Qty']).otherwise(0))\
                                .withColumn('PendPOQTY',when((PO['Cancel_Item']==0),(PO['Pend_Qty'])).otherwise(0))\
                                .withColumn('OrderAmount',PO['P_Qty'] * PO['Rate'])\
                                .withColumn('CancelPOQty',when((PO['Cancel_Item']== 1),PO['Pend_Qty']).otherwise(0))\
                                .withColumn('FillRateQty',(PO['R_Qty']/PO['P_Qty'])*100)\
                                .withColumn('userDefinedNetAmount','0' + PO['Calc_Gross_Amt'] + PO['Calc_commission'] + PO['calc_sp_commission'] + PO['calc_rdf'] + PO['calc_scheme_u'] + PO['calc_scheme_rs'] + PO['Calc_Tax_1'] + PO['Calc_Tax_2'] + PO['Calc_Tax_3'] + PO['calc_sur_on_tax3'] + PO['calc_mfees'] + PO['calc_excise_u'] + PO['Calc_adjustment_u'] + PO['Calc_adjust_rs'] + PO['Calc_freight'] + PO['calc_adjust'] + PO['Calc_Spdisc'] + PO['Calc_DN'] + PO['Calc_CN'] + PO['Calc_Display'] + PO['Calc_Handling'] + PO['calc_Postage'] + PO['calc_round'])  
        
        PO=PO.drop('Challan_Code').drop('Deleted_').drop('Calc_commission').drop('Cancel_Item').drop('Cancel_Order')\
                         .drop('calc_rdf').drop('calc_scheme_u').drop('calc_scheme_rs').drop('Calc_Tax_2').drop('calc_sur_on_tax3').drop('calc_mfees').drop('calc_excise_u')\
                          .drop('Calc_adjustment_u').drop('Calc_adjust_rs').drop('Calc_freight').drop('calc_adjust').drop('Calc_Spdisc').drop('Calc_DN').drop('Calc_CN')\
                          .drop('Calc_Display').drop('Calc_Handling').drop('calc_Postage').drop('calc_round').drop('calc_sp_commission')
        # PO.filter(PO['Order_No'] == 'POM-21192').filter(PO['Code'] == 494024).show()
        # #PO.show()
        # exit()
                               
        #Pur = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseOrder")                     
        #PO = PO.filter(PO['QTY'] != 0)
        # print(PO.count())
        # PO = PO.filter(PO['Code']==495664)
        # PO.show()
        # exit()
        PO.coalesce(1).write.mode("append").save(hdfspath+"/Market/Stage2/PO")
        write_data_sql(PO,"PO",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PO','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(PO.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PO','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")
        
