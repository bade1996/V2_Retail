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
conf = SparkConf().setMaster(smaster).setAppName("Stotransfer")\
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                    .set("spark.executor.cores","3")\
                .set("spark.executor.memory","4g")\
                .set("spark.driver.maxResultSize","20g")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.local.dir", "/tmp/spark-temp")\
                    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)


TR= sqlctx.read.parquet(hdfspath+"/Market/Stage2/Tax_Regions")
CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Comm_Calc_Info")
#SL = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLHead1")

LM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")

def RENAME(df,columns):
    if isinstance(columns, dict):
            for old_name, new_name in columns.items():
                        df = df.withColumnRenamed(old_name, new_name)
            return df
    else:
            raise ValueError("'columns'should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
   



TR = TR.select('Tax_Reg_Name','Tax_Reg_Code')
#SL=SL.select('remarks3','remarks2','Vouch1','remarks1').withColumnRenamed("Vouch1","vouch_code")
LM1= LM1.select('basic_rate_lot', 'Expiry','Lot_Code','Bill_No','mrp_lot',\
        'sale_rate_lot','pur_rate_lot')

CCI=CCI.select('Code','Exchange_Rate')


print(datetime.now())
Start_Year = 2018


try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/StockTransferInwardDetailed") !=0):
        PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase")
        PUTXN.cache()
        if(PUTXN.count() > 0):
            PUTXN = PUTXN.join(CCI,PUTXN.Comm_Calc_Code==CCI.Code,how='left')
            #PUTXN.show(1)
    
            PUTXN=PUTXN.withColumn('Rate_Per_Pack',  PUTXN['Rate'] * PUTXN['CF_Qty'])\
                        .withColumn('Tax3',  PUTXN['Calc_Tax_3'] + PUTXN['Calc_Sur_On_Tax3'])\
                        .withColumn('TotQty',PUTXN['Tot_Qty']/1).withColumn('TotQty_1',PUTXN['Tot_Qty']/1)\
                        .withColumn('PurQty',PUTXN['Tot_Qty']/1).withColumn('FreeQty',PUTXN['Free_Qty']/1)\
                        .withColumn('ReplQty',PUTXN['Repl_Qty']/1).withColumn('SampleQty',PUTXN['Sample_Qty']/1)\
                        .withColumn('UserDefNetAmt_1','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] +PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                        .withColumn('UserDefNetAmt_2','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] + PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])
                        #.withColumn('UDR_Amount',  LM['basic_rate_lot'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_2',  LM['basic_rate_lot'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_3',  LM['basic_rate_lot'] * PUTXN['Tot_Qty']) 
    
            PUTXN = PUTXN.withColumn('MonthOrder', when(month(PUTXN['vouch_date']) < 4, month(PUTXN['vouch_date']) + 12).otherwise(month(PUTXN['vouch_date'])))\
                        .withColumn('DayOrder', dayofmonth(PUTXN['vouch_date']))\
                        .withColumn('Year', year(PUTXN['vouch_date']))
           
            PUTXN = PUTXN.filter(PUTXN.Stock_Trans == 1).filter(PUTXN.Deleted_ == 0)
            
            
            #PUTXN.show(1)
            
            PUTXN=PUTXN.groupby('vouch_code','YearMonth','Cust_Code','CF_Qty','Rate','Pur_Or_PR','Vouch_Num','Goods_In_Transit',  PUTXN['Bill_No'], 'Bill_Date', 'GRN_PreFix', 'GRN_Number',\
                                'godown_Code','Lot_Code','Branch_Code','Bill_Amount','Net_Amt','Exchange_Rate','Item_Det_Code','vouch_date','MonthOrder','DayOrder','Year')\
                        .agg(sum('Tax3').alias('Tax3'),\
                                sum('ReplQty').alias('ReplQty'),sum('SampleQty').alias('SampleQty'),sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                sum("Rate_Per_Pack").alias("Rate_Per_Pack"),sum('Qty_Weight').alias("Qty_Weight"),sum('TotQty').alias("TotQty"),sum('TotQty_1').alias('TotQty_1'),sum('PurQty').alias('PurQty'),\
                                sum('FreeQty').alias('FreeQty'),sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Calc_Net_Amt'),\
                                sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                                sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'))
            print(PUTXN.count())
            #PUTXN.show(1)
            #PUTXN=PUTXN.orderBy('Branch_Name', 'vouch_date','Vouch_Num',  'Lot_Code',  'Godown_Name',  'Act_Name',  'Group_Name', 'User_Code', 'Item_Hd_Name', 'Order_', 'Pack_Name', 'Comp_Name','Item_Desc', 'GRN_PreFix', 'GRN_Number')
    
            PUTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransferInwardDetailed")
            print("StockTransferInwardDetailed Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransferInwardDetailed','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(PUTXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                    cdm = cdm + '0'+ str(month)
                else:
                    cdm = cdm + str(month)
                PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/StockTransferInwardDetailed/YearMonth="+cdm)
                
                PUTXN = PUTXN.join(CCI,PUTXN.Comm_Calc_Code==CCI.Code,how='left')
                
                PUTXN=PUTXN.withColumn('Rate_Per_Pack',  PUTXN['Rate'] * PUTXN['CF_Qty'])\
                            .withColumn('Tax3',  PUTXN['Calc_Tax_3'] + PUTXN['Calc_Sur_On_Tax3'])\
                            .withColumn('TotQty',PUTXN['Tot_Qty']/1).withColumn('TotQty_1',PUTXN['Tot_Qty']/1)\
                            .withColumn('PurQty',PUTXN['Tot_Qty']/1).withColumn('FreeQty',PUTXN['Free_Qty']/1)\
                            .withColumn('ReplQty',PUTXN['Repl_Qty']/1).withColumn('SampleQty',PUTXN['Sample_Qty']/1)\
                            .withColumn('UserDefNetAmt_1','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] +PUTXN['calc_scheme_rs'] + PUTXN['calc_tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                            .withColumn('UserDefNetAmt_2','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] + PUTXN['calc_scheme_rs'] + PUTXN['calc_tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])
                #print('data1')
                PUTXN = PUTXN.withColumn('MonthOrder', when(monthf(PUTXN['vouch_date']) < 4, monthf(PUTXN['vouch_date']) + 12).otherwise(monthf(PUTXN['vouch_date'])))\
                            .withColumn('DayOrder', dayofmonth(PUTXN['vouch_date']))\
                            .withColumn('Year', year(PUTXN['vouch_date']))
                PUTXN = PUTXN.filter(PUTXN.Stock_Trans == 1).filter(PUTXN.Deleted_ == 0)
                #print('data2')
                PUTXN = PUTXN.withColumn('YearMonth',lit(cdm))
                
                PUTXN=PUTXN.groupby('vouch_code','YearMonth','Cust_Code','CF_Qty','Rate','Pur_Or_PR','Vouch_Num','Goods_In_Transit',  PUTXN['Bill_No'], 'Bill_Date', 'GRN_PreFix', 'GRN_Number',\
                                'godown_Code','Lot_Code','Branch_Code','Bill_Amount','Net_Amt','Exchange_Rate','Item_Det_Code','vouch_date','MonthOrder','DayOrder','Year')\
                                .agg(sum('Tax3').alias('Tax3'),\
                                    sum('ReplQty').alias('ReplQty'),sum('SampleQty').alias('SampleQty'),sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                    sum("Rate_Per_Pack").alias("Rate_Per_Pack"),sum('Qty_Weight').alias("Qty_Weight"),sum('TotQty').alias("TotQty"),sum('TotQty_1').alias('TotQty_1'),sum('PurQty').alias('PurQty'),\
                                    sum('FreeQty').alias('FreeQty'),sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Calc_Net_Amt'),\
                                    sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                    sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                    sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                    sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                    sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                                    sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'))
                #print('data4')
                #PUTXN=PUTXN.orderBy('Branch_Name', 'vouch_date','Vouch_Num',  'Lot_Code',  'Godown_Name',  'Act_Name',  'Group_Name', 'User_Code', 'Item_Hd_Name', 'Order_', 'Pack_Name', 'Comp_Name','Item_Desc', 'GRN_PreFix', 'GRN_Number')
    
                PUTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransferInwardDetailed")
                #delete_data(' DELETE FROM KOCKPIT.dbo.StockTransferInwardDetailed WHERE YearMonth = ' + cdm)
                print("StockTransferInwardDetailed Done")
            except Exception as e:
                print(e)
                #exit()
        print(datetime.now())
        PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/StockTransferInwardDetailed")
        PUTXN = PUTXN.drop("YearMonth")
        #PUTXN.show(10,False)
        write_data_sql(PUTXN,"StockTransferInwardDetailed",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransferInwardDetailed','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(PUTXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransferInwardDetailed','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  
 


try:   
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/StockTransferOUTDetailed") !=0):
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLHead1")
        # SLTXN.show()
        # exit()
        SLTXN.cache()
        if(SLTXN.count() > 0):
            SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left')
            SLTXN = SLTXN.join(LM1,on =['Lot_Code'],how='left')
            SLTXN = SLTXN.join(TR,on = ['Tax_Reg_Code'],how='left')
            #SLTXN = SLTXN.join(SL,on = ['vouch_code'],how='left')    
    
            SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn('SaleQty',SLTXN['Tot_Qty']/1).withColumn('NetPUAmount',LM1['pur_rate_lot']*SLTXN['Tot_Qty']).withColumn('NetSPAmount',LM1['mrp_lot']*SLTXN['Tot_Qty']).withColumn('MarkDownOnGrossValue',LM1['mrp_lot']-SLTXN['Rate'])\
                        .withColumn('NetRatePerUnit',(SLTXN['NET_SALE_VALUE']/SLTXN['Tot_Qty'])).withColumn('DefinedMarkdown',((SLTXN['used_cf_rate']-SLTXN['Rate'])/SLTXN['used_cf_rate'])*100).withColumn('MarkDownOnNetValue',((SLTXN['used_cf_rate']-(SLTXN['NET_SALE_VALUE']/SLTXN['Tot_Qty']))/SLTXN['used_cf_rate'])*100)\
                    .withColumn('Free_Value',  LM1['basic_rate_lot'] * SLTXN['Free_Qty']).withColumn('SampleQty',SLTXN['Sample_Qty']/1).withColumn('Rate_Per_Pack',SLTXN['Rate'] * SLTXN['CF_Qty'])\
                    .withColumn('UDR_Amount',  LM1['sale_rate_lot'] * SLTXN['Tot_Qty']).withColumn('Repl_Value',  LM1['basic_rate_lot'] * SLTXN['Repl_Qty']).withColumn('Sample_Value',  LM1['basic_rate_lot'] * SLTXN['Sample_Qty'])\
                    .withColumn('UDR_Amount_1',  LM1['sale_rate_lot'] * SLTXN['Tot_Qty']).withColumn('Rate_Per_Pack',  SLTXN['Rate'] * SLTXN['CF_Qty']).withColumn('Int_Total_Packs',  SLTXN['Tot_Qty'] / SLTXN['CF_Qty'])\
                    .withColumn('UserDefNetAmt_2','0'+ SLTXN['Calc_Gross_Amt'] +  SLTXN['Calc_commission'] +  SLTXN['calc_sp_commission'] +  SLTXN['calc_rdf'] +  SLTXN['calc_scheme_u'] +  SLTXN['calc_scheme_rs'] +  SLTXN['Total_GST_Amount_Paid'] +  SLTXN['Calc_Tax_2'] +  SLTXN['Calc_Tax_3'] +  SLTXN['calc_sur_on_tax3'] +  SLTXN['calc_mfees'] +  SLTXN['calc_excise_u'] +  SLTXN['Calc_adjustment_u'] +  SLTXN['Calc_adjust_rs'] +  SLTXN['Calc_freight'] +  SLTXN['calc_adjust'] +  SLTXN['Calc_Spdisc'] +  SLTXN['Calc_DN'] +  SLTXN['Calc_CN'] +  SLTXN['Calc_Display'] +  SLTXN['Calc_Handling'] +  SLTXN['calc_Postage'] +  SLTXN['calc_Round'] +  SLTXN['calc_Labour'])  
            
            SLTXN = SLTXN.withColumn('MonthOrder', when(month(SLTXN['Vouch_Date']) < 4, month(SLTXN['Vouch_Date']) + 12).otherwise(month(SLTXN['Vouch_Date'])))\
                            .withColumn('DayOrder', dayofmonth(SLTXN['Vouch_Date']))\
                            .withColumn('Year', year(SLTXN['Vouch_Date']))
                           
            SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 1).filter(SLTXN.Sa_Subs_Lot == 0).filter(SLTXN.Type == 'SL').filter(SLTXN.Deleted == 0)             
    
                
            SLTXN=SLTXN.groupby('Branch_Code','Cust_Code','remarks1','remarks3','remarks2','MarkDownOnGrossValue','NetPUAmount','NetSPAmount','Calc_Tax_3','vouch_code','YearMonth','Rate_Per_Pack','Type','Tax_Reg_Name','Lot_Code',\
                                'Bill_Cust_Code','Vouch_Num', 'vouch_date',  'Series_Code', 'Number_', 'Net_Amt','Pay_Mode','New_Vouch_Num','MonthOrder',\
                                    'Deleted','Item_Det_Code')\
                           .agg(sum('TotQty').alias('TotQty'),sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('FreeQty'),sum('SampleQty').alias('SampleQty'),sum('Free_Value').alias('Free_Value'),sum('Sample_Value').alias('Sample_Value'),sum('Repl_Value').alias('Repl_Value'),sum('UDR_Amount_1').alias('UDR_Amount_1'),sum('UDR_Amount').alias('UDR_Amount'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                avg('CF_Qty').alias('CF_Qty'),avg('Rate').alias('RatePerUnit'),sum('Qty_Weight').alias("Qty_Weight"),sum('Int_Total_Packs').alias('Int_Total_Packs'),\
                                sum('Calc_Gross_Amt').alias('Gross_Value'),sum('NET_SALE_VALUE').alias('Net_Sale_Value'),sum('Repl_Qty').alias('ReplQty'),\
                                sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Total_GST_Amount_Paid').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'),avg('sale_rate_lot').alias('UDR_Rate'),\
                                sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'),sum('calc_round').alias('Round_Amt'),\
                                sum('NetRatePerUnit').alias('NetRatePerUnit'),sum('DefinedMarkdown').alias('DefinedMarkdown'),sum('MarkDownOnNetValue').alias('MarkDownOnNetValue'))
                                
                
    
            SLTXN.write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransferOUTDetailed")
            print("StockTransferOUTDetailed Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransferOUTDetailed','DB':DB,'EN':Etn,
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
                    cdm = cdm + '0'+ str(month)
                else:
                    cdm = cdm + str(month)
                print("#"*200)
                print(cdm)
                print("#"*200)
                SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLHead1/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/StockTransferOUTDetailed/YearMonth="+cdm)
                
                SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left')
                SLTXN = SLTXN.join(LM1,on =['Lot_Code'],how='left')
                SLTXN = SLTXN.join(TR,on = ['Tax_Reg_Code'],how='left')
                #SLTXN = SLTXN.join(SL,on = ['vouch_code'],how='inner')    
        
                SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn('SaleQty',SLTXN['Tot_Qty']/1).withColumn('NetPUAmount',LM1['pur_rate_lot']*SLTXN['Tot_Qty']).withColumn('NetSPAmount',LM1['mrp_lot']*SLTXN['Tot_Qty']).withColumn('MarkDownOnGrossValue',LM1['mrp_lot']-SLTXN['Rate'])\
                    .withColumn('NetRatePerUnit',(SLTXN['NET_SALE_VALUE']/SLTXN['Tot_Qty'])).withColumn('DefinedMarkdown',((SLTXN['used_cf_rate']-SLTXN['Rate'])/SLTXN['used_cf_rate'])*100).withColumn('MarkDownOnNetValue',((SLTXN['used_cf_rate']-(SLTXN['NET_SALE_VALUE']/SLTXN['Tot_Qty']))/SLTXN['used_cf_rate'])*100)\
                    .withColumn('Free_Value',  LM1['basic_rate_lot'] * SLTXN['Free_Qty']).withColumn('SampleQty',SLTXN['Sample_Qty']/1).withColumn('Rate_Per_Pack',SLTXN['Rate'] * SLTXN['CF_Qty'])\
                    .withColumn('UDR_Amount',  LM1['sale_rate_lot'] * SLTXN['Tot_Qty']).withColumn('Repl_Value',  LM1['basic_rate_lot'] * SLTXN['Repl_Qty']).withColumn('Sample_Value',  LM1['basic_rate_lot'] * SLTXN['Sample_Qty'])\
                    .withColumn('UDR_Amount_1',  LM1['sale_rate_lot'] * SLTXN['Tot_Qty']).withColumn('Rate_Per_Pack',  SLTXN['Rate'] * SLTXN['CF_Qty']).withColumn('Int_Total_Packs',  SLTXN['Tot_Qty'] / SLTXN['CF_Qty'])\
                    .withColumn('UserDefNetAmt_2','0'+ SLTXN['Calc_Gross_Amt'] +  SLTXN['Calc_commission'] +  SLTXN['calc_sp_commission'] +  SLTXN['calc_rdf'] +  SLTXN['calc_scheme_u'] +  SLTXN['calc_scheme_rs'] +  SLTXN['Total_GST_Amount_Paid'] +  SLTXN['Calc_Tax_2'] +  SLTXN['Calc_Tax_3'] +  SLTXN['calc_sur_on_tax3'] +  SLTXN['calc_mfees'] +  SLTXN['calc_excise_u'] +  SLTXN['Calc_adjustment_u'] +  SLTXN['Calc_adjust_rs'] +  SLTXN['Calc_freight'] +  SLTXN['calc_adjust'] +  SLTXN['Calc_Spdisc'] +  SLTXN['Calc_DN'] +  SLTXN['Calc_CN'] +  SLTXN['Calc_Display'] +  SLTXN['Calc_Handling'] +  SLTXN['calc_Postage'] +  SLTXN['calc_Round'] +  SLTXN['calc_Labour'])  
            
                SLTXN = SLTXN.withColumn('MonthOrder', when(monthf(SLTXN.Vouch_Date) < 4, monthf(SLTXN.Vouch_Date) + 12).otherwise(monthf(SLTXN.Vouch_Date)))\
                                .withColumn('DayOrder', dayofmonth(SLTXN['Vouch_Date']))\
                                .withColumn('Year', year(SLTXN['Vouch_Date']))
                               
                SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 1).filter(SLTXN.Sa_Subs_Lot == 0).filter(SLTXN.Type == 'SL').filter(SLTXN.Deleted == 0)             
                SLTXN = SLTXN.withColumn('YearMonth',lit(cdm))
                    
                SLTXN=SLTXN.groupby('Branch_Code','Cust_Code','remarks1','remarks3','remarks2','MarkDownOnGrossValue','NetPUAmount','NetSPAmount','Calc_Tax_3','vouch_code','YearMonth','Rate_Per_Pack','Type','Tax_Reg_Name','Lot_Code',\
                                'Bill_Cust_Code','Vouch_Num', 'vouch_date',  'Series_Code', 'Number_', 'Net_Amt','Pay_Mode','New_Vouch_Num','MonthOrder',\
                                    'Deleted','Item_Det_Code')\
                               .agg(sum('TotQty').alias('TotQty'),sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('FreeQty'),sum('SampleQty').alias('SampleQty'),sum('Free_Value').alias('Free_Value'),sum('Sample_Value').alias('Sample_Value'),sum('Repl_Value').alias('Repl_Value'),sum('UDR_Amount_1').alias('UDR_Amount_1'),sum('UDR_Amount').alias('UDR_Amount'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                    avg('CF_Qty').alias('CF_Qty'),avg('Rate').alias('RatePerUnit'),sum('Qty_Weight').alias("Qty_Weight"),sum('Int_Total_Packs').alias('Int_Total_Packs'),\
                                    sum('Calc_Gross_Amt').alias('Gross_Value'),sum('NET_SALE_VALUE').alias('Net_Sale_Value'),sum('Repl_Qty').alias('ReplQty'),\
                                    sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                    sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Total_GST_Amount_Paid').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                    sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                    sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                    sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'),avg('sale_rate_lot').alias('UDR_Rate'),\
                                    sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'),sum('calc_round').alias('Round_Amt'),\
                                    sum('NetRatePerUnit').alias('NetRatePerUnit'),sum('DefinedMarkdown').alias('DefinedMarkdown'),sum('MarkDownOnNetValue').alias('MarkDownOnNetValue'))
                                    
                    
                
                SLTXN.write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransferOUTDetailed")
                #delete_data(' DELETE FROM KOCKPIT.dbo.StockTransferOUTDetailed WHERE YearMonth = ' + cdm)
                print("StockTransferOUTDetailed Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/StockTransferOUTDetailed")
        SLTXN = SLTXN.drop("YearMonth")
        write_data_sql(SLTXN,"StockTransferOUTDetailed",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransferOUTDetailed','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransferOUTDetailed','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  


 
