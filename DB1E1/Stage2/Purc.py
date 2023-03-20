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
conf = SparkConf().setMaster(smaster).setAppName("Purchase")\
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


TR= sqlctx.read.parquet(hdfspath+"/Market/Stage2/Tax_Regions")
#PH= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PO_Head")
PT= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseOrder")
SD= sqlctx.read.parquet(hdfspath+"/Market/Stage2/sup_det")
#PPL= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PU_PO_Links")
#PR= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUHead1")

CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Comm_Calc_Info")


CCI=CCI.select('Code','Tax_1','Tax_3','Exchange_Rate')
TR = TR.select('Tax_Reg_Name','Tax_Reg_Code')
#PH = PH.select('vouch_code','Order_Date','Order_No').withColumnRenamed("vouch_code","vouch_code1")
PT = PT.select('Code','Order_Date','Order_No').withColumnRenamed("Code","Order_Item_Code")#.withColumnRenamed("vouch_code","vouch_code1")
#PT= PT.join(PH,on=['vouch_code1'],how='left')
SD = SD.select('GST_Date','GST_No','Act_Code','TIN_No')
#PPL= PPL.select('Code','PU_Code','PO_Code')
#PR= PR.select('Vouch1','remarks1','gr_number','gr_date','remarks2','remarks3','Transport_Mode').withColumnRenamed("Vouch1","vouch_code")


print(datetime.now())
Start_Year = 2018

##LastPurPointReport
print(datetime.now())
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/LastPurPointReport") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUHead1")
        
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
            TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
            TXN = TXN.join(TR,on = ['Tax_Reg_Code'],how='left' )
            TXN = TXN.join(SD,TXN.Cust_Code==SD.Act_Code,how='left' )
            #TXN = TXN.join(PR,on = ['vouch_code'],how='inner' )
            #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
            #TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
                        
            TXN = TXN.filter(TXN.Stock_Trans == 0)\
                   .filter(TXN.Deleted_ == 0)#.filter(TXN.Vouch_Num.like('PU%'))             
            
            
            TXN=TXN.withColumn('Bill_Diff',TXN['Net_Amt'] - TXN['Bill_Amount'] ).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] ).withColumn('Dec_Total_Packs',TXN['Tot_Qty']/TXN['CF_Qty'])\
                    .withColumn('UdNetAmt','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round']) 
            #.withColumn('SubOrder',IMH['Item_Hd_Name'] +PM['Pack_Name'] )
            
            TXN=TXN.groupby('Godown_Code','Cust_Code','Rate','Repl_Qty','Free_Qty','vouch_code','Order_Date','Order_No','YearMonth','Exchange_Rate','Lot_Code','Vouch_Num','Tax_1','Tax_3',\
                            'Qty_Weight','Vouch_Date','Bill_Diff','Bill_No','Bill_Date', 'Branch_Code','CF_Qty','Calc_Scheme_Rs','Calc_Freight','Calc_Adjustment_u','Calc_Scheme_U','Sample_Qty',\
                            'GRN_PreFix', 'GRN_Number','Calc_tax_1','Calc_tax_2','Calc_Commission','Calc_Sp_Commission','Calc_Rdf','Pur_Or_PR','Tax_Reg_Name','Tax_Reg_Code','GST_Date','GST_No','TIN_No',\
                            'Calc_Adjust_RS','Calc_Adjust','Calc_Spdisc','Calc_DN','Calc_cn','Calc_Display','Calc_Handling','Calc_Postage','Calc_MFees','Calc_Labour',\
                            'Rate_Per_Pack','item_det_code','Calc_Excise_U','Goods_In_Transit','remarks1','gr_number','gr_date','remarks2','remarks3','Transport_Mode')\
                    .agg(sum('Dec_Total_Packs').alias('Dec_Total_Packs'),sum('UdNetAmt').alias('UdNetAmt'),sum('Calc_Gross_Amt').alias('Calc_Gross_Amt'),\
                                sum('Tot_Qty').alias('Tot_Qty'),sum('Calc_Tax_3').alias('Calc_Tax_3'),sum('Calc_Net_Amt').alias('Calc_Net_Amt'))
            
            # TXN.filter(TXN.GRN_Number == 1137).filter(TXN.item_det_code== 32200).filter(TXN.Vouch_Date== '2021-12-28').show()
            # exit()
                
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/LastPurPointReport")
            print("LastPurPointReport Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'LastPurPointReport','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(TXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                print('#'*500)
                print(cdm)
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUHead1/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/LastPurPointReport/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
                TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
                TXN = TXN.join(TR,on = ['Tax_Reg_Code'],how='left' )
                TXN = TXN.join(SD,TXN.Cust_Code==SD.Act_Code,how='left' )
                #TXN = TXN.join(PR,on = ['vouch_code'],how='inner' )
                #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
                #TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
                            
                TXN = TXN.filter(TXN.Stock_Trans == 0)\
                       .filter(TXN.Deleted_ == 0)#.filter(TXN.Vouch_Num.like('PU%'))             
                            
                            
                TXN=TXN.withColumn('Bill_Diff',TXN['Net_Amt'] - TXN['Bill_Amount'] ).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] ).withColumn('Dec_Total_Packs',TXN['Tot_Qty']/TXN['CF_Qty'])\
                        .withColumn('UdNetAmt','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round']) 
                TXN = TXN.withColumn('YearMonth',lit(cdm))  
                
                TXN=TXN.groupby('Godown_Code','Cust_Code','Rate','Repl_Qty','Free_Qty','vouch_code','Order_Date','Order_No','YearMonth','Exchange_Rate','Lot_Code','Vouch_Num','Tax_1','Tax_3',\
                                'Qty_Weight','Vouch_Date','Bill_Diff','Bill_No','Bill_Date', 'Branch_Code','CF_Qty','Calc_Scheme_Rs','Calc_Freight','Calc_Adjustment_u','Calc_Scheme_U','Sample_Qty',\
                                'GRN_PreFix', 'GRN_Number','Calc_tax_1','Calc_tax_2','Calc_Commission','Calc_Sp_Commission','Calc_Rdf','Pur_Or_PR','Tax_Reg_Name','Tax_Reg_Code','GST_Date','GST_No','TIN_No',\
                                'Calc_Adjust_RS','Calc_Adjust','Calc_Spdisc','Calc_DN','Calc_cn','Calc_Display','Calc_Handling','Calc_Postage','Calc_MFees','Calc_Labour',\
                                'Rate_Per_Pack','item_det_code','Calc_Excise_U','Goods_In_Transit','remarks1','gr_number','gr_date','remarks2','remarks3','Transport_Mode')\
                        .agg(sum('Dec_Total_Packs').alias('Dec_Total_Packs'),sum('UdNetAmt').alias('UdNetAmt'),sum('Calc_Gross_Amt').alias('Calc_Gross_Amt'),\
                                sum('Tot_Qty').alias('Tot_Qty'),sum('Calc_Tax_3').alias('Calc_Tax_3'),sum('Calc_Net_Amt').alias('Calc_Net_Amt'))
                
                #TXN.filter(TXN.GRN_Number == 1137).filter(TXN.item_det_code== 32200).filter(TXN.Vouch_Date== '2021-12-28').show()
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/LastPurPointReport")
                #delete_data(' DELETE FROM KOCKPIT.dbo.LastPurPointReport WHERE YearMonth = ' + cdm)
                print("LastPurPointReport Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/LastPurPointReport")
        #TXN.show()
        TXN = TXN.drop("YearMonth")
        write_data_sql(TXN,"LastPurPointReport",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'LastPurPointReport','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(TXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'LastPurPointReport','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append") 

'''
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/ItemWisePurReport") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase")
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
    
                
            TXN=TXN.filter(TXN.Deleted_== 0).filter(TXN.Stock_Trans == 0)
                   
            TXN=TXN.withColumn('TotQty',TXN['Tot_Qty']/1).withColumn('TotQty_1',TXN['Tot_Qty']/ 1)\
                .withColumn('PurQty',TXN['Tot_Qty']/ 1).withColumn('FreeQty',TXN['Free_Qty']/ 1)\
                .withColumn('ReplQty',TXN['Repl_Qty']/1).withColumn('SampleQty',TXN['Sample_Qty']/1)\
                .withColumn('Gross_Value',TXN['Calc_Gross_Amt']).withColumn('Net_Sale_Value',TXN['Calc_Net_Amt'])\
                .withColumn('CD',TXN['Calc_Commission']).withColumn('TD',TXN['Calc_Sp_Commission'])\
                .withColumn('SpCD',TXN['Calc_Rdf']).withColumn('Scheme_Unit',TXN['Calc_Scheme_U'])\
                .withColumn('Scheme_Rs',TXN['Calc_Scheme_Rs']).withColumn('Tax1',TXN['Calc_tax_1'])\
                .withColumn('Tax2',TXN['Calc_tax_2']).withColumn('Tax3',TXN['Calc_Tax_3'] + TXN['Calc_Sur_On_Tax3'])\
                .withColumn('Tax3WithoutSur',TXN['Calc_Tax_3']).withColumn('Surcharge',TXN['Calc_Sur_On_Tax3'])\
                .withColumn('ExciseUnit',TXN['Calc_Excise_U']).withColumn('AdjustPerUnit',TXN['Calc_Adjustment_u'])\
                .withColumn('AdjustItem',TXN['Calc_Adjust_RS']).withColumn('Freight',TXN['Calc_Freight'])\
                .withColumn('AdjustmentRupees',TXN['Calc_Adjust']).withColumn('SPDiscount',TXN['Calc_Spdisc'])\
                .withColumn('DebitNote',TXN['Calc_DN']).withColumn('CreditNote',TXN['Calc_cn']).withColumn('Display',TXN['Calc_Display'])\
                .withColumn('Handling',TXN['Calc_Handling']).withColumn('Postage',TXN['Calc_Postage']).withColumn('ExcisePercentage',TXN['Calc_MFees'])\
                .withColumn('Labour',TXN['Calc_Labour'])\
                .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                .withColumn('UserDefNetAmt_2','0'+ TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                #.withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])
        
            TXN=TXN.groupby('vouch_code','YearMonth','Lot_Code','Branch_Code','Item_Det_Code', 'Exchange_Rate','Pur_Or_PR' )\
                    .agg(sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('ReplQty').alias('ReplQty'),sum('TotQty_1').alias('TotQty_1'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                            sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                            sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                            sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                            sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                            sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'))
                    #sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
               
                
                    
            
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/ItemWisePurReport")
            print("ItemWisePurReport Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ItemWisePurReport','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(TXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Purchase/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/ItemWisePurReport/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' ) 
    
                    
                TXN=TXN.filter(TXN.Deleted_== 0).filter(TXN.Stock_Trans == 0)
                TXN = TXN.withColumn('YearMonth',lit(cdm))       
                       
                TXN=TXN.withColumn('TotQty',TXN['Tot_Qty']/1).withColumn('TotQty_1',TXN['Tot_Qty']/ 1)\
                    .withColumn('PurQty',TXN['Tot_Qty']/ 1).withColumn('FreeQty',TXN['Free_Qty']/ 1)\
                    .withColumn('ReplQty',TXN['Repl_Qty']/1).withColumn('SampleQty',TXN['Sample_Qty']/1)\
                    .withColumn('Gross_Value',TXN['Calc_Gross_Amt']).withColumn('Net_Sale_Value',TXN['Calc_Net_Amt'])\
                    .withColumn('CD',TXN['Calc_Commission']).withColumn('TD',TXN['Calc_Sp_Commission'])\
                    .withColumn('SpCD',TXN['Calc_Rdf']).withColumn('Scheme_Unit',TXN['Calc_Scheme_U'])\
                    .withColumn('Scheme_Rs',TXN['Calc_Scheme_Rs']).withColumn('Tax1',TXN['Calc_tax_1'])\
                    .withColumn('Tax2',TXN['Calc_tax_2']).withColumn('Tax3',TXN['Calc_Tax_3'] + TXN['Calc_Sur_On_Tax3'])\
                    .withColumn('Tax3WithoutSur',TXN['Calc_Tax_3']).withColumn('Surcharge',TXN['Calc_Sur_On_Tax3'])\
                    .withColumn('ExciseUnit',TXN['Calc_Excise_U']).withColumn('AdjustPerUnit',TXN['Calc_Adjustment_u'])\
                    .withColumn('AdjustItem',TXN['Calc_Adjust_RS']).withColumn('Freight',TXN['Calc_Freight'])\
                    .withColumn('AdjustmentRupees',TXN['Calc_Adjust']).withColumn('SPDiscount',TXN['Calc_Spdisc'])\
                    .withColumn('DebitNote',TXN['Calc_DN']).withColumn('CreditNote',TXN['Calc_cn']).withColumn('Display',TXN['Calc_Display'])\
                    .withColumn('Handling',TXN['Calc_Handling']).withColumn('Postage',TXN['Calc_Postage']).withColumn('ExcisePercentage',TXN['Calc_MFees'])\
                    .withColumn('Labour',TXN['Calc_Labour'])\
                    .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                    .withColumn('UserDefNetAmt_2','0'+ TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                
        
                TXN=TXN.groupby('vouch_code','YearMonth','Branch_Code','Item_Det_Code', 'Exchange_Rate', 'Pur_Or_PR' )\
                        .agg(sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('ReplQty').alias('ReplQty'),sum('TotQty_1').alias('TotQty_1'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                                sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                                sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                                sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                                sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                                sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'))
            
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/ItemWisePurReport")
                ##delete_data(' DELETE FROM KOCKPIT.dbo.ItemWisePurReport WHERE YearMonth = ' + cdm)
                print("ItemWisePurReport Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/ItemWisePurReport")
        TXN = TXN.drop("YearMonth")
        #TXN.show(1)
        write_data_sql(TXN,"ItemWisePurReport",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ItemWisePurReport','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(TXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ItemWisePurReport','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   

'''