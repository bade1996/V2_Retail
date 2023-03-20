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
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\MARKET99BI;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

def write_data_sql(df,name,mode):
    database = "KOCKPIT"
    user = "sa"
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\MARKET99BI;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df.write.jdbc(url = SQLurl , table = name, properties = SQLprop, mode = mode)
    
def delete_data(dqry):
    server = '103.248.60.5' 
    database = 'KOCKPIT' 
    username = 'sa' 
    password = 'M99@321' 
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
conf = SparkConf().setMaster(smaster).setAppName("Purchase").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)


    
ACT= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Accounts")
AB = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Agents_Brokers")
Ct = sqlctx.read.parquet(hdfspath+"/Market/Stage1/City_")
LM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
GDM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Godown_Mst")
AG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/AccountGroups")
CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
CCI1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
IMOD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_O_Det")
BG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Groups")
PM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
CM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comp_Mst")
ICM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
IMN= sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
CID = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_It_Desc")
GM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
GM11 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
IMD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")

BM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
IMH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
#IMN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
TR= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Tax_Regions")
#PH= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PO_Head")
PT= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseOrder")
SD= sqlctx.read.parquet(hdfspath+"/Market/Stage1/sup_det")
PPL= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PU_PO_Links")
PR= sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUHead1")

ACT=ACT.select('Act_Code','Grp_Code','City_Code','Act_Name','Print_Act_Name') 
CCI=CCI.select('Code','Tax_3','Exchange_Rate')#.withColumnRenamed("Code","Comm_Calc_Code")
AG = AG.select('Grp_Code')
IMOD=IMOD.select('Code').withColumnRenamed("Code","Item_O_Det_Code")
BG=BG.select('Group_Code')
BM=BM.select('Group_Code1','Branch_Name', 'Branch_Code')
GDM=GDM.select('godown_Code','Godown_Name')
AB=AB.select('Agent_Name','Code') 
Ct=Ct.select('City_Code','City_Name')
IMH=IMH.select('Item_Hd_Code','Item_Hd_Name','comp_code','Color_Code','Group_Code','Item_Name_Code','Comm_It_Desc_Code')
IMD=IMD.select('Cf_1_Desc', 'Cf_2_Desc', 'Cf_3_Desc', 'Packing' , 'Cf_1' , 'Cf_2', 'CF_3',\
   'User_Code','Item_Det_Code','Pack_Code','Item_O_Det_Code','Item_Hd_Code' )
CM=CM.select('Comp_Code','Comp_Name')
ICM=ICM.select('Color_Code')
GM=GM.select('Group_Code','Group_Name')
GM11=GM11.select('Group_Code','Group_Name').withColumnRenamed("Group_Code","Group_Code11").withColumnRenamed("Group_Name","Group_Name11")

PM=PM.select('Pack_Code', 'Pack_Name', 'Order_' ,'Pack_Short_Name','Link_Code')
IMN=IMN.select('Item_Name_Code')
LM=LM.select('CF_Lot' ,'Basic_rate','Lot_Code','exp_dmg','sale_rate','Org_Lot_Code','Bill_No','Pur_Date','Lot_Number','Mfg_Date',\
             'Map_Code','Pur_Rate','Expiry','Mrp','sp_rate2').withColumnRenamed("Bill_No","BillNo")
CID=CID.select( 'Item_Desc','Addl_Item_Name','Code').withColumnRenamed("Code","Comm_It_Desc_Code")

TR = TR.select('Tax_Reg_Name','Tax_Reg_Code')
#PH = PH.select('vouch_code','Order_Date','Order_No').withColumnRenamed("vouch_code","vouch_code1")
PT = PT.select('Code','Order_Date','Order_No').withColumnRenamed("Code","Order_Item_Code")#.withColumnRenamed("vouch_code","vouch_code1")
#PT= PT.join(PH,on=['vouch_code1'],how='left')
SD = SD.select('GST_Date','GST_No','Act_Code','TIN_No')
PPL= PPL.select('Code','PU_Code','PO_Code')
PR= PR.select('Vouch1','remarks1','gr_number','gr_date','remarks2','remarks3','Transport_Mode').withColumnRenamed("Vouch1","Vouchcode")


print(datetime.now())
Start_Year = 2018

try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/ItemWisePurchaseReport_I") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' ) 
            TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left')
            TXN = TXN.join(LM,on =['Lot_Code'],how='left')
            TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
            TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
            TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
            TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
            TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
            TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
            TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
            TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
            TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
            TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
            TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
            TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
    
                
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
                .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                .withColumn('UserDefNetAmt_2','0'+ TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
            
        
            TXN=TXN.groupby('Vouchcode','YearMonth','Agent_Name',AB['Code'] ,'cf_1_desc','cf_2_desc','cf_3_desc','Packing' ,'Cf_1' , 'Cf_2', 'CF_3', 'CF_Lot',\
                            'Branch_Name', 'Branch_Code','Pack_Code', 'Pack_Name', 'Pack_Short_Name','Order_', 'Item_Hd_Code', 'Item_Hd_Name', 'Comp_Code', 'Comp_Name',\
                            'Item_Det_Code',  'User_Code', 'Link_Code', 'Item_Desc', 'Addl_Item_Name', 'Pur_Or_PR','exp_dmg' )\
                    .agg(sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('ReplQty').alias('ReplQty'),sum('TotQty_1').alias('TotQty_1'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                            sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                            sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                            sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                            sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                            sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
               
                
                    
            
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/ItemWisePurchaseReport_I")
            print("ItemWisePurchaseReport_I Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ItemWisePurchaseReport_I','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/ItemWisePurchaseReport_I/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' ) 
                TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
                TXN = TXN.join(LM,on =['Lot_Code'],how='left')
                TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
                TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
                TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
                TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
                TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
                TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
                TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
                TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
                TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
                TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
                TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
                TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
                TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
    
                    
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
                    .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                    .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                    .withColumn('UserDefNetAmt_2','0'+ TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                
        
                TXN=TXN.groupby('Vouchcode','YearMonth','Agent_Name',AB['Code'] ,'cf_1_desc','cf_2_desc','cf_3_desc','Packing' ,'Cf_1' , 'Cf_2', 'CF_3', 'CF_Lot',\
                                'Branch_Name', 'Branch_Code','Pack_Code', 'Pack_Name', 'Pack_Short_Name','Order_', 'Item_Hd_Code', 'Item_Hd_Name', 'Comp_Code', 'Comp_Name',\
                                'Item_Det_Code',  'User_Code', 'Link_Code', 'Item_Desc', 'Addl_Item_Name', 'Pur_Or_PR','exp_dmg' )\
                        .agg(sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('ReplQty').alias('ReplQty'),sum('TotQty_1').alias('TotQty_1'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                                sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                                sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                                sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                                sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                                sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
            
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/ItemWisePurchaseReport_I")
                delete_data(' DELETE FROM KOCKPIT.dbo.ItemWisePurchaseReport_I WHERE YearMonth = ' + cdm)
                print("ItemWisePurchaseReport_I Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/ItemWisePurchaseReport_I")
        TXN = TXN.drop("YearMonth")
        #TXN.show(1)
        write_data_sql(TXN,"ItemWisePurchaseReport_I",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ItemWisePurchaseReport_I','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'ItemWisePurchaseReport_I','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
        
##LastPurchasePointReport_I
try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/LastPurchasePointReport_I") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
            TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
            TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
            TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
            TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
            TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
            TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
            TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
            TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
            TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
            TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
            TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
            TXN = TXN.join(TR,on = ['Tax_Reg_Code'],how='left' )
            TXN = TXN.join(SD,on = ['Act_Code'],how='left' )
            TXN = TXN.join(PR,on = ['Vouchcode'],how='left' )
            #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
            #TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
                        
            TXN = TXN.filter(TXN.Stock_Trans == 0)\
                   .filter(TXN.Deleted_ == 0)#.filter(TXN.Vouch_Num.like('PU%'))             
                        
                        
            TXN=TXN.withColumn('Bill_Diff',TXN['Net_Amt'] - TXN['Bill_Amount'] ).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] ).withColumn('SubOrder',IMH['Item_Hd_Name'] +PM['Pack_Name'] ).withColumn('Dec_Total_Packs',TXN['Tot_Qty']/TXN['CF_Qty']).withColumn('UDR_Amount',LM['Pur_Rate'] * TXN['Tot_Qty'])\
                    .withColumn('UdNetAmt','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round']) 
            
            
            TXN=TXN.groupby('Rate','sp_rate2','Repl_Qty','Free_Qty','Comp_Name','Godown_Name','Agent_Name','Vouchcode','Order_Date','Order_No','YearMonth','Exchange_Rate','Item_Hd_Code','Item_Hd_Name','Lot_Number','Lot_Code','Vouch_Num','Tot_Qty',\
                            'Qty_Weight','Vouch_Date','Bill_Diff','Bill_No','Bill_Date','Branch_Name', 'Branch_Code','CF_Qty','Calc_Gross_Amt','Calc_Scheme_Rs','Calc_Freight','Calc_Adjustment_u','Calc_Scheme_U','Sample_Qty','Cf_1_Desc',\
                            'Mfg_Date','GRN_PreFix', 'GRN_Number','Calc_tax_1','Calc_tax_2','Calc_Tax_3','Calc_Net_Amt','Calc_Commission','Calc_Sp_Commission','Calc_Rdf','Pur_Or_PR','Tax_Reg_Name','Tax_Reg_Code','GST_Date','GST_No','TIN_No',\
                            'Calc_Adjust_RS','Calc_Adjust','Calc_Spdisc','Calc_DN','Calc_cn','Calc_Display','Calc_Handling','Calc_Postage','Calc_MFees','Calc_Labour','sale_rate','Pur_Rate','Basic_rate','Packing',\
                            'Pack_Code', 'Pack_Name','Rate_Per_Pack','SubOrder','City_Name','Mrp','comp_code','User_Code','Item_Det_Code','Pur_Date','Calc_Excise_U','Goods_In_Transit','Cf_1' , 'Cf_2', 'CF_3','remarks1','gr_number','gr_date','remarks2','remarks3',PR['Transport_Mode'])\
                    .agg(sum('Dec_Total_Packs').alias('Dec_Total_Packs'),sum('UdNetAmt').alias('UdNetAmt'),sum('UDR_Amount').alias('UDR_Amount'))
    
                
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/LastPurchasePointReport_I")
            print("LastPurchasePointReport_I Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'LastPurchasePointReport_I','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/LastPurchasePointReport_I/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
                TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
                TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
                TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
                TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
                TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
                TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
                TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
                TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
                TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
                TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
                TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
                TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left') 
                TXN = TXN.join(TR,on = ['Tax_Reg_Code'],how='left' )
                TXN = TXN.join(SD,on = ['Act_Code'],how='left')
                TXN = TXN.join(PR,on = ['Vouchcode'],how='left' )
                #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
                #TXN =TXN.join(PT,on= ['Order_Item_Code'] ,how='left')
                            
                            
                TXN = TXN.filter(TXN.Stock_Trans == 0)\
                       .filter(TXN.Deleted_ == 0)#.filter(TXN.Vouch_Num.like('PU%'))             
                TXN =TXN.withColumn('YearMonth',lit(cdm))            
                            
                TXN=TXN.withColumn('Bill_Diff',TXN['Net_Amt'] - TXN['Bill_Amount'] ).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] ).withColumn('SubOrder',IMH['Item_Hd_Name'] +PM['Pack_Name'] ).withColumn('Dec_Total_Packs',TXN['Tot_Qty']/TXN['CF_Qty']).withColumn('UDR_Amount',LM['Pur_Rate'] * TXN['Tot_Qty'])\
                        .withColumn('UdNetAmt','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round']) 
                
                
                TXN=TXN.groupby('Rate','sp_rate2','Repl_Qty','Free_Qty','Comp_Name','Godown_Name','Agent_Name','Vouchcode','Order_Date','Order_No','YearMonth','Exchange_Rate','Item_Hd_Code','Item_Hd_Name','Lot_Number','Lot_Code','Vouch_Num',\
                                'Qty_Weight','Mfg_Date','Tot_Qty','Vouch_Date','Bill_Diff','Bill_No','Bill_Date','Branch_Name', 'Branch_Code','CF_Qty','Calc_Gross_Amt','Calc_Scheme_Rs','Calc_Freight','Calc_Adjustment_u','Sample_Qty','GST_Date','GST_No','TIN_No',\
                                'GRN_PreFix', 'GRN_Number','Calc_tax_1','Calc_tax_2','Calc_Tax_3','Calc_Net_Amt','Calc_Commission','Calc_Sp_Commission','Calc_Rdf','Pur_Or_PR','Tax_Reg_Name','Tax_Reg_Code','Calc_Scheme_U',\
                                'Calc_Adjust_RS','Calc_Adjust','Calc_Spdisc','Calc_DN','Calc_cn','Calc_Display','Calc_Handling','Calc_Postage','Calc_MFees','Calc_Labour','sale_rate','Pur_Rate','Basic_rate','Packing','Cf_1_Desc',\
                                'Pack_Code', 'Pack_Name','Rate_Per_Pack','SubOrder','City_Name','Mrp','comp_code','User_Code','Item_Det_Code','Pur_Date','Calc_Excise_U','Goods_In_Transit', 'Cf_1' , 'Cf_2', 'CF_3','remarks1','gr_number','gr_date','remarks2','remarks3',PR['Transport_Mode'])\
                        .agg(sum('Dec_Total_Packs').alias('Dec_Total_Packs'),sum('UdNetAmt').alias('UdNetAmt'),sum('UDR_Amount').alias('UDR_Amount'))
    
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/LastPurchasePointReport_I")
                delete_data(' DELETE FROM KOCKPIT.dbo.LastPurchasePointReport_I WHERE YearMonth = ' + cdm)
                print("LastPurchasePointReport_I Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/LastPurchasePointReport_I")
        TXN.show()
        TXN = TXN.drop("YearMonth")
        write_data_sql(TXN,"LastPurchasePointReport_I",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'LastPurchasePointReport_I','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'LastPurchasePointReport_I','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append") 

##PurchaseReturnSummary_I
try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PurchaseReturnSummary_I") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
            TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
            TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
            TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
            TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
            TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
            TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
            TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
            TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
            TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
            TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
            TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
            TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
            TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
            TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
            #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
            #TXN = TXN.join(IMOD,TXN.Item_O_Det_Code==IMOD.Code,how='left' )
            TXN = TXN.join(GM11,TXN.Group_Code== GM11.Group_Code11,how='left') 
            TXN = TXN.join(TR,on = ['Tax_Reg_Code'],how='left' )
    
            TXN = TXN.filter(TXN.Stock_Trans == 0)\
                            .filter(TXN.Deleted_ == 0).filter((TXN.Vouch_Num.like('PR%')) | (TXN.Vouch_Num.like('RD%')))
            
            TXN = TXN.withColumn('MonthOrder', when(monthf(TXN.Vouch_Date) < 4, monthf(TXN.Vouch_Date) + 12).otherwise(monthf(TXN.Vouch_Date)))\
                       .withColumn('DayOrder', dayofmonth(TXN['Vouch_Date'])).withColumn('Year', year(TXN['Vouch_Date']))
    
    
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
                    .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                    .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                    .withColumn('UserDefNetAmt_2','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])
                    
            TXN=TXN.groupby('Vouchcode','YearMonth','DayOrder','Year','Act_Name','Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit','Vouch_Date','Tax_Reg_Name','exp_dmg','MonthOrder',\
                              'Bill_No', 'Bill_Date' , 'GRN_PreFix', 'GRN_Number' , 'Agent_Name', AB['Code'],'Branch_Name', 'Branch_Code','net_amt','bill_amount','Pur_Or_PR')\
                    .agg(sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('TotQty_1').alias('TotQty_1'),sum('ReplQty').alias('ReplQty'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                            sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                            sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                            sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                            sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                            sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
    
                
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurchaseReturnSummary_I")
            print("PurchaseReturnSummary_I Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseReturnSummary_I','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PurchaseReturnSummary_I/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
                TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
                TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
                TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
                TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
                TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
                TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
                TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
                TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
                TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
                TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
                TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
                TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
                TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
                TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
                TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
                #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
                #TXN = TXN.join(IMOD,TXN.Item_O_Det_Code==IMOD.Code,how='left' )
                TXN = TXN.join(GM11,TXN.Group_Code== GM11.Group_Code11,how='left') 
                TXN = TXN.join(TR,on = ['Tax_Reg_Code'],how='left' )
    
                TXN = TXN.filter(TXN.Stock_Trans == 0)\
                            .filter(TXN.Deleted_ == 0).filter((TXN.Vouch_Num.like('PR%')) | (TXN.Vouch_Num.like('RD%')))
                
                TXN = TXN.withColumn('MonthOrder', when(monthf(TXN.Vouch_Date) < 4, monthf(TXN.Vouch_Date) + 12).otherwise(monthf(TXN.Vouch_Date)))\
                           .withColumn('DayOrder', dayofmonth(TXN['Vouch_Date'])).withColumn('Year', year(TXN['Vouch_Date']))
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
                        .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                        .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                        .withColumn('UserDefNetAmt_2','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])
                        
                TXN=TXN.groupby('Vouchcode','YearMonth','DayOrder','Year','Act_Name','Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit','Vouch_Date','Tax_Reg_Name','exp_dmg','MonthOrder',\
                                  'Bill_No', 'Bill_Date' , 'GRN_PreFix', 'GRN_Number' , 'Agent_Name', AB['Code'],'Branch_Name', 'Branch_Code','net_amt','bill_amount','Pur_Or_PR')\
                        .agg(sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('TotQty_1').alias('TotQty_1'),sum('ReplQty').alias('ReplQty'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                                sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                                sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                                sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                                sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                                sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
    
    
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurchaseReturnSummary_I")
                delete_data(' DELETE FROM KOCKPIT.dbo.PurchaseReturnSummary_I WHERE YearMonth = ' + cdm)
                print("PurchaseReturnSummary_I Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PurchaseReturnSummary_I")
        TXN.show()
        TXN = TXN.drop("YearMonth")
        write_data_sql(TXN,"PurchaseReturnSummary_I",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseReturnSummary_I','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseReturnSummary_I','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")     

##PurchaseRegisterBillWiseDetailed_I
try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PurchaseRegisterBillWiseDetailed_I") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
            TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
            TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
            TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
            TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
            TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
            TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
            TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
            TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
            TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
            TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
            TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
            TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
            TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
            TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
    
            TXN = TXN.filter(TXN.Stock_Trans == 0)\
                   .filter(TXN.Deleted_ == 0).filter(TXN.Vouch_Num.like('PU%'))
            
            TXN = TXN.withColumn('MonthOrder', when(month(TXN.Vouch_Date) < 4, month(TXN.Vouch_Date) + 12).otherwise(month(TXN.Vouch_Date)))\
                       .withColumn('DayOrder', dayofmonth(TXN['Vouch_Date'])).withColumn('Year', year(TXN['Vouch_Date']))
    
            
    
            TXN=TXN.withColumn('Qty_Weight',TXN['Qty_Weight']/1).withColumn('TotQty',TXN['Tot_Qty']/1).withColumn('TotQty_1',TXN['Tot_Qty']/ 1)\
                    .withColumn('PurQty',TXN['Tot_Qty']/ 1).withColumn('FreeQty',TXN['Free_Qty']/ 1).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] )\
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
                    .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                    .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                    .withColumn('UserDefNetAmt_2','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                
            TXN=TXN.groupby('Vouchcode','YearMonth','Act_Name','Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit','Vouch_Date',TXN['Bill_No'],'Bill_Date',LM['BillNo'],\
                            'GRN_PreFix', 'GRN_Number' , 'Agent_Name', AB['Code'] , 'Lot_Code', 'Org_Lot_Code','Pur_Date', 'Lot_Number' , 'cf_1_desc', 'cf_2_desc','cf_3_desc',\
                            'Packing' ,'Cf_1' , 'Cf_2', 'CF_3', 'CF_Lot' , LM['Expiry'], 'Mfg_Date', 'Map_Code' , 'Exchange_Rate', 'CF_Qty', 'pur_rate' , 'sale_rate' ,'Rate',\
                            'Branch_Name','Branch_Code','Tax_3','Bill_Amount','Net_Amt','Pack_Code' ,'Pack_Name','Pack_Short_Name','Order_','Item_Hd_Code','Item_Hd_Name',\
                            'Comp_Code', 'Comp_Name'  ,'Item_Det_Code','User_Code','Year','Link_Code', 'Item_Desc', 'Addl_Item_Name' , 'Pur_Or_PR','exp_dmg','MonthOrder')\
                    .agg(sum('Rate_Per_Pack').alias('Rate_Per_Pack'),sum('Qty_Weight').alias('Qty_Weight'),sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('TotQty_1').alias('TotQty_1'),sum('ReplQty').alias('ReplQty'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                            sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                            sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                            sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                            sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                            sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
            
            
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurchaseRegisterBillWiseDetailed_I")
            print("PurchaseRegisterBillWiseDetailed_I Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseRegisterBillWiseDetailed_I','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PurchaseRegisterBillWiseDetailed_I/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
                TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
                TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
                TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
                TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
                TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
                TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
                TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
                TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
                TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
                TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
                TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
                TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
                TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
                TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
                #TXN = TXN.join(IMOD,TXN.Item_O_Det_Code==IMOD.Code,how='left' )
                TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
                TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
    
                TXN = TXN.filter(TXN.Stock_Trans == 0)\
                       .filter(TXN.Deleted_ == 0).filter(TXN.Vouch_Num.like('PU%'))
                
                TXN = TXN.withColumn('MonthOrder', when(monthf(TXN.Vouch_Date) < 4, monthf(TXN.Vouch_Date) + 12).otherwise(monthf(TXN.Vouch_Date)))\
                           .withColumn('DayOrder', dayofmonth(TXN['Vouch_Date'])).withColumn('Year', year(TXN['Vouch_Date']))
    
                TXN =TXN.withColumn('YearMonth',lit(cdm))
    
                TXN=TXN.withColumn('Qty_Weight',TXN['Qty_Weight']/1).withColumn('TotQty',TXN['Tot_Qty']/1).withColumn('TotQty_1',TXN['Tot_Qty']/ 1)\
                        .withColumn('PurQty',TXN['Tot_Qty']/ 1).withColumn('FreeQty',TXN['Free_Qty']/ 1).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] )\
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
                        .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                        .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                        .withColumn('UserDefNetAmt_2','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                    
                TXN=TXN.groupby('Vouchcode','YearMonth','Act_Name','Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit','Vouch_Date',TXN['Bill_No'],'Bill_Date',LM['BillNo'],\
                                'GRN_PreFix', 'GRN_Number' , 'Agent_Name', AB['Code'] , 'Lot_Code', 'Org_Lot_Code' ,'Pur_Date', 'Lot_Number' , 'cf_1_desc', 'cf_2_desc','cf_3_desc',\
                                'Packing' ,'Cf_1' , 'Cf_2', 'CF_3', 'CF_Lot' , LM['Expiry'], 'Mfg_Date', 'Map_Code' , 'Exchange_Rate', 'CF_Qty', 'pur_rate' , 'sale_rate' ,'Rate',\
                                'Branch_Name','Branch_Code','Tax_3','Bill_Amount','Net_Amt','Pack_Code' ,'Pack_Name','Pack_Short_Name','Order_','Item_Hd_Code','Item_Hd_Name',\
                                'Comp_Code', 'Comp_Name'  ,'Item_Det_Code','User_Code','Year','Link_Code', 'Item_Desc', 'Addl_Item_Name' , 'Pur_Or_PR','exp_dmg','MonthOrder')\
                        .agg(sum('Rate_Per_Pack').alias('Rate_Per_Pack'),sum('Qty_Weight').alias('Qty_Weight'),sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('TotQty_1').alias('TotQty_1'),sum('ReplQty').alias('ReplQty'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                                sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                                sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                                sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                                sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                                sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
                
    
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurchaseRegisterBillWiseDetailed_I")
                delete_data(' DELETE FROM KOCKPIT.dbo.PurchaseRegisterBillWiseDetailed_I WHERE YearMonth = ' + cdm)
                print("PurchaseRegisterBillWiseDetailed_I Done")
                
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PurchaseRegisterBillWiseDetailed_I")
        #TXN.show()
        TXN = TXN.drop("YearMonth")
        write_data_sql(TXN,"PurchaseRegisterBillWiseDetailed_I",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseRegisterBillWiseDetailed_I','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseRegisterBillWiseDetailed_I','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")     
    
    
    

##PurchaseReportWithCP_SP_I
try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/PurchaseReportWithCP_SP_I") !=0):
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        TXN.cache()
        if(TXN.count() > 0):
            TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
            #TXN.show(10, False)
            #exit()
            TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
            TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
            TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
            TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
            TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
            TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
            TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
            TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
            TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
            TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
            TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
            TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
            TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
            TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
            #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
            #TXN = TXN.join(IMOD,TXN.Item_O_Det_Code==IMOD.Code,how='left' )
            TXN = TXN.join(GM11,TXN.Group_Code== GM11.Group_Code11,how='left') 
    
            TXN = TXN.filter(TXN.Stock_Trans == 0)\
                   .filter(TXN.Deleted_ == 0).filter(TXN.Vouch_Num.like('PU%'))
            
            TXN = TXN.withColumn('MonthOrder', when(month(TXN.Vouch_Date) < 4, month(TXN.Vouch_Date) + 12).otherwise(month(TXN.Vouch_Date)))\
                       .withColumn('DayOrder', dayofmonth(TXN['Vouch_Date'])).withColumn('Year', year(TXN['Vouch_Date']))
    
            
    
            TXN=TXN.withColumn('Qty_Weight',TXN['Qty_Weight']/1).withColumn('TotQty',TXN['Tot_Qty']/1).withColumn('TotQty_1',TXN['Tot_Qty']/ 1)\
                    .withColumn('PurQty',TXN['Tot_Qty']/ 1).withColumn('FreeQty',TXN['Free_Qty']/ 1).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] )\
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
                    .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                    .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                    .withColumn('UserDefNetAmt_2','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage']) 
                    
                    
            TXN=TXN.groupby('Vouchcode','YearMonth','Act_Name','Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit','Vouch_Date',TXN['Bill_No'],'Bill_Date',\
                            'GRN_PreFix', 'GRN_Number' , 'Agent_Name', AB['Code'] , 'Lot_Code', 'Org_Lot_Code' ,'Pur_Date', 'Lot_Number' , 'cf_1_desc', 'cf_2_desc','cf_3_desc',\
                            'Packing' ,'Cf_1' , 'Cf_2', 'CF_3', 'CF_Lot' , 'Expiry', 'Mfg_Date', 'Map_Code' , 'Exchange_Rate', 'CF_Qty', 'pur_rate' , 'sale_rate' ,'Rate',\
                            'Branch_Name','Branch_Code','Tax_3','Bill_Amount','Net_Amt','Pack_Code' ,'Pack_Name','Pack_Short_Name','Order_','Item_Hd_Code','Item_Hd_Name',\
                            'Comp_Code', 'Comp_Name' , 'User_Code' ,'Item_Det_Code', GM11['Group_Name11'] , GM11['Group_Code11'] ,\
                            'Year','Link_Code', 'Item_Desc', 'Addl_Item_Name' , 'Pur_Or_PR','exp_dmg','MonthOrder')\
                    .agg(sum('Rate_Per_Pack').alias('Rate_Per_Pack'),sum('Qty_Weight').alias('Qty_Weight'),sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('TotQty_1').alias('TotQty_1'),sum('ReplQty').alias('ReplQty'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                            sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                            sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                            sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                            sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                            sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
    
            
            TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurchaseReportWithCP_SP_I")
            print("PurchaseReportWithCP_SP_I Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseReportWithCP_SP_I','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/PurchaseReportWithCP_SP_I/YearMonth="+cdm)
               
                TXN = TXN.join(CCI,TXN.Comm_Calc_Code==CCI.Code,how='left' )
                #TXN.show(10, False)
                #exit()
                TXN = TXN.join(LM,on =['Lot_Code'],how='left' )
                TXN = TXN.join(IMD,on = ['Item_Det_Code'],how='left' )
                TXN = TXN.join(GDM,on = ['Godown_Code'],how='left' )
                TXN = TXN.join(ACT,TXN.Cust_Code==ACT.Act_Code,how='left' )
                TXN = TXN.join(AB,TXN.Agent_Code==AB.Code,how='left' )
                TXN = TXN.join(BM,on = ['Branch_Code'], how = 'left')
                TXN = TXN.join(Ct,on = ['City_Code'], how = 'left')
                TXN = TXN.join(PM,on = ['Pack_Code'],how='left' )
                TXN = TXN.join(AG,on = ['Grp_Code'], how = 'left')
                TXN = TXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                TXN = TXN.join(CM,on = ['comp_code'], how = 'left')
                TXN = TXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
                TXN = TXN.join(BG,TXN.Group_Code1== BG.Group_Code,how='left') 
                TXN = TXN.join(GM,on = ['Group_Code'], how = 'left')
                TXN = TXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                TXN = TXN.join(IMOD,on = ['Item_O_Det_Code'],how='left' )
                TXN = TXN.join(CID,on = ['Comm_It_Desc_Code'],how='left')
                #TXN = TXN.join(CID,TXN.Comm_It_Desc_Code==CID.Code,how='left')
                #TXN = TXN.join(IMOD,TXN.Item_O_Det_Code==IMOD.Code,how='left' )
                TXN = TXN.join(GM11,TXN.Group_Code== GM11.Group_Code11,how='left') 
    
                TXN = TXN.filter(TXN.Stock_Trans == 0)\
                       .filter(TXN.Deleted_ == 0).filter(TXN.Vouch_Num.like('PU%'))
                
                TXN = TXN.withColumn('MonthOrder', when(monthf(TXN.Vouch_Date) < 4, monthf(TXN.Vouch_Date) + 12).otherwise(monthf(TXN.Vouch_Date)))\
                           .withColumn('DayOrder', dayofmonth(TXN['Vouch_Date'])).withColumn('Year', year(TXN['Vouch_Date']))
    
                TXN = TXN.withColumn('YearMonth',lit(cdm))
    
                TXN=TXN.withColumn('Qty_Weight',TXN['Qty_Weight']/1).withColumn('TotQty',TXN['Tot_Qty']/1).withColumn('TotQty_1',TXN['Tot_Qty']/ 1)\
                        .withColumn('PurQty',TXN['Tot_Qty']/ 1).withColumn('FreeQty',TXN['Free_Qty']/ 1).withColumn('Rate_Per_Pack',TXN['Rate'] * TXN['CF_Qty'] )\
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
                        .withColumn('Labour',TXN['Calc_Labour']).withColumn('UDR_Amount',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_2',LM['sale_rate'] * TXN['Tot_Qty']).withColumn('UDR_Amount_3',LM['Basic_rate'] * TXN['Tot_Qty'])\
                        .withColumn('UserDefNetAmt_1','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Tax_1'] + TXN['Calc_Tax_2'] + TXN['Calc_Tax_3'] + TXN['calc_sur_on_tax3'] + TXN['calc_mfees'] + TXN['calc_excise_u'] + TXN['Calc_adjustment_u'] + TXN['Calc_adjust_rs'] + TXN['Calc_freight'] + TXN['calc_adjust'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage'] + TXN['calc_round'])\
                        .withColumn('UserDefNetAmt_2','0' + TXN['Calc_Gross_Amt'] + TXN['Calc_commission'] + TXN['calc_sp_commission'] + TXN['calc_rdf'] + TXN['calc_scheme_u'] + TXN['calc_scheme_rs'] + TXN['Calc_Spdisc'] + TXN['Calc_DN'] + TXN['Calc_CN'] + TXN['Calc_Display'] + TXN['Calc_Handling'] + TXN['calc_Postage']) 
                        
                        
                TXN=TXN.groupby('Vouchcode','YearMonth','Act_Name','Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit','Vouch_Date',TXN['Bill_No'],'Bill_Date',\
                                'GRN_PreFix', 'GRN_Number' , 'Agent_Name', AB['Code'] , 'Lot_Code', 'Org_Lot_Code' ,'Pur_Date', 'Lot_Number' , 'cf_1_desc', 'cf_2_desc','cf_3_desc',\
                                'Packing' ,'Cf_1' , 'Cf_2', 'CF_3', 'CF_Lot' , 'Expiry', 'Mfg_Date', 'Map_Code' , 'Exchange_Rate', 'CF_Qty', 'pur_rate' , 'sale_rate' ,'Rate',\
                                'Branch_Name','Branch_Code','Tax_3','Bill_Amount','Net_Amt','Pack_Code' ,'Pack_Name','Pack_Short_Name','Order_','Item_Hd_Code','Item_Hd_Name',\
                                'Comp_Code', 'Comp_Name' , 'User_Code' ,'Item_Det_Code', GM11['Group_Name11'] , GM11['Group_Code11'] ,\
                                'Year','Link_Code', 'Item_Desc', 'Addl_Item_Name' , 'Pur_Or_PR','exp_dmg','MonthOrder')\
                        .agg(sum('Rate_Per_Pack').alias('Rate_Per_Pack'),sum('Qty_Weight').alias('Qty_Weight'),sum('TotQty').alias('TotQty'),sum('Gross_Value').alias('Gross_Value'),sum('TotQty_1').alias('TotQty_1'),sum('ReplQty').alias('ReplQty'),sum('FreeQty').alias('FreeQty'),sum('PurQty').alias('PurQty'),\
                                sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                                sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                                sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SampleQty').alias('SampleQty'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                                sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                                sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'))
    
    
                TXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/PurchaseReportWithCP_SP_I")
                delete_data(' DELETE FROM KOCKPIT.dbo.PurchaseReportWithCP_SP_I WHERE YearMonth = ' + cdm)
                print("PurchaseReportWithCP_SP_I Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PurchaseReportWithCP_SP_I")
        TXN = TXN.drop("YearMonth")
        TXN.show()
        write_data_sql(TXN,"PurchaseReportWithCP_SP_I",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseReportWithCP_SP_I','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PurchaseReportWithCP_SP_I','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")     

