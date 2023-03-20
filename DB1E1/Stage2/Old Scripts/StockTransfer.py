from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType ,StringType
from pyspark.sql.functions import sum as sumf, first as firstf,month as monthf
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime
import pyodbc
import re,os,datetime
import time,sys

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
    

config = os.path.dirname(os.path.realpath(__file__))
Market = config[config.rfind("/")+1:]
Etn = Market[Market.rfind("E"):]
DB = Market[:Market.rfind("E")]
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)


ACT= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Accounts")
ACT1= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Accounts")
TR= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Tax_Regions")
#exit()
AB = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Agents_Brokers")
AB1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Agents_Brokers")
Ct = sqlctx.read.parquet(hdfspath+"/Market/Stage1/City_")
LM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
LM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
#LM.show()
#exit()

GDM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Godown_Mst")
AG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/AccountGroups")
CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
CCI1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
IMOD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_O_Det")
BG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Groups")
PM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
PM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
CM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comp_Mst")
ICM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
IMN= sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
CID = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_It_Desc")
GM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
IMD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")
IMD1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")

BM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
BM1= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")

IMH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
SL = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLHead1")

def RENAME(df,columns):
    if isinstance(columns, dict):
            for old_name, new_name in columns.items():
                        df = df.withColumnRenamed(old_name, new_name)
            return df
    else:
            raise ValueError("'columns'should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
   



ACT=ACT.select('Act_Name','Print_Act_Name','Act_Code','Grp_Code','City_Code')
Ct=Ct.select('City_Name','City_Code')
AG = AG.select('Grp_Code')
AB=AB.select('Agent_Name','Code')#.withColumnRenamed("Code",'Agent_Code')
AB1=AB1.select('Agent_Name','Code').withColumnRenamed("Code",'Agent_Code')
GDM=GDM.select('godown_Name','godown_Code')
CCI=CCI.select('Code','Exchange_Rate')
IMOD=IMOD.select('Code')
BG=BG.select('Group_Code')
BM=BM.select('Group_Code1','Branch_Name', 'Branch_Code')
CM=CM.select('Comp_Code','Comp_Name')\
        .withColumn("Comp_Code", CM.Comp_Code.cast(IntegerType()))
GM=GM.select('Group_Code','Group_Name')
PM=PM.select('Pack_Code', 'Pack_Name', 'Order_','Link_Code','Pack_Short_Name')\
    .withColumn("Pack_Code", PM.Pack_Code.cast(StringType()))
IMN=IMN.select('Item_Name_Code')
CID=CID.select( 'Item_Desc','Addl_Item_Name','Code')
LM=LM.select('CF_Lot','Basic_rate', 'Expiry', 'Mfg_Date', 'Map_Code','Lot_Code','exp_dmg', 'Org_Lot_Code','Bill_No',\
                'Pur_Date', 'Lot_Number','sale_rate','pur_rate','Item_Det_Code')
LM=LM.withColumnRenamed("Pur_Date","Lot_Pur_Date").withColumnRenamed("Map_Code","Branch_Code1").withColumnRenamed('Bill_No','Lot_Bill_No')
LM=RENAME(LM,{"pur_rate":"Rate_2","sale_rate":"Rate_1"})
IMH=IMH.select('Item_Hd_Code','Item_Hd_Name','comp_code','Color_Code','Group_Code','Item_Name_Code','Comm_It_Desc_Code')\
    .withColumn("Item_Hd_Code", IMH.Item_Hd_Code.cast(StringType()))
ICM=ICM.select('Color_Code')
IMD=IMD.select('Cf_1_Desc', 'Cf_2_Desc', 'Cf_3_Desc', 'Packing', 'Cf_1', 'Cf_2', 'CF_3',\
        'User_Code','Item_Det_Code','Pack_Code','Item_O_Det_Code','Item_Hd_Code')\
        .withColumnRenamed("Cf_1_Desc","ItemDescCf1")\
        .withColumn("User_Code", IMD.User_Code.cast(StringType())).alias("ItemChangeCode")

ACT1=ACT1.select('Act_Name','Print_Act_Name','Act_Code','Grp_Code','City_Code','City_Start_Pos','User_Code')
TR = TR.select('Tax_Reg_Name','Tax_Reg_Code')
CCI1= CCI1.select('Code','Commission_P','Sp_Commission_P','Rdf_P')
BM1=BM1.select('Group_Code1','Branch_Name', 'Branch_Code','Rate_Group_Code')
IMD1= IMD1.select('User_Code','Item_Det_Code','Pack_Code','Item_O_Det_Code','Item_Hd_Code')
PM1=PM1.select('Pack_Code')
LM1= LM1.select('Basic_rate', 'Expiry','Lot_Code', 'Org_Lot_Code','Bill_No','Mrp',\
        'Pur_Date', 'Lot_Number','sale_rate','pur_rate','Item_Det_Code')
SL=SL.select('remarks3','remarks2','vouch1','remarks1').withColumnRenamed("vouch1","Vouchcode")


print(datetime.now())
Start_Year = 2018
try:
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/StockTransfer_InwardDetailedI") !=0):
        PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        PUTXN.cache()
        if(PUTXN.count() > 0):
            PUTXN = PUTXN.join(CCI,PUTXN.Comm_Calc_Code==CCI.Code,how='left')
            PUTXN = PUTXN.join(LM,on =['Lot_Code'],how='left')
            PUTXN = PUTXN.join(IMD,on = ['Item_Det_Code'],how='left')
            PUTXN = PUTXN.join(GDM,on = ['Godown_Code'],how='left')
            PUTXN = PUTXN.join(ACT,PUTXN.Cust_Code==ACT.Act_Code,how='left')
            PUTXN = PUTXN.join(AB,PUTXN.Agent_Code==AB.Code,how='left')
            PUTXN = PUTXN.join(BM,on = ['Branch_Code'], how = 'left')
            PUTXN = PUTXN.join(Ct,on = ['City_Code'], how = 'left')
            PUTXN = PUTXN.join(PM,on = ['Pack_Code'],how='left')
            PUTXN = PUTXN.join(AG,on = ['Grp_Code'], how = 'left')
            PUTXN = PUTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            PUTXN = PUTXN.join(CM,on = ['comp_code'], how = 'left')
            PUTXN = PUTXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
            PUTXN = PUTXN.join(BG,PUTXN.Group_Code1== BG.Group_Code,how='left') 
            PUTXN = PUTXN.join(GM,on = ['Group_Code'], how = 'left')
            PUTXN = PUTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            PUTXN = PUTXN.join(IMOD,PUTXN.Item_O_Det_Code==IMOD.Code,how='left')
            PUTXN = PUTXN.join(CID,PUTXN.Comm_It_Desc_Code==CID.Code,how='left')
    
            #PUTXN.show(1)
    
            PUTXN=PUTXN.withColumn('Rate_Per_Pack',  PUTXN['Rate'] * PUTXN['CF_Qty'])\
                        .withColumn('Tax3',  PUTXN['Calc_Tax_3'] + PUTXN['Calc_Sur_On_Tax3'])\
                        .withColumn('TotQty',PUTXN['Tot_Qty']/1).withColumn('TotQty_1',PUTXN['Tot_Qty']/1)\
                        .withColumn('PurQty',PUTXN['Tot_Qty']/1).withColumn('FreeQty',PUTXN['Free_Qty']/1)\
                        .withColumn('ReplQty',PUTXN['Repl_Qty']/1).withColumn('SampleQty',PUTXN['Sample_Qty']/1)\
                        .withColumn('UserDefNetAmt_1','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] +PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                        .withColumn('UserDefNetAmt_2','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] + PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                        .withColumn('UDR_Amount',  LM['Basic_rate'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_2',  LM['Basic_rate'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_3',  LM['Basic_rate'] * PUTXN['Tot_Qty']) 
    
            PUTXN = PUTXN.withColumn('MonthOrder', when(month(PUTXN.Vouch_Date) < 4, month(PUTXN.Vouch_Date) + 12).otherwise(month(PUTXN.Vouch_Date)))\
                        .withColumn('DayOrder', dayofmonth(PUTXN['Vouch_Date']))\
                        .withColumn('Year', year(PUTXN['Vouch_Date']))
           
            PUTXN = PUTXN.filter(PUTXN.Stock_Trans == 1)\
                            .filter(PUTXN.Deleted_ == 0)
            
            
            #PUTXN.show(1)
            
            PUTXN=PUTXN.groupby('Vouchcode',LM['Lot_Pur_Date'],'YearMonth','Act_Name','Print_Act_Name','Act_Code','CF_Qty','Rate','Pur_Or_PR','City_Name','Vouch_Num','Goods_In_Transit',  PUTXN['Bill_No'], 'Bill_Date', 'GRN_PreFix', 'GRN_Number',\
                                'Agent_Name',AB['Code'],'godown_Name','godown_Code','Lot_Code','exp_dmg', 'Org_Lot_Code',LM['Lot_Bill_No'],'CF_Lot','Basic_rate','Mfg_Date', 'Branch_Code1','Rate_1','Rate_2','Branch_Name', 'Branch_Code','Bill_Amount','Net_Amt', \
                                IMD['ItemDescCf1'], 'Cf_2_Desc', 'Cf_3_Desc', 'Packing', 'Cf_1', 'Cf_2', 'CF_3','User_Code','Exchange_Rate','Pack_Code', 'Pack_Name', 'Order_','Link_Code','Pack_Short_Name','Item_Hd_Code', 'Item_Hd_Name',\
                                'Comp_Code','Comp_Name','Group_Code',GM['Group_Name'],'Vouch_Date','MonthOrder','DayOrder','Year','Item_Desc','Addl_Item_Name')\
                        .agg(sum('Tax3').alias('Tax3'),\
                                sum('ReplQty').alias('ReplQty'),sum('SampleQty').alias('SampleQty'),sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'),\
                                sum("Rate_Per_Pack").alias("Rate_Per_Pack"),sum('Qty_Weight').alias("Qty_Weight"),sum('TotQty').alias("TotQty"),sum('TotQty_1').alias('TotQty_1'),sum('PurQty').alias('PurQty'),\
                                sum('FreeQty').alias('FreeQty'),sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),\
                                sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                                sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'))
            print(PUTXN.count())
            #PUTXN.show(1)
            #PUTXN=PUTXN.orderBy('Branch_Name', 'Vouch_Date','Vouch_Num',  'Lot_Code',  'Godown_Name',  'Act_Name',  'Group_Name', 'User_Code', 'Item_Hd_Name', 'Order_', 'Pack_Name', 'Comp_Name','Item_Desc', 'GRN_PreFix', 'GRN_Number')
    
            PUTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_InwardDetailedI")
            print("StockTransfer_InwardDetailedI Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_InwardDetailedI','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0'+ str(month)
                else:
                    cdm = cdm + str(month)
                PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/StockTransfer_InwardDetailedI/YearMonth="+cdm)
                
                PUTXN = PUTXN.join(CCI,PUTXN.Comm_Calc_Code==CCI.Code,how='left')
                PUTXN = PUTXN.join(LM,on =['Lot_Code'],how='left')
                PUTXN = PUTXN.join(IMD,on = ['Item_Det_Code'],how='left')
                PUTXN = PUTXN.join(GDM,on = ['Godown_Code'],how='left')
                PUTXN = PUTXN.join(ACT,PUTXN.Cust_Code==ACT.Act_Code,how='left')
                PUTXN = PUTXN.join(AB,PUTXN.Agent_Code==AB.Code,how='left')
                PUTXN = PUTXN.join(BM,on = ['Branch_Code'], how = 'left')
                PUTXN = PUTXN.join(Ct,on = ['City_Code'], how = 'left')
                PUTXN = PUTXN.join(PM,on = ['Pack_Code'],how='left')
                PUTXN = PUTXN.join(AG,on = ['Grp_Code'], how = 'left')
                PUTXN = PUTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                PUTXN = PUTXN.join(CM,on = ['comp_code'], how = 'left')
                PUTXN = PUTXN.join(ICM,on = ['Color_Code'], how = 'left').drop('Group_Code')
                PUTXN = PUTXN.join(BG,PUTXN.Group_Code1== BG.Group_Code,how='left') 
                PUTXN = PUTXN.join(GM,on = ['Group_Code'], how = 'left')
                PUTXN = PUTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                PUTXN = PUTXN.join(IMOD,PUTXN.Item_O_Det_Code==IMOD.Code,how='left')
                PUTXN = PUTXN.join(CID,PUTXN.Comm_It_Desc_Code==CID.Code,how='left')
                #print('data')
    
                PUTXN=PUTXN.withColumn('Rate_Per_Pack',  PUTXN['Rate'] * PUTXN['CF_Qty'])\
                            .withColumn('Tax3',  PUTXN['Calc_Tax_3'] + PUTXN['Calc_Sur_On_Tax3'])\
                            .withColumn('TotQty',PUTXN['Tot_Qty']/1).withColumn('TotQty_1',PUTXN['Tot_Qty']/1)\
                            .withColumn('PurQty',PUTXN['Tot_Qty']/1).withColumn('FreeQty',PUTXN['Free_Qty']/1)\
                            .withColumn('ReplQty',PUTXN['Repl_Qty']/1).withColumn('SampleQty',PUTXN['Sample_Qty']/1)\
                            .withColumn('UserDefNetAmt_1','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] +PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                            .withColumn('UserDefNetAmt_2','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] + PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                            .withColumn('UDR_Amount',  LM['Basic_rate'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_2',  LM['Basic_rate'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_3',  LM['Basic_rate'] * PUTXN['Tot_Qty']) 
                #print('data1')
                PUTXN = PUTXN.withColumn('MonthOrder', when(monthf(PUTXN.Vouch_Date) < 4, monthf(PUTXN.Vouch_Date) + 12).otherwise(monthf(PUTXN.Vouch_Date)))\
                            .withColumn('DayOrder', dayofmonth(PUTXN['Vouch_Date']))\
                            .withColumn('Year', year(PUTXN['Vouch_Date']))
                PUTXN = PUTXN.filter(PUTXN.Stock_Trans == 1).filter(PUTXN.Deleted_ == 0)
                #print('data2')
                PUTXN = PUTXN.withColumn('YearMonth',lit(cdm))
                
                PUTXN=PUTXN.groupby('Vouchcode',LM['Lot_Pur_Date'],'YearMonth','Act_Name','Print_Act_Name','Act_Code','CF_Qty','Rate','Pur_Or_PR','City_Name','Vouch_Num','Goods_In_Transit',  PUTXN['Bill_No'], 'Bill_Date', 'GRN_PreFix', 'GRN_Number',\
                                    'Agent_Name',AB['Code'],'godown_Name','godown_Code','Lot_Code','exp_dmg', 'Org_Lot_Code',LM['Lot_Bill_No'],'CF_Lot','Basic_rate','Mfg_Date', 'Branch_Code1','Rate_1','Rate_2','Branch_Name', 'Branch_Code','Bill_Amount','Net_Amt', \
                                    IMD['ItemDescCf1'], 'Cf_2_Desc', 'Cf_3_Desc', 'Packing', 'Cf_1', 'Cf_2', 'CF_3','User_Code','Exchange_Rate','Pack_Code', 'Pack_Name', 'Order_','Link_Code','Pack_Short_Name','Item_Hd_Code', 'Item_Hd_Name',\
                                    'Comp_Code','Comp_Name','Group_Code',GM['Group_Name'],'Vouch_Date','MonthOrder','DayOrder','Year','Item_Desc','Addl_Item_Name')\
                            .agg(sum('Tax3').alias('Tax3'),\
                                    sum('ReplQty').alias('ReplQty'),sum('SampleQty').alias('SampleQty'),sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                    sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'),\
                                    sum("Rate_Per_Pack").alias("Rate_Per_Pack"),sum('Qty_Weight').alias("Qty_Weight"),sum('TotQty').alias("TotQty"),sum('TotQty_1').alias('TotQty_1'),sum('PurQty').alias('PurQty'),\
                                    sum('FreeQty').alias('FreeQty'),sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),\
                                    sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                    sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                    sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                    sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                    sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                                    sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'))
                #print('data4')
                #PUTXN=PUTXN.orderBy('Branch_Name', 'Vouch_Date','Vouch_Num',  'Lot_Code',  'Godown_Name',  'Act_Name',  'Group_Name', 'User_Code', 'Item_Hd_Name', 'Order_', 'Pack_Name', 'Comp_Name','Item_Desc', 'GRN_PreFix', 'GRN_Number')
    
                PUTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_InwardDetailedI")
                delete_data(' DELETE FROM KOCKPIT.dbo.StockTransfer_InwardDetailedI WHERE YearMonth = ' + cdm)
                print("StockTransfer_InwardDetailedI Done")
            except Exception as e:
                print(e)
                #exit()
        print(datetime.now())
        PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/StockTransfer_InwardDetailedI")
        PUTXN = PUTXN.drop("YearMonth")
        #PUTXN.show(10,False)
        write_data_sql(PUTXN,"StockTransfer_InwardDetailedI",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_InwardDetailedI','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_InwardDetailedI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  
        
    
    
try:    
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/StockTransfer_INSummaryI") !=0):
        PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData")
        PUTXN.cache()
        if(PUTXN.count() > 0):
            PUTXN = PUTXN.join(CCI,PUTXN.Comm_Calc_Code==CCI.Code,how='left')           
            PUTXN = PUTXN.join(LM,on =['Lot_Code'],how='left')
            PUTXN = PUTXN.join(IMD,on=['Item_Det_Code'],how='left')
            PUTXN = PUTXN.join(GDM,on = ['Godown_Code'],how='left')
            PUTXN = PUTXN.join(ACT,PUTXN.Cust_Code==ACT.Act_Code,how='left')
            PUTXN = PUTXN.join(AB,PUTXN.Agent_Code==AB.Code,how='left')    
            PUTXN = PUTXN.join(BM,on = ['Branch_Code'], how = 'left')
            PUTXN = PUTXN.join(Ct,on = ['City_Code'], how = 'left')
            PUTXN = PUTXN.join(PM,on = ['Pack_Code'],how='left')
            PUTXN = PUTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            PUTXN = PUTXN.join(CM,on = ['comp_code'], how = 'left')
            PUTXN = PUTXN.join(ICM,on = ['Color_Code'], how = 'left')
            PUTXN = PUTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            PUTXN = PUTXN.join(GM,on = ['Group_Code'], how = 'left') 
            PUTXN = PUTXN.join(IMOD,PUTXN.Item_O_Det_Code==IMOD.Code,how='left')
            PUTXN = PUTXN.join(BG,PUTXN.Group_Code1==BG.Group_Code,how='left')
    
            PUTXN =PUTXN.withColumn('TotQty',PUTXN['Tot_Qty']/1).withColumn('TotQty_1',PUTXN['Tot_Qty']/1)\
                      .withColumn('PurQty',PUTXN['Tot_Qty']/1).withColumn('FreeQty',PUTXN['Free_Qty']/1)\
                      .withColumn('ReplQty',PUTXN['Repl_Qty']/1).withColumn('SampleQty',PUTXN['Sample_Qty']/1)\
                      .withColumn('Tax3',PUTXN['Calc_Tax_3'] + PUTXN['Calc_Sur_On_Tax3']).withColumn('UserDefNetAmt_1','0'+PUTXN['Calc_Gross_Amt'])\
                      .withColumn('UserDefNetAmt_2','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] + PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                      .withColumn('UDR_Amount',  LM['Rate_2'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_2',  LM['Rate_1'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_3',  LM['Basic_rate'] * PUTXN['Tot_Qty'])
        
            
            
            PUTXN = PUTXN.withColumn('MonthOrder', when(month(PUTXN.Vouch_Date) < 4, month(PUTXN.Vouch_Date) + 12).otherwise(month(PUTXN.Vouch_Date)))\
                        .withColumn('DayOrder', dayofmonth(PUTXN['Vouch_Date']))\
                        .withColumn('Year', year(PUTXN['Vouch_Date']))
            PUTXN = PUTXN.filter(PUTXN.Stock_Trans == 1).filter(PUTXN.Deleted_ == 0)
    
            PUTXN=PUTXN.groupby('Vouchcode','YearMonth',ACT['Act_Name'],'Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit',  'Bill_No', 'Bill_Date', 'GRN_PreFix', 'GRN_Number','Pur_Or_PR',\
                              'Agent_Name',AB['Code'],BM['Branch_Name'], 'Branch_Code','Bill_Amount','Net_Amt','Vouch_Date','MonthOrder','DayOrder','Year','exp_dmg')\
                         .agg(sum('Tax3').alias('Tax3'),sum('TotQty').alias('TotQty'),sum('TotQty_1').alias('TotQty_1'),sum('PurQty').alias('PurQty'),sum('FreeQty').alias('FreeQty'),\
                            sum('ReplQty').alias('ReplQty'),sum('SampleQty').alias('SampleQty'),sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                            sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'),\
                            sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),\
                            sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                            sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                            sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                            sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                            sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                            sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'))
            
            PUTXN=PUTXN.orderBy('Branch_Name', PUTXN['Vouch_Date'],PUTXN['Vouch_Num'],  ACT['Act_Name'], 'GRN_PreFix', 'GRN_Number')
    
            PUTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_INSummaryI")
            print("StockTransfer_INSummaryI Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_INSummaryI','DB':DB,'EN':Etn,
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
                if(month <= 9):
                    cdm = cdm + '0'+ str(month)
                else:
                    cdm = cdm + str(month)
                PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PurchaseData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/StockTransfer_INSummaryI/YearMonth="+cdm)
                
                PUTXN = PUTXN.join(CCI,PUTXN.Comm_Calc_Code==CCI.Code,how='left')           
                PUTXN = PUTXN.join(LM,on =['Lot_Code'],how='left')
                PUTXN = PUTXN.join(IMD,on=['Item_Det_Code'],how='left')
                PUTXN = PUTXN.join(GDM,on = ['Godown_Code'],how='left')
                PUTXN = PUTXN.join(ACT,PUTXN.Cust_Code==ACT.Act_Code,how='left')
                PUTXN = PUTXN.join(AB,PUTXN.Agent_Code==AB.Code,how='left')    
                PUTXN = PUTXN.join(BM,on = ['Branch_Code'], how = 'left')
                PUTXN = PUTXN.join(Ct,on = ['City_Code'], how = 'left')
                PUTXN = PUTXN.join(PM,on = ['Pack_Code'],how='left')
                PUTXN = PUTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                PUTXN = PUTXN.join(CM,on = ['comp_code'], how = 'left')
                PUTXN = PUTXN.join(ICM,on = ['Color_Code'], how = 'left')
                PUTXN = PUTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                PUTXN = PUTXN.join(GM,on = ['Group_Code'], how = 'left') 
                PUTXN = PUTXN.join(IMOD,PUTXN.Item_O_Det_Code==IMOD.Code,how='left')
                PUTXN = PUTXN.join(BG,PUTXN.Group_Code1==BG.Group_Code,how='left')
    
                PUTXN =PUTXN.withColumn('TotQty',PUTXN['Tot_Qty']/1).withColumn('TotQty_1',PUTXN['Tot_Qty']/1)\
                          .withColumn('PurQty',PUTXN['Tot_Qty']/1).withColumn('FreeQty',PUTXN['Free_Qty']/1)\
                          .withColumn('ReplQty',PUTXN['Repl_Qty']/1).withColumn('SampleQty',PUTXN['Sample_Qty']/1)\
                          .withColumn('Tax3',PUTXN['Calc_Tax_3'] + PUTXN['Calc_Sur_On_Tax3']).withColumn('UserDefNetAmt_1','0'+PUTXN['Calc_Gross_Amt'])\
                          .withColumn('UserDefNetAmt_2','0'+ PUTXN['Calc_Gross_Amt'] + PUTXN['Calc_commission'] + PUTXN['calc_sp_commission'] + PUTXN['calc_rdf'] + PUTXN['calc_scheme_u'] + PUTXN['calc_scheme_rs'] + PUTXN['Calc_Tax_1'] + PUTXN['Calc_Tax_2'] + PUTXN['Calc_Tax_3'] + PUTXN['calc_sur_on_tax3'] + PUTXN['calc_mfees'] + PUTXN['calc_excise_u'] + PUTXN['Calc_adjustment_u'] + PUTXN['Calc_adjust_rs'] + PUTXN['Calc_freight'] + PUTXN['calc_adjust'] + PUTXN['Calc_Spdisc'] + PUTXN['Calc_DN'] + PUTXN['Calc_CN'] + PUTXN['Calc_Display'] + PUTXN['Calc_Handling'] + PUTXN['calc_Postage'] + PUTXN['calc_round'])\
                          .withColumn('UDR_Amount',  LM['Rate_2'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_2',  LM['Rate_1'] * PUTXN['Tot_Qty']).withColumn('UDR_Amount_3',  LM['Basic_rate'] * PUTXN['Tot_Qty'])
            
                
                
                PUTXN = PUTXN.withColumn('MonthOrder', when(monthf(PUTXN.Vouch_Date) < 4, monthf(PUTXN.Vouch_Date) + 12).otherwise(monthf(PUTXN.Vouch_Date)))\
                            .withColumn('DayOrder', dayofmonth(PUTXN['Vouch_Date']))\
                            .withColumn('Year', year(PUTXN['Vouch_Date']))
                PUTXN = PUTXN.filter(PUTXN.Stock_Trans == 1)\
                                .filter(PUTXN.Deleted_ == 0)
                PUTXN = PUTXN.withColumn('YearMonth',lit(cdm))
    
                PUTXN=PUTXN.groupby('Vouchcode','YearMonth',ACT['Act_Name'],'Print_Act_Name','Act_Code','City_Name','Vouch_Num','Goods_In_Transit',  'Bill_No', 'Bill_Date', 'GRN_PreFix', 'GRN_Number','Pur_Or_PR',\
                                  'Agent_Name',AB['Code'],BM['Branch_Name'], 'Branch_Code','Bill_Amount','Net_Amt','Vouch_Date','MonthOrder','DayOrder','Year','exp_dmg')\
                             .agg(sum('Tax3').alias('Tax3'),sum('TotQty').alias('TotQty'),sum('TotQty_1').alias('TotQty_1'),sum('PurQty').alias('PurQty'),sum('FreeQty').alias('FreeQty'),\
                                sum('ReplQty').alias('ReplQty'),sum('SampleQty').alias('SampleQty'),sum('UserDefNetAmt_1').alias('UserDefNetAmt_1'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                sum('UDR_Amount').alias('UDR_Amount'),sum('UDR_Amount_2').alias('UDR_Amount_2'),sum('UDR_Amount_3').alias('UDR_Amount_3'),\
                                sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),\
                                sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                                sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'))
                
                PUTXN=PUTXN.orderBy('Branch_Name', PUTXN['Vouch_Date'],PUTXN['Vouch_Num'],  ACT['Act_Name'], 'GRN_PreFix', 'GRN_Number')
                PUTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_INSummaryI")
                delete_data(' DELETE FROM KOCKPIT.dbo.StockTransfer_INSummaryI WHERE YearMonth = ' + cdm)
                print("StockTransfer_INSummaryI Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        PUTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/StockTransfer_INSummaryI")
        PUTXN = PUTXN.drop("YearMonth")
        PUTXN.show(1)
        write_data_sql(PUTXN,"StockTransfer_INSummaryI",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_INSummaryI','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_INSummaryI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  
        
    
try:   
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/StockTransfer_OUTDetailedI") !=0):
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData")
        SLTXN.cache()
        if(SLTXN.count() > 0):
            SLTXN = SLTXN.join(CCI1,SLTXN.Comm_Calc_Code==CCI1.Code,how='left')
            SLTXN = SLTXN.join(ACT1,SLTXN.Cust_Code==ACT1.Act_Code,how='left') 
            SLTXN = SLTXN.join(IMD1,on = ['Item_Det_Code'],how='left')
            SLTXN = SLTXN.join(GDM,on = ['Godown_Code'],how='left')
            SLTXN = SLTXN.join(BM1,on = ['Branch_Code'], how = 'left')
            SLTXN = SLTXN.join(Ct,on = ['City_Code'], how = 'left')
            SLTXN = SLTXN.join(TR,on = ['Tax_Reg_Code'],how='left')
            SLTXN = SLTXN.join(LM1,on =['Lot_Code'],how='left')
            SLTXN = SLTXN.join(AG,on = ['Grp_Code'], how = 'left')
            #SLTXN = SLTXN.join(ACT1,SLTXN.Act_Code_For_Txn_X==ACT1.Act_Code,how='left')
            SLTXN = SLTXN.join(AB,SLTXN.Agent_Code==AB.Code,how='left')
            SLTXN = SLTXN.join(PM1,on = ['Pack_Code'],how='left')
            SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
            SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')#.drop('Group_Code')
            SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
            SLTXN = SLTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            SLTXN = SLTXN.join(CID,SLTXN.Comm_It_Desc_Code==CID.Code,how='left')
            SLTXN = SLTXN.join(BG,SLTXN.Group_Code1== BG.Group_Code,how='left') 
            SLTXN = SLTXN.join(IMOD,SLTXN.Item_O_Det_Code==IMOD.Code,how='left') 
            SLTXN = SLTXN.join(SL,on = ['Vouchcode'],how='left')    
    
            SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn('SaleQty',SLTXN['Tot_Qty']/1).withColumn('NetPUAmount',SLTXN['pur_rate']*SLTXN['Tot_Qty']).withColumn('NetSPAmount',SLTXN['Mrp']*SLTXN['Tot_Qty']).withColumn('MarkDownOnGrossValue',SLTXN['Mrp']-SLTXN['Rate'])\
                    .withColumn('Free_Value',  LM1['Basic_rate'] * SLTXN['Free_Qty']).withColumn('SampleQty',SLTXN['Sample_Qty']/1).withColumn('Rate_Per_Pack',SLTXN['Rate'] * SLTXN['CF_Qty'])\
                    .withColumn('UDR_Amount',  LM1['Sale_rate'] * SLTXN['Tot_Qty']).withColumn('Repl_Value',  LM1['Basic_rate'] * SLTXN['Repl_Qty']).withColumn('Sample_Value',  LM1['Basic_rate'] * SLTXN['Sample_Qty'])\
                    .withColumn('UDR_Amount_1',  LM1['Sale_rate'] * SLTXN['Tot_Qty']).withColumn('Rate_Per_Pack',  SLTXN['Rate'] * SLTXN['CF_Qty']).withColumn('Int_Total_Packs',  SLTXN['Tot_Qty'] / SLTXN['CF_Qty'])\
                    .withColumn('UserDefNetAmt_2','0'+ SLTXN['Calc_Gross_Amt'] +  SLTXN['Calc_commission'] +  SLTXN['calc_sp_commission'] +  SLTXN['calc_rdf'] +  SLTXN['calc_scheme_u'] +  SLTXN['calc_scheme_rs'] +  SLTXN['Calc_Tax_1'] +  SLTXN['Calc_Tax_2'] +  SLTXN['Calc_Tax_3'] +  SLTXN['calc_sur_on_tax3'] +  SLTXN['calc_mfees'] +  SLTXN['calc_excise_u'] +  SLTXN['Calc_adjustment_u'] +  SLTXN['Calc_adjust_rs'] +  SLTXN['Calc_freight'] +  SLTXN['calc_adjust'] +  SLTXN['Calc_Spdisc'] +  SLTXN['Calc_DN'] +  SLTXN['Calc_CN'] +  SLTXN['Calc_Display'] +  SLTXN['Calc_Handling'] +  SLTXN['calc_Postage'] +  SLTXN['calc_Round'] +  SLTXN['calc_Labour'])  
            
            SLTXN = SLTXN.withColumn('MonthOrder', when(month(SLTXN.Vouch_Date) < 4, month(SLTXN.Vouch_Date) + 12).otherwise(month(SLTXN.Vouch_Date)))\
                            .withColumn('DayOrder', dayofmonth(SLTXN['Vouch_Date']))\
                            .withColumn('Year', year(SLTXN['Vouch_Date']))
                           
            SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 1).filter(SLTXN.Sa_Subs_Lot == 0).filter(SLTXN.Sale_Or_SR == 'SL').filter(SLTXN.Deleted == 0)             
    
                
            SLTXN=SLTXN.groupby('remarks1','remarks3','remarks2','MarkDownOnGrossValue','NetPUAmount','NetSPAmount','Calc_Tax_3','Branch_Name','Vouchcode','YearMonth','Act_Name','Print_Act_Name','Act_Code','City_Code','City_Start_Pos',ACT1['User_Code'],'Rate_Per_Pack','Sale_Or_SR','Tax_Reg_Name','Lot_Code', 'Org_Lot_Code',\
                                    'City_Name','Lot_Number','Bill_Cust_Code','Vouch_Num', 'Vouch_Date',  'Series_Code', 'Number_', 'Net_Amt','Pay_Mode','New_Vouch_Num','MonthOrder','Item_Desc',\
                                    'Deleted','Basic_rate','pur_rate','Mrp','Commission_P','Sp_Commission_P','Rdf_P','Item_Hd_Code', 'Item_Hd_Name','Comp_Code', 'Comp_Name','Group_Name')\
                           .agg(sum('TotQty').alias('TotQty'),sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('FreeQty'),sum('SampleQty').alias('SampleQty'),sum('Free_Value').alias('Free_Value'),sum('Sample_Value').alias('Sample_Value'),sum('Repl_Value').alias('Repl_Value'),sum('UDR_Amount_1').alias('UDR_Amount_1'),sum('UDR_Amount').alias('UDR_Amount'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                avg('CF_Qty').alias('CF_Qty'),avg('Rate').alias('RatePerUnit'),sum('Qty_Weight').alias("Qty_Weight"),sum('Int_Total_Packs').alias('Int_Total_Packs'),\
                                sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),sum('Repl_Qty').alias('ReplQty'),\
                                sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'),avg('Sale_rate').alias('UDR_Rate'),\
                                sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'),sum('calc_round').alias('Round_Amt'))
                                
                
    
            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_OUTDetailedI")
            print("StockTransfer_OUTDetailedI Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_OUTDetailedI','DB':DB,'EN':Etn,
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
                    cdm = cdm + '0'+ str(month)
                else:
                    cdm = cdm + str(month)
                SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/StockTransfer_OUTDetailedI/YearMonth="+cdm)
                
                SLTXN = SLTXN.join(CCI1,SLTXN.Comm_Calc_Code==CCI1.Code,how='left')
                SLTXN = SLTXN.join(ACT1,SLTXN.Cust_Code==ACT1.Act_Code,how='left') 
                SLTXN = SLTXN.join(IMD1,on = ['Item_Det_Code'],how='left')
                SLTXN = SLTXN.join(GDM,on = ['Godown_Code'],how='left')
                SLTXN = SLTXN.join(BM1,on = ['Branch_Code'], how = 'left')
                SLTXN = SLTXN.join(Ct,on = ['City_Code'], how = 'left')
                SLTXN = SLTXN.join(TR,on = ['Tax_Reg_Code'],how='left')
                SLTXN = SLTXN.join(LM1,on =['Lot_Code'],how='left')
                SLTXN = SLTXN.join(AG,on = ['Grp_Code'], how = 'left')
                #SLTXN = SLTXN.join(ACT1,SLTXN.Act_Code_For_Txn_X==ACT1.Act_Code,how='left')
                SLTXN = SLTXN.join(AB,SLTXN.Agent_Code==AB.Code,how='left')
                SLTXN = SLTXN.join(PM1,on = ['Pack_Code'],how='left')
                SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
                SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')#.drop('Group_Code')
                SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
                SLTXN = SLTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
                SLTXN = SLTXN.join(CID,SLTXN.Comm_It_Desc_Code==CID.Code,how='left')
                SLTXN = SLTXN.join(BG,SLTXN.Group_Code1== BG.Group_Code,how='left') 
                SLTXN = SLTXN.join(IMOD,SLTXN.Item_O_Det_Code==IMOD.Code,how='left') 
                SLTXN = SLTXN.join(SL,on = ['Vouchcode'],how='left')    
        
                SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn('SaleQty',SLTXN['Tot_Qty']/1).withColumn('NetPUAmount',SLTXN['pur_rate']*SLTXN['Tot_Qty']).withColumn('NetSPAmount',SLTXN['Mrp']*SLTXN['Tot_Qty']).withColumn('MarkDownOnGrossValue',SLTXN['Mrp']-SLTXN['Rate'])\
                    .withColumn('Free_Value',  LM1['Basic_rate'] * SLTXN['Free_Qty']).withColumn('SampleQty',SLTXN['Sample_Qty']/1).withColumn('Rate_Per_Pack',SLTXN['Rate'] * SLTXN['CF_Qty'])\
                    .withColumn('UDR_Amount',  LM1['Sale_rate'] * SLTXN['Tot_Qty']).withColumn('Repl_Value',  LM1['Basic_rate'] * SLTXN['Repl_Qty']).withColumn('Sample_Value',  LM1['Basic_rate'] * SLTXN['Sample_Qty'])\
                    .withColumn('UDR_Amount_1',  LM1['Sale_rate'] * SLTXN['Tot_Qty']).withColumn('Rate_Per_Pack',  SLTXN['Rate'] * SLTXN['CF_Qty']).withColumn('Int_Total_Packs',  SLTXN['Tot_Qty'] / SLTXN['CF_Qty'])\
                    .withColumn('UserDefNetAmt_2','0'+ SLTXN['Calc_Gross_Amt'] +  SLTXN['Calc_commission'] +  SLTXN['calc_sp_commission'] +  SLTXN['calc_rdf'] +  SLTXN['calc_scheme_u'] +  SLTXN['calc_scheme_rs'] +  SLTXN['Calc_Tax_1'] +  SLTXN['Calc_Tax_2'] +  SLTXN['Calc_Tax_3'] +  SLTXN['calc_sur_on_tax3'] +  SLTXN['calc_mfees'] +  SLTXN['calc_excise_u'] +  SLTXN['Calc_adjustment_u'] +  SLTXN['Calc_adjust_rs'] +  SLTXN['Calc_freight'] +  SLTXN['calc_adjust'] +  SLTXN['Calc_Spdisc'] +  SLTXN['Calc_DN'] +  SLTXN['Calc_CN'] +  SLTXN['Calc_Display'] +  SLTXN['Calc_Handling'] +  SLTXN['calc_Postage'] +  SLTXN['calc_Round'] +  SLTXN['calc_Labour'])  
            
                SLTXN = SLTXN.withColumn('MonthOrder', when(monthf(SLTXN.Vouch_Date) < 4, monthf(SLTXN.Vouch_Date) + 12).otherwise(monthf(SLTXN.Vouch_Date)))\
                                .withColumn('DayOrder', dayofmonth(SLTXN['Vouch_Date']))\
                                .withColumn('Year', year(SLTXN['Vouch_Date']))
                               
                SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 1).filter(SLTXN.Sa_Subs_Lot == 0).filter(SLTXN.Sale_Or_SR == 'SL').filter(SLTXN.Deleted == 0)             
                SLTXN = SLTXN.withColumn('YearMonth',lit(cdm))
                    
                SLTXN=SLTXN.groupby('remarks3','remarks2','remarks1','MarkDownOnGrossValue','NetPUAmount','NetSPAmount','Calc_Tax_3','Branch_Name','Vouchcode','YearMonth','Act_Name','Print_Act_Name','Act_Code','City_Code','City_Start_Pos',ACT1['User_Code'],'Rate_Per_Pack','Sale_Or_SR','Tax_Reg_Name','Lot_Code', 'Org_Lot_Code',\
                                        'City_Name','Lot_Number','Bill_Cust_Code','Vouch_Num', 'Vouch_Date',  'Series_Code', 'Number_', 'Net_Amt','Pay_Mode','New_Vouch_Num','MonthOrder','Item_Desc',\
                                        'Deleted','Basic_rate','pur_rate','Mrp','Commission_P','Sp_Commission_P','Rdf_P','Item_Hd_Code', 'Item_Hd_Name','Comp_Code', 'Comp_Name','Group_Name')\
                               .agg(sum('TotQty').alias('TotQty'),sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('FreeQty'),sum('SampleQty').alias('SampleQty'),sum('Free_Value').alias('Free_Value'),sum('Sample_Value').alias('Sample_Value'),sum('Repl_Value').alias('Repl_Value'),sum('UDR_Amount_1').alias('UDR_Amount_1'),sum('UDR_Amount').alias('UDR_Amount'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                                    avg('CF_Qty').alias('CF_Qty'),avg('Rate').alias('RatePerUnit'),sum('Qty_Weight').alias("Qty_Weight"),sum('Int_Total_Packs').alias('Int_Total_Packs'),\
                                    sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),sum('Repl_Qty').alias('ReplQty'),\
                                    sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                                    sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                                    sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                                    sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                                    sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'),avg('Sale_rate').alias('UDR_Rate'),\
                                    sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'),sum('calc_round').alias('Round_Amt'))
                                    
                    
                
                SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_OUTDetailedI")
                delete_data(' DELETE FROM KOCKPIT.dbo.StockTransfer_OUTDetailedI WHERE YearMonth = ' + cdm)
                print("StockTransfer_OUTDetailedI Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/StockTransfer_OUTDetailedI")
        SLTXN = SLTXN.drop("YearMonth")
        write_data_sql(SLTXN,"StockTransfer_OUTDetailedI",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_OUTDetailedI','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_OUTDetailedI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  


##StockTransfer_OUTSummaryI
try:    
    if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/StockTransfer_OUTSummaryI") !=0):
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData")
        SLTXN.cache()
        if(SLTXN.count() > 0):
            SLTXN=  SLTXN.join(CCI1,SLTXN.Comm_Calc_Code==CCI1.Code,how='right')
            SLTXN = SLTXN.join(AB1,on=['Agent_Code'],how='left')
            SLTXN = SLTXN.join(ACT1,SLTXN.Cust_Code==ACT1.Act_Code,how='left') 
            SLTXN = SLTXN.join(IMD1,on = ['Item_Det_Code'],how='left')
            SLTXN = SLTXN.join(GDM,on = ['Godown_Code'],how='left')
            SLTXN = SLTXN.join(BM1,on = ['Branch_Code'], how = 'left')
            SLTXN = SLTXN.join(Ct,on = ['City_Code'], how = 'left')
            SLTXN = SLTXN.join(TR,on = ['Tax_Reg_Code'],how='left')
            SLTXN = SLTXN.join(LM1,on =['Lot_Code'],how='left')
            SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
            SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')
            SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
            SLTXN = SLTXN.join(IMOD,IMD1.Item_O_Det_Code==IMOD.Code,how='left')
            SLTXN = SLTXN.join(CID,SLTXN.Comm_It_Desc_Code==CID.Code,how='left')
            SLTXN = SLTXN.join(BG,SLTXN.Group_Code1== BG.Group_Code,how='left')      
    
            SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn('SaleQty',SLTXN['Tot_Qty']/1)\
                    .withColumn('ReplQty',SLTXN['Repl_Qty']/1).withColumn('SampleQty',SLTXN['Sample_Qty']/1).withColumn('Repl_Value',  LM1['Basic_rate'] * SLTXN['Repl_Qty']).withColumn('Sample_Value',  LM1['Basic_rate'] * SLTXN['Sample_Qty'])\
                    .withColumn('Free_Value',  LM1['Basic_rate'] * SLTXN['Free_Qty']).withColumn('Rate_Per_Pack',SLTXN['Rate'] * SLTXN['CF_Qty']).withColumn('Int_Total_Packs',  SLTXN['Tot_Qty'] / SLTXN  ['CF_Qty']) \
                    .withColumn('UDR_Amount',LM1['Sale_rate'] * SLTXN['Tot_Qty'])\
                    .withColumn('UserDefNetAmt','0'+ SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] +SLTXN['calc_rdf'] +SLTXN['calc_scheme_u'] +SLTXN['calc_scheme_rs'] +SLTXN['Calc_Tax_1'] +SLTXN['Calc_Tax_2'] +SLTXN['Calc_Tax_3'] +SLTXN['calc_sur_on_tax3'] +SLTXN['calc_mfees'] +SLTXN['calc_excise_u'] +SLTXN['Calc_adjustment_u'] +SLTXN['Calc_adjust_rs'] +SLTXN['Calc_freight'] +SLTXN['calc_adjust'] +SLTXN['Calc_Spdisc'] +SLTXN['Calc_DN'] +SLTXN['Calc_CN'] +SLTXN['Calc_Display'] +SLTXN['Calc_Handling'] +SLTXN['calc_Postage'] +SLTXN['calc_Round'] +SLTXN['calc_Labour'])\
                    .withColumn('UserDefNetAmt_2','0'+ SLTXN['Calc_Gross_Amt']).withColumn('UDR_Amount_1',LM1['Sale_rate'] * SLTXN['Tot_Qty'])
        
            SLTXN = SLTXN.withColumn('MonthOrder', when(month(SLTXN.Vouch_Date) < 4, month(SLTXN.Vouch_Date) + 12).otherwise(month(SLTXN.Vouch_Date)))\
                            .withColumn('DayOrder', dayofmonth(SLTXN['Vouch_Date']))\
                            .withColumn('Year', year(SLTXN['Vouch_Date']))
                           
            SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 1).filter(SLTXN.Sa_Subs_Lot == 0).filter(SLTXN.Sale_Or_SR == 'SL').filter(SLTXN.Deleted == 0)             
    
                
            SLTXN=SLTXN.groupby('Vouchcode','YearMonth','Act_Name','Act_Code','City_Name','City_Start_Pos',ACT1['User_Code'],'Sale_Or_SR','Tax_Reg_Name','Deleted','New_Vouch_Num','MonthOrder',\
                              'Bill_Cust_Code','Vouch_Num', 'Vouch_Date',  'Series_Code', 'Number_', 'Net_Amt','Pay_Mode','Branch_Name', 'Branch_Code','Rate_Group_Code','pur_rate',CCI1['Commission_P'],CCI1['Sp_Commission_P'] ,CCI1['Rdf_P'])\
                     .agg(sum('UDR_Amount_1').alias('UDR_Amount_1'),avg('Sale_rate').alias('UDR_Rate'),sum('TotQty').alias('TotQty'),sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('FreeQty'),sum('SampleQty').alias('SampleQty'),sum('Free_Value').alias('Free_Value'),sum('Sample_Value').alias('Sample_Value'),sum('Repl_Value').alias('Repl_Value'),sum('UDR_Amount').alias('UDR_Amount'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                        avg('Rate').alias('RatePerUnit'),avg('CF_Qty').alias('CF_Qty'),sum('Qty_Weight').alias("Qty_Weight"),sum('Int_Total_Packs').alias('Int_Total_Packs'),sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),\
                        sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                        sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                        sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                        sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                        sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                        sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'),sum('calc_round').alias('Round_Amt'))
        
            SLTXN=SLTXN.orderBy(BM1['Branch_Name'] ,'Vouch_Date','Series_Code','Number_',  'Act_Name', 'Sale_Or_SR')
                
    
            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_OUTSummaryI")
            print("StockTransfer_OUTSummaryI Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_OUTSummaryI','DB':DB,'EN':Etn,
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
                    cdm = cdm + '0'+ str(month)
                else:
                    cdm = cdm + str(month)
                SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData/YearMonth="+cdm)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/StockTransfer_OUTSummaryI/YearMonth="+cdm)
                
                SLTXN=  SLTXN.join(CCI1,SLTXN.Comm_Calc_Code==CCI1.Code,how='right')
                SLTXN = SLTXN.join(AB1,on=['Agent_Code'],how='left')
                SLTXN = SLTXN.join(ACT1,SLTXN.Cust_Code==ACT1.Act_Code,how='left') 
                SLTXN = SLTXN.join(IMD1,on = ['Item_Det_Code'],how='left')
                SLTXN = SLTXN.join(GDM,on = ['Godown_Code'],how='left')
                SLTXN = SLTXN.join(BM1,on = ['Branch_Code'], how = 'left')
                SLTXN = SLTXN.join(Ct,on = ['City_Code'], how = 'left')
                SLTXN = SLTXN.join(TR,on = ['Tax_Reg_Code'],how='left')
                SLTXN = SLTXN.join(LM1,on =['Lot_Code'],how='left')
                SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
                SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
                SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')
                SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
                SLTXN = SLTXN.join(IMOD,IMD1.Item_O_Det_Code==IMOD.Code,how='left')
                SLTXN = SLTXN.join(CID,SLTXN.Comm_It_Desc_Code==CID.Code,how='left')
                SLTXN = SLTXN.join(BG,SLTXN.Group_Code1== BG.Group_Code,how='left')      
    
                SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn('SaleQty',SLTXN['Tot_Qty']/1)\
                        .withColumn('ReplQty',SLTXN['Repl_Qty']/1).withColumn('SampleQty',SLTXN['Sample_Qty']/1).withColumn('Repl_Value',  LM1['Basic_rate'] * SLTXN['Repl_Qty']).withColumn('Sample_Value',  LM1['Basic_rate'] * SLTXN['Sample_Qty'])\
                        .withColumn('Free_Value',  LM1['Basic_rate'] * SLTXN['Free_Qty']).withColumn('Rate_Per_Pack',SLTXN['Rate'] * SLTXN['CF_Qty']).withColumn('Int_Total_Packs',  SLTXN['Tot_Qty'] / SLTXN  ['CF_Qty']) \
                        .withColumn('UDR_Amount',LM1['Sale_rate'] * SLTXN['Tot_Qty'])\
                        .withColumn('UserDefNetAmt','0'+ SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] +SLTXN['calc_rdf'] +SLTXN['calc_scheme_u'] +SLTXN['calc_scheme_rs'] +SLTXN['Calc_Tax_1'] +SLTXN['Calc_Tax_2'] +SLTXN['Calc_Tax_3'] +SLTXN['calc_sur_on_tax3'] +SLTXN['calc_mfees'] +SLTXN['calc_excise_u'] +SLTXN['Calc_adjustment_u'] +SLTXN['Calc_adjust_rs'] +SLTXN['Calc_freight'] +SLTXN['calc_adjust'] +SLTXN['Calc_Spdisc'] +SLTXN['Calc_DN'] +SLTXN['Calc_CN'] +SLTXN['Calc_Display'] +SLTXN['Calc_Handling'] +SLTXN['calc_Postage'] +SLTXN['calc_Round'] +SLTXN['calc_Labour'])\
                        .withColumn('UserDefNetAmt_2','0'+ SLTXN['Calc_Gross_Amt']).withColumn('UDR_Amount_1',LM1['Sale_rate'] * SLTXN['Tot_Qty'])
            
                SLTXN = SLTXN.withColumn('MonthOrder', when(monthf(SLTXN.Vouch_Date) < 4, monthf(SLTXN.Vouch_Date) + 12).otherwise(monthf(SLTXN.Vouch_Date)))\
                                .withColumn('DayOrder', dayofmonth(SLTXN['Vouch_Date']))\
                                .withColumn('Year', year(SLTXN['Vouch_Date']))
                               
                SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 1).filter(SLTXN.Sa_Subs_Lot == 0).filter(SLTXN.Sale_Or_SR == 'SL').filter(SLTXN.Deleted == 0)             
                SLTXN = SLTXN.withColumn('YearMonth',lit(cdm))
                    
                SLTXN=SLTXN.groupby('Vouchcode','YearMonth','Act_Name','Act_Code','City_Name','City_Start_Pos',ACT1['User_Code'],'Sale_Or_SR','Tax_Reg_Name','Deleted','New_Vouch_Num','MonthOrder',\
                                  'Bill_Cust_Code','Vouch_Num', 'Vouch_Date',  'Series_Code', 'Number_', 'Net_Amt','Pay_Mode','Branch_Name', 'Branch_Code','Rate_Group_Code','pur_rate',CCI1['Commission_P'],CCI1['Sp_Commission_P'] ,CCI1['Rdf_P'])\
                         .agg(sum('UDR_Amount_1').alias('UDR_Amount_1'),avg('Sale_rate').alias('UDR_Rate'),sum('TotQty').alias('TotQty'),sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('FreeQty'),sum('SampleQty').alias('SampleQty'),sum('Free_Value').alias('Free_Value'),sum('Sample_Value').alias('Sample_Value'),sum('Repl_Value').alias('Repl_Value'),sum('UDR_Amount').alias('UDR_Amount'),sum('UserDefNetAmt_2').alias('UserDefNetAmt_2'),\
                            avg('Rate').alias('RatePerUnit'),avg('CF_Qty').alias('CF_Qty'),sum('Qty_Weight').alias("Qty_Weight"),sum('Int_Total_Packs').alias('Int_Total_Packs'),sum('Calc_Gross_Amt').alias('Gross_Value'),sum('Calc_Net_Amt').alias('Net_Sale_Value'),\
                            sum('Calc_Commission').alias('CD'),sum('Calc_Sp_Commission').alias('TD'),sum('Calc_Rdf').alias('SpCD') , sum('Calc_Scheme_U').alias('Scheme_Unit'), \
                            sum('Calc_Scheme_Rs').alias('Scheme_Rs') ,sum('Calc_tax_1').alias('Tax1') ,sum('Calc_tax_2').alias('Tax2') ,sum('Calc_Tax_3').alias('Tax3WithoutSur'), \
                            sum('Calc_Sur_On_Tax3').alias('Surcharge'), sum('Calc_Excise_U').alias('ExciseUnit') ,sum('Calc_Adjustment_u').alias('AdjustPerUnit'),\
                            sum('Calc_Adjust_RS').alias('AdjustItem'), sum('Calc_Freight').alias('Freight'), sum('Calc_Adjust').alias('AdjustmentRupees'),\
                            sum('Calc_Spdisc').alias('SPDiscount') ,sum('Calc_DN').alias('DebitNote') ,sum('Calc_cn').alias('CreditNote') ,sum('Calc_Display').alias('Display'), \
                            sum('Calc_Handling').alias('Handling'), sum('Calc_Postage').alias('Postage') ,sum('Calc_MFees').alias('ExcisePercentage'),sum('Calc_Labour').alias('Labour'),sum('calc_round').alias('Round_Amt'))
            
                SLTXN=SLTXN.orderBy(BM1['Branch_Name'] ,'Vouch_Date','Series_Code','Number_',  'Act_Name', 'Sale_Or_SR')
                    
                SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/StockTransfer_OUTSummaryI")
                delete_data(' DELETE FROM KOCKPIT.dbo.StockTransfer_OUTSummaryI WHERE YearMonth = ' + cdm)
                print("StockTransfer_OUTSummaryI Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/StockTransfer_OUTSummaryI")
        SLTXN = SLTXN.drop("YearMonth")
        write_data_sql(SLTXN,"StockTransfer_OUTSummaryI",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_OUTSummaryI','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'StockTransfer_OUTSummaryI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")  