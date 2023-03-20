from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import sum as sumf, first as firstf
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime

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

config = os.path.dirname(os.path.realpath(__file__))
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)


    
CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
BM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
LM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
IMH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
CM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comp_Mst")
IMD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det") 
ICM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
GM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
IMN= sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
PM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
GVM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Gift_Vouch_Mst")
SM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Setup_Masters")
Sl_MPM= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sl_MPM")
   
SM= SM.select('Mst_Name', 'Code','General_Text_1')
CCI=CCI.select('Code')
BM=BM.select('Branch_Name', 'Branch_Code')
LM=LM.select('Sale_rate','Lot_Code','Item_Det_Code','Basic_rate')
IMH=IMH.select('Item_Hd_Code','comp_code','Color_Code','Group_Code','Item_Name_Code' )
IMD=IMD.select('Item_Det_Code','Pack_Code','Item_Hd_Code')
CM=CM.select('Comp_Code')
ICM=ICM.select('Color_Code')
GM=GM.select('Group_Code')
PM=PM.select('Pack_Code')
IMN=IMN.select('Item_Name_Code')
GVM =GVM.select('Gift_Vouch_Num','Gift_Voucher_Group_Code','Gift_Vouch_Prefix','Gift_Vouch_Number','Code','Gift_Vouch_Barcode','Create_Date','Expiry_Date','Sale_Vouch_Code','Amount')\
            .withColumnRenamed("Code","GiftVoucherCode")

Sl_MPM=Sl_MPM.select('Vouch_Code','Gift_Vouch_Amount').withColumnRenamed("Vouch_Code","vouch_code")


'''
print(datetime.now())
Start_Year = 2018

if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/GiftVouchers_I") !=0):
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData")
    SLTXN.cache()
    if(SLTXN.count() > 0):
        SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left' )
        #SLTXN = SLTXN.join(SLHD,SLTXN.vouch_code==SLHD.Vouch_Code,how='left' )
        SLTXN = SLTXN.join(LM,on =['Lot_Code'],how='left' )
        SLTXN = SLTXN.join(IMD,on = ['Item_Det_Code'],how='left' )
        SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
        #print(SLTXN.columns)
        
        SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
        SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
        SLTXN = SLTXN.join(PM,on = ['Pack_Code'],how='left' )
        SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')
        SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
        SLTXN = SLTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
        #print(SLTXN.columns)
        #print(SLTXN.columns)
        #SLTXN.show(10,False)
        #print(GVM.columns)
        #GVM.show(10,False)
        SLTXN = SLTXN.join(GVM,SLTXN.Vouchcode== GVM.Sale_Vouch_Code,how='left') 
        
        #SLTXN.show(1)
        SLTXN = SLTXN.join(SM,SLTXN.Gift_Voucher_Group_Code== SM.Code,how='left') 
        
        
        
        #.withColumn("Max_Amount_For_Cd_P", lit('Max_Amount_For_Cd_P'))\    
        
        SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1)\
                    .withColumn('UDAmt',  LM['Basic_rate'] * SLTXN['Tot_Qty']) .withColumn('UDAmt_1',SLTXN['Tot_Qty'] * LM['Sale_rate'])\
                    .withColumn('UdNet_Amt_2','0' +  SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])
                                     
        SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 0).filter(SLTXN.Deleted == 0).filter(SLTXN['Sa_Subs_Lot'] == 0)
           
        SLTXN=SLTXN.groupby('Gift_Vouch_Num','YearMonth','Branch_Name', 'Branch_Code','Sale_Or_Sr','Member_Code','Vouchcode','Vouch_Num','series_code','number_','Chq_No',\
                            'Chq_Date','Vouch_Date','Mst_Name', SM['Code'],'General_Text_1',GVM['GiftVoucherCode'],'Amount','Expiry_Date','Gift_Vouch_Barcode','Create_Date','Gift_Vouch_Number')\
                   .agg(sum('UDAmt').alias('UDAmt'),sum('UDAmt_1').alias('UDAmt_1'),sum('TotQty').alias('TotQty'),\
                        sum('Calc_Gross_Amt').alias('Gross_Amt'),sum('UdNet_Amt_2').alias('UdNet_Amt_2'))
            
        SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/GiftVouchers_I")
        print("GiftVouchers_I Done")
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
            SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData/YearMonth="+cdm)
            os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/GiftVouchers_I/YearMonth="+cdm)
           
            SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left' )
            #SLTXN = SLTXN.join(SLHD,SLTXN.vouch_code==SLHD.Vouch_Code,how='left' )
            SLTXN = SLTXN.join(LM,on =['Lot_Code'],how='left' )
            SLTXN = SLTXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
            #print(SLTXN.columns)
            
            SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
            SLTXN = SLTXN.join(PM,on = ['Pack_Code'],how='left' )
            SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')
            SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
            SLTXN = SLTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            #print(SLTXN.columns)
            #print(SLTXN.columns)
            #SLTXN.show(10,False)
            #print(GVM.columns)
            #GVM.show(10,False)
            SLTXN = SLTXN.join(GVM,SLTXN.Vouchcode== GVM.Sale_Vouch_Code,how='left') 
            
            SLTXN = SLTXN.join(SM,SLTXN.Gift_Voucher_Group_Code== SM.Code,how='left') 
            SLTXN = SLTXN.withColumn('YearMonth',lit(cdm))
                        
            SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1)\
                        .withColumn('UDAmt',  LM['Basic_rate'] * SLTXN['Tot_Qty']) .withColumn('UDAmt_1',SLTXN['Tot_Qty'] * LM['Sale_rate'])\
                        .withColumn('UdNet_Amt_2','0' +  SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])
                                         
            SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 0).filter(SLTXN.Deleted == 0).filter(SLTXN['Sa_Subs_Lot'] == 0)
               
            SLTXN=SLTXN.groupby('YearMonth','Branch_Name', 'Branch_Code','Sale_Or_Sr','Member_Code','Vouchcode','Vouch_Num','series_code','number_','Chq_No',\
                                'Chq_Date','Vouch_Date','Mst_Name', SM['Code'],'General_Text_1',GVM['GiftVoucherCode'],'Amount','Expiry_Date','Gift_Vouch_Barcode','Create_Date','Gift_Vouch_Number')\
                       .agg(sum('UDAmt').alias('UDAmt'),sum('UDAmt_1').alias('UDAmt_1'),sum('TotQty').alias('TotQty'),\
                            sum('Calc_Gross_Amt').alias('Gross_Amt'),sum('UdNet_Amt_2').alias('UdNet_Amt_2'))

            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/GiftVouchers_I")
            print("GiftVouchers_I Done")
        except Exception as e:
            print(e)
    print(datetime.now())
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/GiftVouchers_I")
    SLTXN = SLTXN.drop("YearMonth")
    SLTXN.show(1)
    write_data_sql(SLTXN,"GiftVouchers_I",owmode)

'''

print(datetime.now())
Start_Year = 2018

if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/GiftVoucher") !=0):
    S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData")
    S1.cache()
    if(S1.count() > 0):
        
        S1 = S1.join(Sl_MPM,on = ['vouch_code'],how='left')
        S1 = S1.join(GVM,S1.vouch_code==GVM.Sale_Vouch_Code,how='left' )
        
        #S1 = S1.filter(S1.Stock_Trans == 0).filter(S1.Deleted == 0).filter(S1.Sa_Subs_Lot== 0)
       
               
        S1=S1.groupby('YearMonth','Item_Det_Code','Gift_Vouch_Num','lot_code','vouch_date')\
             .agg(sum('Gift_Vouch_Amount').alias('Gift_Vouch_Amount'))
           
        S1.show(20,False)
        exit()    
        
        S1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/GiftVoucher")
        print("GiftVoucher Done")
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
            S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData/YearMonth="+cdm)
            os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/GiftVoucher/YearMonth="+cdm)
            
            S1 = S1.join(Sl_MPM,on = ['vouch_code'],how='left')
            S1 = S1.join(GVM,S1.vouch_code==GVM.Sale_Vouch_Code,how='left' )
            
            #S1 = S1.filter(S1.Stock_Trans == 0).filter(S1.Deleted == 0).filter(S1.Sa_Subs_Lot== 0)
            S1 = S1.withColumn('YearMonth',lit(cdm)) 
                   
            S1=S1.groupby('YearMonth','Item_Det_Code','Gift_Vouch_Num','lot_code','vouch_date')\
                 .agg(sum('Gift_Vouch_Amount').alias('Gift_Vouch_Amount'))
        
            
            S1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/GiftVoucher")
            print("GiftVoucher Done")              
                          
        except Exception as e:
            print(e)
    print(datetime.now())
    S1.show()
    S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage2/GiftVoucher")
    S1 = S1.drop("YearMonth")
    write_data_sql(S1,"GiftVoucher",owmode)










