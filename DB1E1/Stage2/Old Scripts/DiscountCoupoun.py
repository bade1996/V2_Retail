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
CDM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Discount_Coupon_Mst")
CDG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Setup_Masters")
  
   
CDM=CDM.select('Discount_Coupon_Group_Code','CD_Rs','Discount_Coupon_Prefix','Code','Sale_Vouch_Code',\
                  'Discount_Coupon_Number','Expiry_Date','CD_P','Discount_Coupon_Barcode','Sale_Fin_Year','Discount_Coupon_Num')\
            .filter(CDM.Sale_Fin_Year=='20192020')
    
    
    
CDG=CDG.select('Mst_Name', 'Code','General_Text_1')\
           .withColumnRenamed("Code","Discount_Coupon_Group_Code")
            
CCI=CCI.select('Code')
BM=BM.select('Branch_Name', 'Branch_Code')
    
LM=LM.select('Mrp','Lot_Code','Item_Det_Code')
IMH=IMH.select('Item_Hd_Code','comp_code','Color_Code','Group_Code','Item_Name_Code' )
IMD=IMD.select('Item_Det_Code','Pack_Code','Item_Hd_Code')
CM=CM.select('Comp_Code')
ICM=ICM.select('Color_Code')
GM=GM.select('Group_Code')
PM=PM.select('Pack_Code')
IMN=IMN.select('Item_Name_Code')

'''
print(datetime.now())
Start_Year = 2018

if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/DiscountCoupoun_I") !=0):
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData")
    SLTXN.cache()
    if(SLTXN.count() > 0):
        SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left' )
        SLTXN = SLTXN.join(LM,on =['Lot_Code'],how='left' )
        SLTXN = SLTXN.join(IMD,on = ['Item_Det_Code'],how='left' )
        SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
        SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
        SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
        SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')
        SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
        SLTXN = SLTXN.join(PM,on = ['Pack_Code'],how='left' )
        SLTXN = SLTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
        SLTXN = SLTXN.join(CDM,SLTXN.Vouchcode== CDM.Sale_Vouch_Code,how='left') 

        SLTXN = SLTXN.join(CDG,SLTXN.Discount_Coupon_Group_Code== CDG.Discount_Coupon_Group_Code,how='left')
    

        SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 0).filter(SLTXN.Deleted == 0).filter(SLTXN.Sa_Subs_Lot== 0)
        SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn("Max_Amount_For_Cd_P", lit('Max_Amount_For_Cd_P'))\
                .withColumn('UDAmt',  SLTXN['Rate'] * SLTXN['Tot_Qty']) .withColumn('UDAmt_1',SLTXN['Tot_Qty'] * LM['Mrp'])\
                .withColumn('UdNet_Amt_2','0' + SLTXN['Calc_adjust_rs'])
                
        SLTXN=SLTXN.groupby('Discount_Coupon_Num','YearMonth','Branch_Name', 'Branch_Code','Sale_Or_Sr','Member_Code','Vouch_Num','series_code','number_','Chq_No','Max_Amount_For_Cd_P',\
                        'Chq_Date','Vouch_Date','Mst_Name', CDG['Discount_Coupon_Group_Code'],'General_Text_1','Discount_Coupon_Prefix',CDM['Code'],'Discount_Coupon_Number','Expiry_Date','CD_P','Discount_Coupon_Barcode')\
               .agg(sum('UDAmt').alias('UDAmt'),sum('UDAmt_1').alias('UDAmt_1'),sum('TotQty').alias('TotQty'),sum('UdNet_Amt_2').alias('UdNet_Amt_2'),\
                    sum('Calc_Gross_Amt').alias('Gross_Amt'),sum('CD_Rs').alias('Discount_Coupon_Amount'))
    
        
        SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/DiscountCoupoun_I")
        print("DiscountCoupoun_I Done")
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
            os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/DiscountCoupoun_I/YearMonth="+cdm)
           
            SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left' )
            SLTXN = SLTXN.join(LM,on =['Lot_Code'],how='left' )
            SLTXN = SLTXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
            SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
            SLTXN = SLTXN.join(ICM,on = ['Color_Code'], how = 'left')
            SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
            SLTXN = SLTXN.join(PM,on = ['Pack_Code'],how='left' )
            SLTXN = SLTXN.join(IMN,on = ['Item_Name_Code'], how = 'left')
            SLTXN = SLTXN.join(CDM,SLTXN.Vouchcode== CDM.Sale_Vouch_Code,how='left') 

            SLTXN = SLTXN.join(CDG,SLTXN.Discount_Coupon_Group_Code== CDG.Discount_Coupon_Group_Code,how='left')
            SLTXN = SLTXN.withColumn('YearMonth',lit(cdm)) 

            SLTXN = SLTXN.filter(SLTXN.Stock_Trans == 0).filter(SLTXN.Deleted == 0).filter(SLTXN.Sa_Subs_Lot== 0)
            SLTXN=SLTXN.withColumn('TotQty',SLTXN['Tot_Qty']/1).withColumn("Max_Amount_For_Cd_P", lit('Max_Amount_For_Cd_P'))\
                    .withColumn('UDAmt',  SLTXN['Rate'] * SLTXN['Tot_Qty']) .withColumn('UDAmt_1',SLTXN['Tot_Qty'] * LM['Mrp'])\
                    .withColumn('UdNet_Amt_2','0' + SLTXN['Calc_adjust_rs'] )
                    
            SLTXN=SLTXN.groupby('Discount_Coupon_Num','YearMonth','Branch_Name', 'Branch_Code','Sale_Or_Sr','Member_Code','Vouch_Num','series_code','number_','Chq_No','Max_Amount_For_Cd_P',\
                            'Chq_Date','Vouch_Date','Mst_Name', CDG['Discount_Coupon_Group_Code'],'General_Text_1','Discount_Coupon_Prefix',CDM['Code'],'Discount_Coupon_Number','Expiry_Date','CD_P','Discount_Coupon_Barcode')\
                   .agg(sum('UDAmt').alias('UDAmt'),sum('UDAmt_1').alias('UDAmt_1'),sum('TotQty').alias('TotQty'),sum('UdNet_Amt_2').alias('UdNet_Amt_2'),\
                        sum('Calc_Gross_Amt').alias('Gross_Amt'),sum('CD_Rs').alias('Discount_Coupon_Amount'))
        

            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/DiscountCoupoun_I")
            print("DiscountCoupoun_I Done")
        except Exception as e:
            print(e)
    print(datetime.now())
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DiscountCoupoun_I")
    SLTXN = SLTXN.drop("YearMonth")
    write_data_sql(SLTXN,"DiscountCoupoun_I",owmode)

'''

print(datetime.now())
Start_Year = 2018

if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/DiscountCoupoun") !=0):
    S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
    S1.cache()
    if(S1.count() > 0):
        
        S1 = S1.join(CDM,S1.vouch_code==CDM.Sale_Vouch_Code,how='right' )
        
        #S1 = S1.filter(S1.Stock_Trans == 0).filter(S1.Deleted == 0).filter(S1.Sa_Subs_Lot== 0)
        #S1 = S1.withColumn('UdNet_Amt_2','0' + S1['Calc_Adjust_RS'])
               
        S1=S1.groupby('YearMonth','Lot_Code','Discount_Coupon_Num','vouch_date')\
             .agg(sum('Calc_Adjust_RS').alias('Calc_Adjust_RS'))
        S1.show(20,False)
        exit()    
        
        S1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/DiscountCoupoun")
        print("DiscountCoupoun Done")
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
            S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales/YearMonth="+cdm)
            os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/DiscountCoupoun/YearMonth="+cdm)
            
            S1 = S1.join(CDM,S1.vouch_code==CDM.Sale_Vouch_Code,how='left' )
        
            #S1 = S1.filter(S1.Stock_Trans == 0).filter(S1.Deleted == 0).filter(S1.Sa_Subs_Lot== 0)
            S1 = S1.withColumn('YearMonth',lit(cdm)) 
               
            S1=S1.groupby('YearMonth','Lot_Code','Discount_Coupon_Num','vouch_date')\
             .agg(sum('Calc_Adjust_RS').alias('Calc_Adjust_RS'))
           
            
            
            S1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/DiscountCoupoun")
            print("DiscountCoupoun Done")              
                          
        except Exception as e:
            print(e)
    print(datetime.now())
    S1.show()
    S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage2/DiscountCoupoun")
    S1 = S1.drop("YearMonth")
    write_data_sql(S1,"DiscountCoupoun",owmode)






