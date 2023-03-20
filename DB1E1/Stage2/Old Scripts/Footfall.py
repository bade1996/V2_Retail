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


LM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
CM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comp_Mst")
IMH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
GM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
BM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
IMD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")
RF = sqlctx.read.parquet(hdfspath+"/Market/Stage1/RetailFootfall")
    
BM=BM.select('Branch_Name', 'Branch_Code','Branch_Short_Name')
LM=LM.select('Lot_Code','Item_Det_Code')
IMH=IMH.select('Item_Hd_Code','comp_code','Group_Code')
IMD=IMD.select('Item_Det_Code','Item_Hd_Code')
CM=CM.select('Comp_Code')
GM=GM.select('Group_Code','Group_Name')
RF=RF.select('Code','Branch_Code','Visit_Time')
#S1 =S1.withColumnRenamed("Branch_Code",'BranchCode')

'''
print(datetime.now())
Start_Year = 2018

if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Footfall_I") !=0):
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SalesData")
    SLTXN.cache()
    if(SLTXN.count() > 0):
        
        SLTXN = SLTXN.join(LM,on =['Lot_Code'],how='left' )
        SLTXN = SLTXN.join(IMD,on = ['Item_Det_Code'],how='left' )
        SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
        SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
        SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
        SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
        SLTXN = SLTXN.join(S1,SLTXN.Vouchcode==S1.Code,how='left')
        
        SLTXN=SLTXN.filter(SLTXN.Stock_Trans == 0)\
               .filter(SLTXN.Deleted== 0)
               
        
               
        SLTXN=SLTXN.withColumn('TotQty',when((SLTXN['Sale_Or_Sr'] == 'SR'),(SLTXN['Tot_Qty']) * -1).otherwise(SLTXN['Tot_Qty'])/1)\
                .withColumn('Gross_Amt',when((SLTXN['Sale_Or_Sr'] == 'SR'),(SLTXN['Calc_Gross_Amt']) * -1).otherwise(SLTXN['Calc_Gross_Amt']))\
                .withColumn('CD',when((SLTXN['Sale_Or_Sr'] == 'SR'),(SLTXN['Calc_Commission']) * -1).otherwise(SLTXN['Calc_Commission']))\
                .withColumn('UD_Net_Amt',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])*-1)\
                .withColumn('UD_Net_Amt_2',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0' + SLTXN['Calc_adjust_rs']+ SLTXN['calc_adjust'])).otherwise('0' + SLTXN['Calc_adjust_rs'] + SLTXN['calc_adjust'])*  -1)   
                
        #SLTXN.groupBy('Branch_Code','Visit_Time').count().select('Branch_Code', f.col('count').alias('n'))
        #SLTXN=SLTXN.groupBy("Branch_Code","Visit_Time").agg()
        
        
        SLTXN=SLTXN.groupby('YearMonth','Vouch_Date','Branch_Code','Branch_Name','Branch_Short_Name','Group_Name','Group_Code')\
                   .agg(sum('Gross_Amt').alias('Gross_Amt'),sum('CD').alias('CD'),sum('TotQty').alias('TotQty'),\
                        sum('UD_Net_Amt').alias('UD_Net_Amt'),sum('UD_Net_Amt_2').alias('UD_Net_Amt_2'),\
                        count(when(('BranchCode'), ('Visit_Time'))).alias("TotalFootfall"))
                   
        SLTXN.show(1)
        exit()    
        
        SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Footfall_I")
        print("Footfall_I Done")
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
            os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Footfall_I/YearMonth="+cdm)
            
            SLTXN = SLTXN.join(LM,on =['Lot_Code'],how='left' )
            SLTXN = SLTXN.join(IMD,on = ['Item_Det_Code'],how='left' )
            SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
            SLTXN = SLTXN.join(IMH,on = ['Item_Hd_Code'], how = 'left')
            SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
            SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
            SLTXN = SLTXN.join(S1,SLTXN.Vouchcode==S1.Code,how='left')
            
            SLTXN=SLTXN.filter(SLTXN.Stock_Trans == 0)\
                   .filter(SLTXN.Deleted== 0)
            SLTXN = SLTXN.withColumn('YearMonth',lit(cdm))     
                   
            SLTXN=SLTXN.withColumn('TotQty',when((SLTXN['Sale_Or_Sr'] == 'SR'),(SLTXN['Tot_Qty']) * -1).otherwise(SLTXN['Tot_Qty'])/1)\
                    .withColumn('Gross_Amt',when((SLTXN['Sale_Or_Sr'] == 'SR'),(SLTXN['Calc_Gross_Amt']) * -1).otherwise(SLTXN['Calc_Gross_Amt']))\
                    .withColumn('CD',when((SLTXN['Sale_Or_Sr'] == 'SR'),(SLTXN['Calc_Commission']) * -1).otherwise(SLTXN['Calc_Commission']))\
                    .withColumn('UD_Net_Amt',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])*-1)\
                    .withColumn('UD_Net_Amt_2',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0' + SLTXN['Calc_adjust_rs']+ SLTXN['calc_adjust'])).otherwise('0' + SLTXN['Calc_adjust_rs'] + SLTXN['calc_adjust'])*  -1)   
            
            SLTXN=SLTXN.groupBy("Branch_Code","Visit_Time").agg(count().alias("TotalFootfall"))
           
            SLTXN=SLTXN.groupby('YearMonth','Vouch_Date','Branch_Code','Branch_Name','Branch_Short_Name','Group_Name','Group_Code')\
                       .agg(sum('Gross_Amt').alias('Gross_Amt'),sum('CD').alias('CD'),sum('TotQty').alias('TotQty'),\
                            sum('UD_Net_Amt').alias('UD_Net_Amt'),sum('UD_Net_Amt_2').alias('UD_Net_Amt_2'))
                
                
            SLTXN.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Footfall_I")
            print("Footfall_I Done")
        except Exception as e:
            print(e)
    print(datetime.now())
    SLTXN.show()
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Footfall_I")
    SLTXN = SLTXN.drop("YearMonth")
    write_data_sql(SLTXN,"Footfall_I",owmode)

'''

if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Footfall") !=0):
    S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
    S1.cache()
    if(S1.count() > 0):
        
        S1 = S1.join(RF,on =['Branch_Code'],how='left' )
        S1 = S1.withColumn('VisitHour',hour('Visit_Time'))
        
        S1 = S1.withColumn('TimeInterval',when((S1['VisitHour'] >= 1) & (S1['VisitHour'] < 2),'1-2').
                                when((S1['VisitHour'] >= 2) & (S1['VisitHour'] < 3),'2-3').
                                when((S1['VisitHour'] >= 3) & (S1['VisitHour'] < 4),'3-4').
                                when((S1['VisitHour'] >= 4) & (S1['VisitHour'] < 5),'4-5').
                                when((S1['VisitHour'] >= 5) & (S1['VisitHour'] < 6),'5-6').
                                when((S1['VisitHour'] >= 6) & (S1['VisitHour'] < 7),'6-7').
                                when((S1['VisitHour'] >= 7) & (S1['VisitHour'] < 8),'7-8').
                                when((S1['VisitHour'] >= 8) & (S1['VisitHour'] < 9),'8-9').
                                when((S1['VisitHour'] >= 9) & (S1['VisitHour'] < 10),'9-10').
                                when((S1['VisitHour'] >= 10) & (S1['VisitHour'] < 11),'10-11').
                                when((S1['VisitHour'] >= 11) & (S1['VisitHour'] < 12),'11-12').
                                when((S1['VisitHour'] >= 12) & (S1['VisitHour'] < 13),'12-13').
                                when((S1['VisitHour'] >= 13) & (S1['VisitHour'] < 14),'13-14').
                                when((S1['VisitHour'] >= 14) & (S1['VisitHour'] < 15),'14-15').
                                when((S1['VisitHour'] >= 15) & (S1['VisitHour'] < 16),'15-16').
                                when((S1['VisitHour'] >= 16) & (S1['VisitHour'] < 17),'16-17').
                                when((S1['VisitHour'] >= 17) & (S1['VisitHour'] < 18),'17-18').
                                when((S1['VisitHour'] >= 18) & (S1['VisitHour'] < 19),'18-19').
                                when((S1['VisitHour'] >= 19) & (S1['VisitHour'] < 20),'19-20').
                                when((S1['VisitHour'] >= 21) & (S1['VisitHour'] < 22),'21-22').
                                when((S1['VisitHour'] >= 22) & (S1['VisitHour'] < 23),'22-23').
                                when((S1['VisitHour'] >= 23) & (S1['VisitHour'] < 24),'23-24').otherwise('0-1'))
        
        S1 =S1.groupby('YearMonth','Branch_Code','lot_code','TimeInterval','vouch_date')\
                        .agg(count('Code').alias('Code'))
                        
        
        S1.show(10,False)   
        exit()
        S1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Footfall")
        print("Footfall Done")
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
            os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Footfall/YearMonth="+cdm)
            
            S1 = S1.join(RF,on =['Branch_Code'],how='left' )
            S1 = S1.withColumn('VisitHour',hour('Visit_Time'))
            
            S1 = S1.withColumn('TimeInterval',when((S1['VisitHour'] >= 1) & (S1['VisitHour'] < 2),'1-2').
                                    when((S1['VisitHour'] >= 2) & (S1['VisitHour'] < 3),'2-3').
                                    when((S1['VisitHour'] >= 3) & (S1['VisitHour'] < 4),'3-4').
                                    when((S1['VisitHour'] >= 4) & (S1['VisitHour'] < 5),'4-5').
                                    when((S1['VisitHour'] >= 5) & (S1['VisitHour'] < 6),'5-6').
                                    when((S1['VisitHour'] >= 6) & (S1['VisitHour'] < 7),'6-7').
                                    when((S1['VisitHour'] >= 7) & (S1['VisitHour'] < 8),'7-8').
                                    when((S1['VisitHour'] >= 8) & (S1['VisitHour'] < 9),'8-9').
                                    when((S1['VisitHour'] >= 9) & (S1['VisitHour'] < 10),'9-10').
                                    when((S1['VisitHour'] >= 10) & (S1['VisitHour'] < 11),'10-11').
                                    when((S1['VisitHour'] >= 11) & (S1['VisitHour'] < 12),'11-12').
                                    when((S1['VisitHour'] >= 12) & (S1['VisitHour'] < 13),'12-13').
                                    when((S1['VisitHour'] >= 13) & (S1['VisitHour'] < 14),'13-14').
                                    when((S1['VisitHour'] >= 14) & (S1['VisitHour'] < 15),'14-15').
                                    when((S1['VisitHour'] >= 15) & (S1['VisitHour'] < 16),'15-16').
                                    when((S1['VisitHour'] >= 16) & (S1['VisitHour'] < 17),'16-17').
                                    when((S1['VisitHour'] >= 17) & (S1['VisitHour'] < 18),'17-18').
                                    when((S1['VisitHour'] >= 18) & (S1['VisitHour'] < 19),'18-19').
                                    when((S1['VisitHour'] >= 19) & (S1['VisitHour'] < 20),'19-20').
                                    when((S1['VisitHour'] >= 21) & (S1['VisitHour'] < 22),'21-22').
                                    when((S1['VisitHour'] >= 22) & (S1['VisitHour'] < 23),'22-23').
                                    when((S1['VisitHour'] >= 23) & (S1['VisitHour'] < 24),'23-24').otherwise('0-1'))
            
            S1 =S1.groupby('YearMonth','Branch_Code','lot_code','TimeInterval','vouch_date')\
                            .agg(count('Code').alias('Code'))
                            
            
                
            S1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Footfall")
            print("Footfall Done")              
                          
        except Exception as e:
            print(e)
    print(datetime.now())
    S1.show()
    S1 = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Footfall")
    S1 = S1.drop("YearMonth")
    write_data_sql(S1,"Footfall",owmode)






