'''
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
import os
import pandas as pd

config = os.path.dirname(os.path.realpath(__file__))
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
    

conf = SparkConf().setMaster(smaster).setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
sqlctx = SQLContext(sc)

#hc = HiveContext(sc)
branch = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Branch")
branch.show()
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
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

config = os.path.dirname(os.path.realpath(__file__))
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("Stage1_transaction").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()


#POData         
Start_Year = 2018
if(os.system("/home/hadoopusr/hadoop/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/POdata") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        POdata = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(ph.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.Vouch_Date)),2) as YearMonth,ph.Deleted_,ph.Vouch_Num,ph.Vouch_Code as Vouchcode,ph.Goods_In_Transit,ph.Vouch_Date,ph.Bill_Amount ,ph.Net_Amt ,\
                                ph.Stock_Trans,ph.Cust_Code,ph.Agent_Code,ph.Branch_Code,ph.Bill_No,ph.Bill_Date ,ph.GRN_PreFix,ph.GRN_Number,ph.Tax_Reg_Code,pt.CF_Qty,pt.Qty_Weight,pt.Rate,pt.Tot_Qty,pt.Free_Qty,pt.Repl_Qty,pt.Sample_Qty,pt.Lot_Code,pt.Comm_Calc_Code,pt.Item_Det_Code,pt.Godown_Code,pt.\
                                Calc_Gross_Amt,pt.Calc_Net_Amt,pt.Calc_Commission,pt.Calc_Sp_Commission,pt.Calc_Rdf,pt.Calc_Scheme_U,pt.Calc_Scheme_Rs,pt.Calc_tax_1,pt.Order_Item_Code,pt.Pur_Or_PR,pt.Challan_Code,pt.Order_item_code as OrderItemCode\
                                pt.Calc_tax_2,pt.Calc_Tax_3,pt.Calc_Sur_On_Tax3,pt.Calc_Excise_U,pt.Calc_Adjustment_u,pt.Calc_Adjust_RS,pt.Calc_Freight,pt.Calc_Adjust,pt.Calc_Spdisc,pt.Calc_DN,pt.Calc_cn,pt.Calc_Display,pt.Calc_Handling,pt.Calc_Postage,pt.Calc_MFees,pt.Calc_Labour,pt.calc_round,\
                                POH.vouch_code as POHVouchcode,POH.Valid_Date AS PoValidDate,POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate,POH.Branch_Code, POH.Sup_Code,POH.Order_Type,POH.Tot_Tax AS TotalTAX,POT.Item_Det_Code,POT.Tot_Qty,POT.Rate,POT.Cancel_Item,POT.Pend_Qty,POT.code,POT.Vouch_Code as POTVouchcode,\
                                from Po_Txn AS POT LEFT JOIN Pur_Txn"+FY+" pt WITH (NOLOCK) ON (POT.code = pt.Order_Item_Code)\
                                 LEFT JOIN Pur_Head"+FY+" ph left join WITH (NOLOCK) ON (ph.Vouch_Code = pt.Vouch_Code)\
                                INNER JOIN Po_Head AS POH WITH (NOLOCK) ON (POT.vouch_code=POH.vouch_code)  as ph")
        POdata = POdata.where(col("Vouch_Date").isNotNull())
        POdata.cache()
        
        #POdata.show(1)
        if(POdata.count() > 0):
            POdata = POdata.withColumn("Vouch_Date",POdata['Vouch_Date'].cast(DateType()))
            POdata.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/POdata")
            POdata.show(1)
            print('Data done')
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
            POdata =  read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(ph.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.Vouch_Date)),2) as YearMonth,ph.Deleted_,ph.Vouch_Num,ph.Vouch_Code as Vouchcode,ph.Goods_In_Transit,ph.Vouch_Date,ph.Bill_Amount ,ph.Net_Amt ,\
                                ph.Stock_Trans,ph.Cust_Code,ph.Agent_Code,ph.Branch_Code,ph.Bill_No,ph.Bill_Date ,ph.GRN_PreFix,ph.GRN_Number,ph.Tax_Reg_Code,pt.CF_Qty,pt.Qty_Weight,pt.Rate,pt.Tot_Qty,pt.Free_Qty,pt.Repl_Qty,pt.Sample_Qty,pt.Lot_Code,pt.Comm_Calc_Code,pt.Item_Det_Code,pt.Godown_Code,pt.\
                                Calc_Gross_Amt,pt.Calc_Net_Amt,pt.Calc_Commission,pt.Calc_Sp_Commission,pt.Calc_Rdf,pt.Calc_Scheme_U,pt.Calc_Scheme_Rs,pt.Calc_tax_1,pt.Order_Item_Code,pt.Pur_Or_PR,pt.Challan_Code,pt.Order_item_code as OrderItemCode\
                                pt.Calc_tax_2,pt.Calc_Tax_3,pt.Calc_Sur_On_Tax3,pt.Calc_Excise_U,pt.Calc_Adjustment_u,pt.Calc_Adjust_RS,pt.Calc_Freight,pt.Calc_Adjust,pt.Calc_Spdisc,pt.Calc_DN,pt.Calc_cn,pt.Calc_Display,pt.Calc_Handling,pt.Calc_Postage,pt.Calc_MFees,pt.Calc_Labour,pt.calc_round,\
                                POH.vouch_code as POHVouchcode,POH.Valid_Date AS PoValidDate,POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate,POH.Branch_Code, POH.Sup_Code,POH.Order_Type,POH.Tot_Tax AS TotalTAX,POT.Item_Det_Code,POT.Tot_Qty,POT.Rate,POT.Cancel_Item,POT.Pend_Qty,POT.code,POT.Vouch_Code as POTVouchcode,\
                                from Po_Txn AS POT LEFT JOIN Pur_Txn"+FY+" pt WITH (NOLOCK) ON (POT.code = pt.Order_Item_Code)\
                                LEFT JOIN Pur_Head"+FY+" ph left join WITH (NOLOCK) ON ( ph.Vouch_Code = pt.Vouch_Code)\
                                INNER JOIN Po_Head AS POH WITH (NOLOCK) ON ( POT.vouch_code=POH.vouch_code) ) as ph")
            POdata = POdata.where(col("Vouch_Date").isNotNull())
            print('1')
            POdata.cache()
            if(POdata.count() > 0):
                cdm = str(datetime.today().year)
                if(month <= 9):
                    cdm = cdm + '0' + str(month)
                else:
                    cdm = cdm + str(month)
                os.system("/home/hadoopusr/hadoop/bin/hadoop fs -rm -r hdfs://103.248.60.14:9000/KOCKPIT/Market/Stage1/POdata/YearMonth="+cdm)
                POdata = POdata.withColumn("Vouch_Date",POdata['Vouch_Date'].cast(DateType()))
                #POdata.show(1)
                POdata.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/POdata")            
                POdata.show(1)
        except Exception as e:
            print(e)   
            



