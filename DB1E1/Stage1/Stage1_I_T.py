from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf 
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
# print(config.head(100))

for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("Stage1_transaction")\
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .set("spark.executor.cores","4")\
                .set("spark.executor.memory","5g")\
                .set("spark.driver.maxResultSize","0")\
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
spark = SparkSession.builder.getOrCreate()
sqlctx = SQLContext(sc)
print(datetime.now())

#Sales
Start_Year = 2018
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/Sales") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY)
        sales= read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth, st.Code,st.cf_qty,st.Qty_Weight,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,st.Calc_Gross_Amt,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,\
                         st.Calc_Commission,st.Calc_Sp_Commission,st.Calc_Rdf,st.Calc_Scheme_U,st.Calc_Scheme_Rs,st.Calc_MFees,st.Calc_Labour,st.calc_round,\
                         st.Calc_tax_2,st.Calc_Tax_3,st.Calc_Sur_On_Tax3,st.Calc_Excise_U,st.Calc_Adjustment_u,st.Calc_Freight,st.Calc_Adjust,st.used_cf_rate,st.Godown_Code,st.Comm_Calc_Code,st.Sa_Subs_Lot,st.Deleted,sh.Bill_Cust_Code,sh.Vouch_Num,sh.Series_Code,sh.Number_,sh.Net_Amt ,sh.New_Vouch_Num ,sh.Tax_Reg_Code,\
                         sh.Agent_Code,sh.Pay_Mode,sh.Act_Code_For_Txn_X,sh.Chq_No,sh.Member_Code,sh.Chq_Date,sh.Vouch_Time,sh.Store_Code,st.Calc_Adjust_RS,st.Challan_Code,sh.Cashier_Code,st.Calc_Net_Amt as NET_SALE_VALUE, st.Sale_Or_SR as Type, sh.vouch_date, sh.Branch_Code, st.item_det_code, st.lot_code, st.Tot_Qty,st.rate,\
                        (st.calc_tax_1) as Total_GST_Amount_Paid,sh.Stock_Trans,(st.calc_commission + st.calc_labour + st.calc_rdf + st.calc_mfees + st.calc_sp_commission + st.Calc_Scheme_U + st.Calc_Scheme_Rs) as Discount_Amount,sh.vouch_code,st.PS_Txn_Code,\
                         sh.cust_code from sl_head"+FY+" sh left join sl_txn"+FY+" st on sh.vouch_code = st.vouch_code ) as sh")
        
        sales = sales.where(col("vouch_date").isNotNull())
        print(sales.count())
        #sales.show(10)
        #exit
        sales.cache()
        if(sales.count() > 0):
            sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
            sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Sales")
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
        print(FY)
        cdm = str(datetime.today().year)
        if(month == 0):
            month = 12
            cdm = str(datetime.today().year - 1)
        if(month == -1):
            month = 11
            cdm = str(datetime.today().year - 1)
        
        if(month <= 9):
            cdm = cdm + '0' + str(month)
            # print("111111111111111111111111111111111111111111111111111111111111")
        else:
            cdm = cdm + str(month)
            # print("222222222222222222222222222222222222222222222222222")
        print("#"*100)
        print(cdm)
        print("jai")
        try:
            sales= read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth, st.Code,st.cf_qty,st.Qty_Weight,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,st.Calc_Gross_Amt,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,\
                         st.Calc_Commission,st.Calc_Sp_Commission,st.Calc_Rdf,st.Calc_Scheme_U,st.Calc_Scheme_Rs,st.Calc_MFees,st.Calc_Labour,st.calc_round,\
                         st.Calc_tax_2,st.Calc_Tax_3,st.Calc_Sur_On_Tax3,st.Calc_Excise_U,st.Calc_Adjustment_u,st.Calc_Freight,st.Calc_Adjust,st.used_cf_rate,st.Godown_Code,st.Comm_Calc_Code,st.Sa_Subs_Lot,st.Deleted,sh.Bill_Cust_Code,sh.Vouch_Num,sh.Series_Code,sh.Number_,sh.Net_Amt ,sh.New_Vouch_Num ,sh.Tax_Reg_Code,\
                         sh.Agent_Code,sh.Pay_Mode,sh.Act_Code_For_Txn_X,sh.Chq_No,sh.Member_Code,sh.Chq_Date,sh.Vouch_Time,sh.Store_Code,st.Calc_Adjust_RS,st.Challan_Code,sh.Cashier_Code,st.Calc_Net_Amt as NET_SALE_VALUE, st.Sale_Or_SR as Type, sh.vouch_date, sh.Branch_Code, st.item_det_code, st.lot_code, st.Tot_Qty,st.rate, \
                        (st.calc_tax_1) as Total_GST_Amount_Paid,sh.Stock_Trans,(st.calc_commission + st.calc_labour + st.calc_rdf + st.calc_mfees + st.calc_sp_commission + st.Calc_Scheme_U + st.Calc_Scheme_Rs) as Discount_Amount,sh.vouch_code,st.PS_Txn_Code,\
                        sh.cust_code from sl_head"+FY+" sh left join sl_txn"+FY+" st on sh.vouch_code = st.vouch_code   where month(sh.vouch_date) = "+str(month)+") as sh")
        
            sales = sales.where(col("vouch_date").isNotNull())
            sales.cache()
            if(sales.count() > 0):
                # cdm = str(datetime.today().year)
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                # else:
                    # cdm = cdm + str(month)
                # #hdfs://103.248.60.14:9000
                # print("#"*100)
                # print(cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/Sales/YearMonth="+cdm)
                sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
                sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Sales")
        except Exception as e:
            print(e)
print(datetime.now())



#Purchase
Start_Year = 2018
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/Purchase") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY)
        purchase = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(ph.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.vouch_date)),2) as YearMonth,ph.Deleted_,ph.vouch_date, ph.vouch_num, ph.vouch_code, ph.Bill_No,ph.Goods_In_Transit,ph.Bill_Amount ,ph.Net_Amt ,\
                                    ph.Stock_Trans,ph.Cust_Code,ph.Agent_Code,ph.Branch_Code,ph.Bill_Date ,ph.GRN_PreFix,ph.GRN_Number,ph.Tax_Reg_Code,ptxn.CF_Qty,ptxn.Qty_Weight,ptxn.Rate,ptxn.Tot_Qty,ptxn.Free_Qty,ptxn.Repl_Qty,ptxn.Sample_Qty,ptxn.Comm_Calc_Code,ptxn.Godown_Code,ptxn.\
                                    Calc_Gross_Amt,ptxn.Calc_Net_Amt,ptxn.Calc_Commission,ptxn.Calc_Sp_Commission,ptxn.Calc_Rdf,ptxn.Calc_Scheme_U,ptxn.Calc_Scheme_Rs,ptxn.Calc_tax_1,ptxn.Order_Item_Code,\
                                    ptxn.Calc_tax_2,ptxn.Calc_Tax_3,ptxn.Calc_Sur_On_Tax3,ptxn.Calc_Excise_U,ptxn.Calc_Adjustment_u,ptxn.Calc_Adjust_RS,ptxn.Calc_Freight,ptxn.Calc_Adjust,ptxn.Lot_Sch_Code,ptxn.Carton_Code,ptxn.Calc_Sale_Amt,ptxn.Challan_Code,\
                                    ptxn.Calc_Spdisc,ptxn.Calc_DN,ptxn.Calc_cn,ptxn.Calc_Display,ptxn.Calc_Handling,ptxn.Calc_Postage,ptxn.Calc_MFees,ptxn.Calc_Labour,ptxn.calc_round,\
                                    ptxn.Code,ptxn.Pur_Or_PR,ptxn.item_det_code, ptxn.lot_code, ptxn.Txn_Deleted_ FROM LOGICDBS99.dbo.Pur_Head"+FY+" ph left join LOGICDBS99.dbo.pur_txn"+FY+" ptxn on ph.vouch_code = ptxn.vouch_code ) as pur")
        
        purchase = purchase.where(col("vouch_date").isNotNull())
        purchase.cache()
        if(purchase.count() > 0):
            purchase = purchase.withColumn("vouch_date",purchase['vouch_date'].cast(DateType()))
            purchase.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Purchase")
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
        print(FY)
        cdm = str(datetime.today().year)
        if(month == 0):
            month = 12
            cdm = str(datetime.today().year - 1)
        if(month == -1):
            month = 11
            cdm = str(datetime.today().year - 1)
        
        if(month <= 9):
            cdm = cdm + '0' + str(month)
            # print("111111111111111111111111111111111111111111111111111111111111")
        else:
            cdm = cdm + str(month)
            # print("222222222222222222222222222222222222222222222222222")
        print("#"*100)
        print(cdm)
        print("jai")
        try:
            purchase = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(ph.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.vouch_date)),2) as YearMonth,ph.Deleted_,ph.vouch_date, ph.vouch_num, ph.vouch_code, ph.Bill_No,ph.Goods_In_Transit,ph.Bill_Amount ,ph.Net_Amt ,\
                                    ph.Stock_Trans,ph.Cust_Code,ph.Agent_Code,ph.Branch_Code,ph.Bill_Date ,ph.GRN_PreFix,ph.GRN_Number,ph.Tax_Reg_Code,ptxn.CF_Qty,ptxn.Qty_Weight,ptxn.Rate,ptxn.Tot_Qty,ptxn.Free_Qty,ptxn.Repl_Qty,ptxn.Sample_Qty,ptxn.Comm_Calc_Code,ptxn.Godown_Code,ptxn.\
                                    Calc_Gross_Amt,ptxn.Calc_Net_Amt,ptxn.Calc_Commission,ptxn.Calc_Sp_Commission,ptxn.Calc_Rdf,ptxn.Calc_Scheme_U,ptxn.Calc_Scheme_Rs,ptxn.Calc_tax_1,ptxn.Order_Item_Code,\
                                    ptxn.Calc_tax_2,ptxn.Calc_Tax_3,ptxn.Calc_Sur_On_Tax3,ptxn.Calc_Excise_U,ptxn.Calc_Adjustment_u,ptxn.Calc_Adjust_RS,ptxn.Calc_Freight,ptxn.Calc_Adjust,ptxn.Lot_Sch_Code,ptxn.Carton_Code,ptxn.Calc_Sale_Amt,ptxn.Challan_Code,\
                                    ptxn.Calc_Spdisc,ptxn.Calc_DN,ptxn.Calc_cn,ptxn.Calc_Display,ptxn.Calc_Handling,ptxn.Calc_Postage,ptxn.Calc_MFees,ptxn.Calc_Labour,ptxn.calc_round,\
                                    ptxn.Code,ptxn.Pur_Or_PR,ptxn.item_det_code, ptxn.lot_code, ptxn.Txn_Deleted_ FROM LOGICDBS99.dbo.Pur_Head"+FY+" ph left join LOGICDBS99.dbo.pur_txn"+FY+" ptxn on ph.vouch_code = ptxn.vouch_code  where month(ph.vouch_date) = "+str(month)+") as pur")
        
            purchase = purchase.where(col("vouch_date").isNotNull())
            purchase.cache()
            if(purchase.count() > 0):
                # cdm = str(datetime.today().year)
                # if(month == 0):
                    # month = 12
                    # cdm = str(datetime.today().year - 1)
                # if(month == -1):
                    # month = 11
                    # cdm = str(datetime.today().year - 1)
                    #
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                    # print("111111111111111111111111111111111111111111111111111111111111")
                # else:
                    # cdm = cdm + str(month)
                    # print("222222222222222222222222222222222222222222222222222")
                # print("#"*100)
                # print(cdm)
                #hdfs://103.248.60.14:9000
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/Purchase/YearMonth="+cdm)
                purchase = purchase.withColumn("vouch_date",purchase['vouch_date'].cast(DateType()))
                purchase.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Purchase")
        except Exception as e:
            print(e)
print(datetime.now())


#Stock
Start_Year = 2018
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/Stock") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY)
        # print("*"*50)
        if(i == Start_Year):
            stock = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(date_))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(date_)),2) as YearMonth, date_,Branch_Code,lot_code,net_qty,Godown_Code FROM stk_dtxn"+FY+") as stock")
        else:
            stock = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(date_))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(date_)),2) as YearMonth, date_,Branch_Code,lot_code,net_qty,Godown_Code FROM stk_dtxn"+FY+" where date_ !='"+ str(i)+"-03-31') as stock")
        stock = stock.where(col("date_").isNotNull())
        stock.cache()
        if(stock.count() > 0):
            stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
            stock.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Stock")
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
        cdm = str(datetime.today().year)
        if(month == 0):
            month = 12
            cdm = str(datetime.today().year - 1)
        if(month == -1):
            month = 11
            cdm = str(datetime.today().year - 1)
        
        if(month <= 9):
            cdm = cdm + '0' + str(month)
            # print("111111111111111111111111111111111111111111111111111111111111")
        else:
            cdm = cdm + str(month)
            # print("222222222222222222222222222222222222222222222222222")
        print("#"*100)
        print(cdm)
        print(FY)
        try:
            stock = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(date_))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(date_)),2) as YearMonth, date_,Branch_Code,lot_code,net_qty,Godown_Code FROM stk_dtxn"+FY+" where date_ !='"+ str(Start_Year)+"-03-31' and month(date_) = "+str(month)+") as stock")
            stock = stock.where(col("date_").isNotNull())
            stock.cache()
            if(stock.count() > 0):
                # cdm = str(datetime.today().year)
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                # else:
                    # cdm = cdm + str(month)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/Stock/YearMonth="+cdm)
                stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
                print(cdm)
                stock.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Stock")
        except Exception as e:
            print(e)
        

#Challan
Start_Year = 2018
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/Challan") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY) 
        po = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(ch.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ch.vouch_date)),2) as YearMonth,ch.Cust_Code,ch.vouch_date,ct.code,ct.item_det_code,ct.lot_code,ch.Branch_Code,ct.Org_Qty from Chal_Head ch left join Chal_Txn ct on ch.vouch_code = ct.vouch_code where ch.vouch_date between '"+str(i)+"-04-01' and '"+str(i+1)+"-03-31' and ct.Deleted = 0) as pur")
        po = po.where(col("Vouch_Date").isNotNull())
        po.cache()
        if(po.count() > 0):
            po = po.withColumn("vouch_date",po['vouch_date'].cast(DateType()))
            po.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Challan")
else:
    for i in range(2,-1,-1):
        month = datetime.today().month - i
        if(month <= 3):
            Start_Year = (datetime.today().year) - 1
            End_Year = (datetime.today().year)
        else:
            Start_Year = (datetime.today().year) 
            End_Year = int(datetime.today().year) + 1
        FY = str(Start_Year)+str(End_Year)
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
        if(month == 1):
            cdm_back = str(int(cdm) - 89)
        else:
            cdm_back = str(int(cdm) - 1)
        yr = cdm[:4]
        mn = cdm[4:]
        try:
            po = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(ch.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ch.vouch_date)),2) as YearMonth,ch.vouch_date,ch.Cust_Code,ct.code,ct.item_det_code,ct.lot_code,ch.Branch_Code,ct.Org_Qty from Chal_Head ch left join Chal_Txn ct on ch.vouch_code = ct.vouch_code where YEAR(ch.vouch_Date) = '"+str(yr)+"' and MONTH(ch.vouch_Date) ='"+str(mn)+"' and ct.Deleted = 0) as pur")
            po = po.where(col("vouch_date").isNotNull())
            po.cache()
            if(po.count() > 0):
                # cdm = str(datetime.today().year)
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                # else:
                    # cdm = cdm + str(month)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/Challan/YearMonth="+cdm)
                po = po.withColumn("vouch_date",po['vouch_date'].cast(DateType()))
                po.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/Challan")
        except Exception as e:
            print(e)




#PurchaseOrder
Start_Year = 2018
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/PurchaseOrder") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY)
        po = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(PH.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(PH.Vouch_Date)),2) as YearMonth,PH.Vouch_Code ,PH.Vouch_Num ,PH.Vouch_Date ,PH.Sup_Code ,PT.Tax,PT.Extra_Percent_1,PH.Tot_Extra_Percent_1,PH.Cancel_Order,PH.Order_Date,PT.Cancel_Item,PH.Valid_Date,PH.Branch_Code,PH.Tot_Tax,PH.Order_Type ,\
                                PH.Order_No,PT.Rate, PT.Code,PT.Pend_Qty ,PT.Tot_Qty ,PT.Item_Det_Code,PT.PO_MRP from PO_Head PH left join PO_Txn PT on PH.Vouch_Code = PT.Vouch_Code where PH.Vouch_Date between '"+str(i)+"-04-01' and '"+str(i+1)+"-03-31') as pur")
        po = po.where(col("vouch_date").isNotNull())
        po.cache()
        if(po.count() > 0):
            po = po.withColumn("Vouch_Date",po['Vouch_Date'].cast(DateType()))
            po.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PurchaseOrder")
else:
    for i in range(2,-1,-1):
        month = datetime.today().month - i
        if(month <= 3):
            Start_Year = (datetime.today().year) - 1
            End_Year = (datetime.today().year)
        else:
            Start_Year = (datetime.today().year) 
            End_Year = int(datetime.today().year) + 1
        FY = str(Start_Year)+str(End_Year)
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
        if(month == 1):
            cdm_back = str(int(cdm) - 89)
        else:
            cdm_back = str(int(cdm) - 1)
        yr = cdm[:4]
        mn = cdm[4:]
        try:
            po = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(PH.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(PH.Vouch_Date)),2) as YearMonth,PH.Vouch_Code ,PH.Vouch_Num ,PH.Vouch_Date ,PH.Sup_Code,PH.Cancel_Order ,PH.Order_Date,PT.Cancel_Item,PH.Valid_Date,PH.Branch_Code,PH.Tot_Tax,PH.Order_Type ,\
                                PH.Order_No,PT.Rate,PT.Tax,PT.Extra_Percent_1,PH.Tot_Extra_Percent_1,PT.Code,PT.Pend_Qty ,PT.Tot_Qty ,PT.Item_Det_Code,PT.PO_MRP from PO_Head PH left join PO_Txn PT on PH.Vouch_Code = PT.Vouch_Code where YEAR(PH.Vouch_Date) = '"+str(yr)+"' and MONTH(PH.Vouch_Date) ='"+str(mn)+"') as pur")
            po = po.where(col("vouch_date").isNotNull())
            po.cache()
            if(po.count() > 0):
                # cdm = str(datetime.today().year)
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                # else:
                    # cdm = cdm + str(month)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/PurchaseOrder/YearMonth="+cdm)
                po = po.withColumn("vouch_date",po['vouch_date'].cast(DateType()))
                po.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PurchaseOrder")
        except Exception as e:
            print(e)




#DO
Start_Year = 2018
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/DO") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY) 
        po = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(dh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(dh.Vouch_Date)),2) as YearMonth,dh.Vouch_Code ,dh.Vouch_Date ,dh.Godown_Code ,dh.Cust_Code ,dh.Branch_Code, dt.Code,dt.Lot_Code,dt.Quantity,dt.Link_Code from DO_Head dh left join dbo.DO_Txn dt on dh.Vouch_Code = dt.Vouch_Code where dh.Vouch_Date between '"+str(i)+"-04-01' and '"+str(i+1)+"-03-31' and dt.Deleted_ = 0) as pur")
        po = po.where(col("Vouch_Date").isNotNull())
        po.cache()
        if(po.count() > 0):
            po = po.withColumn("vouch_date",po['vouch_date'].cast(DateType()))
            po.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/DO")
else:
    for i in range(2,-1,-1):
        month = datetime.today().month - i
        if(month <= 3):
            Start_Year = (datetime.today().year) - 1
            End_Year = (datetime.today().year)
        else:
            Start_Year = (datetime.today().year) 
            End_Year = int(datetime.today().year) + 1
        FY = str(Start_Year)+str(End_Year)
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
        if(month == 1):
            cdm_back = str(int(cdm) - 89)
        else:
            cdm_back = str(int(cdm) - 1)
        yr = cdm[:4]
        mn = cdm[4:]
        try:
            po = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(dh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(dh.Vouch_Date)),2) as YearMonth,dh.Vouch_Code ,dh.Vouch_Date ,dh.Godown_Code ,dh.Cust_Code ,dh.Branch_Code, dt.Code,dt.Lot_Code,dt.Quantity,dt.Link_Code from DO_Head dh left join dbo.DO_Txn dt on dh.Vouch_Code = dt.Vouch_Code where YEAR(dh.Vouch_Date) = '"+str(yr)+"' and MONTH(dh.Vouch_Date) ='"+str(mn)+"' and dt.Deleted_ = 0) as pur")
            po = po.where(col("vouch_date").isNotNull())
            po.cache()
            if(po.count() > 0):
                # cdm = str(datetime.today().year)
                # if(month <= 9):
                    # cdm = cdm + '0' + str(month)
                # else:
                    # cdm = cdm + str(month)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/DO/YearMonth="+cdm)
                po = po.withColumn("vouch_date",po['vouch_date'].cast(DateType()))
                po.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/DO")
        except Exception as e:
            print(e)


#PU_PO_Links
Start_Year = 2018
os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/PUPOLinks")
if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/PUPOLinks") !=0):
    month = int(datetime.today().month)
    if(month<=3):
        End_Year = (datetime.today().year) - 1
    else:
        End_Year = int(datetime.today().year)
    for i in range(Start_Year,End_Year+1):
        FY = str(i)+str(i+1)
        print(FY)
        purchase = read_data_sql("(Select PU_Code,PO_Code as Code , Qty_Adjust ,Fin_Year from PU_PO_Links where Fin_Year = '"+FY+"') as pur")
        # purchase = purchase.filter(POLinks['Code']== 492650)
        # purchase.show()
        # exit()
        purchase.cache()
        if(purchase.count() > 0):
            purchase.coalesce(1).write.mode("append").partitionBy("Fin_Year").save(hdfspath+"/Market/Stage1/PUPOLinks")
            
print(datetime.now())

#PUHead1         
Start_Year = 2018
try:   
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/PUHead1") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            PUHead1 = read_data_sql("(Select CONVERT(VARCHAR(4),YEAR(ph.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.vouch_date)),2) as YearMonth,ph.Deleted_,ph.vouch_date, ph.vouch_num, ph.vouch_code, ph.Bill_No,ph.Goods_In_Transit,ph.Bill_Amount ,ph.Net_Amt ,\
                                    ph.Stock_Trans,ph.Cust_Code,ph.Agent_Code,ph.Branch_Code,ph.Bill_Date ,ph.GRN_PreFix,ph.GRN_Number,ph.Tax_Reg_Code,ptxn.CF_Qty,ptxn.Qty_Weight,ptxn.Rate,ptxn.Tot_Qty,ptxn.Free_Qty,ptxn.Repl_Qty,ptxn.Sample_Qty,ptxn.Comm_Calc_Code,ptxn.Godown_Code,ptxn.\
                                    Calc_Gross_Amt,ptxn.Calc_Net_Amt,ptxn.Calc_Commission,ptxn.Calc_Sp_Commission,ptxn.Calc_Rdf,ptxn.Calc_Scheme_U,ptxn.Calc_Scheme_Rs,ptxn.Calc_tax_1,ptxn.Order_Item_Code,\
                                    ptxn.Calc_tax_2,ptxn.Calc_Tax_3,ptxn.Calc_Sur_On_Tax3,ptxn.Calc_Excise_U,ptxn.Calc_Adjustment_u,ptxn.Calc_Adjust_RS,ptxn.Calc_Freight,ptxn.Calc_Adjust,ptxn.Lot_Sch_Code,ptxn.Carton_Code,ptxn.Calc_Sale_Amt,ptxn.Challan_Code,\
                                    ptxn.Calc_Spdisc,ptxn.Calc_DN,ptxn.Calc_cn,ptxn.Calc_Display,ptxn.Calc_Handling,ptxn.Calc_Postage,ptxn.Calc_MFees,ptxn.Calc_Labour,ptxn.calc_round,\
                                    ptxn.Pur_Or_PR,ptxn.item_det_code, ptxn.lot_code, ptxn.Txn_Deleted_,ph1.remarks1,ph1.gr_number,ph1.gr_date,ph1.remarks2,ph1.remarks3,ph1.Transport_Mode\
                                        from Pur_Head"+FY+" ph left join Pur_Head1"+FY+" ph1 on ph.Vouch_Code = ph1.Vouch_Code left join Pur_Txn"+FY+" ptxn on ph.Vouch_Code = ptxn.Vouch_Code) as pur")
            PUHead1 = PUHead1.where(col("Vouch_Date").isNotNull())
            PUHead1.cache()
            
            PUHead1.show(1)
            if(PUHead1.count() > 0):
                PUHead1 = PUHead1.withColumn("Vouch_Date",PUHead1['Vouch_Date'].cast(DateType()))
                PUHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PUHead1")
                PUHead1.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PUHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(PUHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            cdm = str(datetime.today().year)
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
                
            if(month <= 9):
                cdm = cdm + '0' + str(month)
                # print("111111111111111111111111111111111111111111111111111111111111")
            else:
                cdm = cdm + str(month)
                # print("222222222222222222222222222222222222222222222222222")
            print("#"*100)
            print(cdm)
            try:
                PUHead1 =read_data_sql ("(Select CONVERT(VARCHAR(4),YEAR(ph.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(ph.vouch_date)),2) as YearMonth,ph.Deleted_,ph.vouch_date, ph.vouch_num, ph.vouch_code, ph.Bill_No,ph.Goods_In_Transit,ph.Bill_Amount ,ph.Net_Amt ,\
                                    ph.Stock_Trans,ph.Cust_Code,ph.Agent_Code,ph.Branch_Code,ph.Bill_Date ,ph.GRN_PreFix,ph.GRN_Number,ph.Tax_Reg_Code,ptxn.CF_Qty,ptxn.Qty_Weight,ptxn.Rate,ptxn.Tot_Qty,ptxn.Free_Qty,ptxn.Repl_Qty,ptxn.Sample_Qty,ptxn.Comm_Calc_Code,ptxn.Godown_Code,ptxn.\
                                    Calc_Gross_Amt,ptxn.Calc_Net_Amt,ptxn.Calc_Commission,ptxn.Calc_Sp_Commission,ptxn.Calc_Rdf,ptxn.Calc_Scheme_U,ptxn.Calc_Scheme_Rs,ptxn.Calc_tax_1,ptxn.Order_Item_Code,\
                                    ptxn.Calc_tax_2,ptxn.Calc_Tax_3,ptxn.Calc_Sur_On_Tax3,ptxn.Calc_Excise_U,ptxn.Calc_Adjustment_u,ptxn.Calc_Adjust_RS,ptxn.Calc_Freight,ptxn.Calc_Adjust,ptxn.Lot_Sch_Code,ptxn.Carton_Code,ptxn.Calc_Sale_Amt,ptxn.Challan_Code,\
                                    ptxn.Calc_Spdisc,ptxn.Calc_DN,ptxn.Calc_cn,ptxn.Calc_Display,ptxn.Calc_Handling,ptxn.Calc_Postage,ptxn.Calc_MFees,ptxn.Calc_Labour,ptxn.calc_round,\
                                    ptxn.Pur_Or_PR,ptxn.item_det_code, ptxn.lot_code, ptxn.Txn_Deleted_,ph1.remarks1,ph1.gr_number,ph1.gr_date,ph1.remarks2,ph1.remarks3,ph1.Transport_Mode\
                                        from Pur_Head"+FY+" ph left join Pur_Head1"+FY+" ph1 on ph.Vouch_Code = ph1.Vouch_Code left join Pur_Txn"+FY+" ptxn on ph.Vouch_Code = ptxn.Vouch_Code  where month(ph.vouch_date) = "+str(month)+") as pur")
                PUHead1 = PUHead1.where(col("Vouch_Date").isNotNull())
                
                PUHead1.cache()
                if(PUHead1.count() > 0):
                    # cdm = str(datetime.today().year)
                    # if(month <= 9):
                        # cdm = cdm + '0' + str(month)
                    # else:
                        # cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/PUHead1/YearMonth="+cdm)
                    PUHead1 = PUHead1.withColumn("Vouch_Date",PUHead1['Vouch_Date'].cast(DateType()))
                    #PUHead1.show(1)
                    PUHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PUHead1")            
                    #PUHead1.show(1)
                    print('PUHead1')
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PUHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(PUHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PUHead1','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")



#SLHead1            
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/SLHead1") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            SLHead1 = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth, st.Code,st.cf_qty,st.Qty_Weight,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,st.Calc_Gross_Amt,\
                                    st.Calc_Commission,st.Calc_Sp_Commission,st.Calc_Rdf,st.Calc_Scheme_U,st.Calc_Scheme_Rs,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,\
                                    st.Calc_tax_2,st.Calc_Tax_3,st.Calc_Sur_On_Tax3,st.Calc_Excise_U,st.Calc_Adjustment_u,st.Calc_Freight,st.Calc_Adjust,st.used_cf_rate,st.Godown_Code,st.Comm_Calc_Code,st.Sa_Subs_Lot,st.Deleted,sh.Bill_Cust_Code,sh.Vouch_Num,sh.Series_Code,sh.Number_,sh.Net_Amt ,sh.New_Vouch_Num ,sh.Tax_Reg_Code,\
                                    sh.Pay_Mode,st.Calc_Adjust_RS,st.Challan_Code,sh.Cashier_Code,st.Calc_Net_Amt as NET_SALE_VALUE, st.Sale_Or_SR as Type, sh.vouch_date, sh.Branch_Code, st.item_det_code, st.lot_code, st.Tot_Qty,st.rate,\
                                    (st.calc_tax_1) as Total_GST_Amount_Paid,sh.Stock_Trans,sh.vouch_code,\
                                    sh.cust_code,sh1.remarks1,sh1.remarks2,sh1.remarks3 from Sl_Head"+FY+"  sh left join Sl_Head1"+FY+"  sh1 on sh.Vouch_Code = sh1.Vouch_Code \
                                      left join Sl_Txn"+FY+"  st on sh.Vouch_Code = st.Vouch_Code ) as slhead1")
            SLHead1 = SLHead1.where(col("Vouch_Date").isNotNull())
            SLHead1.cache()
            
            # SLHead1.show(1)
            if(SLHead1.count() > 0):
                SLHead1 = SLHead1.withColumn("Vouch_Date",SLHead1['Vouch_Date'].cast(DateType()))
                SLHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLHead1")
                SLHead1.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            cdm = str(datetime.today().year)
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
            
            if(month <= 9):
                cdm = cdm + '0' + str(month)
                # print("111111111111111111111111111111111111111111111111111111111111")
            else:
                cdm = cdm + str(month)
                # print("222222222222222222222222222222222222222222222222222")
            print("#"*100)
            print(cdm)
            try:
                SLHead1 =read_data_sql ("(SELECT CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth, st.Code,st.cf_qty,st.Qty_Weight,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,st.Calc_Gross_Amt,\
                    st.Calc_Commission,st.Calc_Sp_Commission,st.Calc_Rdf,st.Calc_Scheme_U,st.Calc_Scheme_Rs,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,\
                    st.Calc_tax_2,st.Calc_Tax_3,st.Calc_Sur_On_Tax3,st.Calc_Excise_U,st.Calc_Adjustment_u,st.Calc_Freight,st.Calc_Adjust,st.used_cf_rate,st.Godown_Code,st.Comm_Calc_Code,st.Sa_Subs_Lot,st.Deleted,sh.Bill_Cust_Code,sh.Vouch_Num,sh.Series_Code,sh.Number_,sh.Net_Amt ,sh.New_Vouch_Num ,sh.Tax_Reg_Code,\
                sh.Pay_Mode,st.Calc_Adjust_RS,st.Challan_Code,st.Calc_Net_Amt as NET_SALE_VALUE, st.Sale_Or_SR as Type, sh.vouch_date, sh.Branch_Code, st.item_det_code, st.lot_code, st.Tot_Qty,st.rate,\
                (st.calc_tax_1) as Total_GST_Amount_Paid,sh.Stock_Trans,sh.vouch_code,\
                sh.cust_code,sh1.remarks1,sh1.remarks2,sh1.remarks3 from Sl_Head"+FY+" sh left join Sl_Head1"+FY+" sh1 on sh.Vouch_Code = sh1.Vouch_Code\
                      left join Sl_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code where month(sh.vouch_date) = "+str(month)+" ) as pur")
                SLHead1 = SLHead1.where(col("Vouch_Date").isNotNull())
                
                SLHead1.cache()
                if(SLHead1.count() > 0):
                    #cdm = str(datetime.today().year)
                    # if(month <= 9):
                        # cdm = cdm + '0' + str(month)
                    # else:
                        # cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/SLHead1/YearMonth="+cdm)
                    SLHead1 = SLHead1.withColumn("Vouch_Date",SLHead1['Vouch_Date'].cast(DateType()))
                    
                    SLHead1.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLHead1")            
                    #SLHead1.show(1)
                    print('SLHead1')
                    
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLHead1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLHead1.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLHead1','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
    
    
    

#STRF         
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/STRF") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            STRF = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Date,st.Vouch_Code,st.Godown_Code,st.Rate,sh.Vouch_Num,sh.Cust_Code,st.Lot_Code_To,sh.Branch_Code,sh.Dept_Code,sh.G_Code_From,st.Quantity,sh.G_Code_To,sh.Remarks_1 from STRF_HEAD"+FY+" sh left join STRF_TXN"+FY+" st on sh.Vouch_Code = st.Vouch_Code ) as strf")
            STRF = STRF.where(col("Vouch_Date").isNotNull())
            STRF.cache()
            
            STRF.show(1)
            if(STRF.count() > 0):
                STRF = STRF.withColumn("Vouch_Date",STRF['Vouch_Date'].cast(DateType()))
                STRF.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/STRF")
                STRF.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'STRF','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(STRF.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            cdm = str(datetime.today().year)
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
                
            if(month <= 9):
                cdm = cdm + '0' + str(month)
                # print("111111111111111111111111111111111111111111111111111111111111")
            else:
                cdm = cdm + str(month)
                # print("222222222222222222222222222222222222222222222222222")
            print("#"*100)
            print(cdm)
            try:
                STRF = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Date,st.Vouch_Code,sh.Vouch_Num,sh.Cust_Code,st.Godown_Code,st.Rate,sh.Branch_Code,st.Lot_Code_To,sh.Dept_Code,sh.G_Code_From,st.Quantity,sh.G_Code_To,sh.Remarks_1 from STRF_HEAD"+FY+" sh left join STRF_TXN"+FY+" st on sh.Vouch_Code = st.Vouch_Code  where month(sh.vouch_date) = "+str(month)+") as strf")
                STRF = STRF.where(col("Vouch_Date").isNotNull())
                print('1')
                STRF.cache()
                if(STRF.count() > 0):
                    # cdm = str(datetime.today().year)
                    # if(month <= 9):
                        # cdm = cdm + '0' + str(month)
                    # else:
                        # cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/STRF/YearMonth="+cdm)
                    STRF = STRF.withColumn("Vouch_Date",STRF['Vouch_Date'].cast(DateType()))
                    #STRF.show(1)
                    STRF.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/STRF")            
                    #STRF.show(1)
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'STRF','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(STRF.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'STRF','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
    

##SLSCH
Start_Year = 2018
try:   
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/SLSCH") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            print(FY)
            SLSCH = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Code,sh.Vouch_Date,sc.Code_Type,sc.Sch_Det_Code,sc.SL_Txn_Code,sc.CD,sc.TD,sc.CALC_CD,sc.CALC_TD,st.Calc_Gross_Amt,st.Tot_Qty,\
                                    st.Calc_Commission,st.Calc_Net_Amt as NET_SALE_VALUE,st.calc_sp_commission,st.calc_rdf,st.calc_scheme_u,st.calc_scheme_rs,(st.calc_tax_1) as Total_GST_Amount_Paid,st.Calc_Tax_2,st.Calc_Tax_3,st.calc_sur_on_tax3,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,\
                                    sc.Calc_Sch_Unit, sc.Calc_Sch_Rs,sh.Vouch_Num,sh.Net_Amt,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Freight,st.Calc_Adjust,st.calc_excise_u,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,st.Item_Det_Code,st.Lot_Code,st.Calc_Adjustment_u,\
                                   st.Calc_Adjust_RS,st.Sale_Or_SR as Type,st.Deleted,sh.Cust_Code,sh.Branch_Code,sh.Stock_Trans from SL_Head"+FY+" sh left join SL_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code inner join SL_SCH"+FY+" sc on sc.SL_Txn_Code = st.Code ) as slsch")

            SLSCH = SLSCH.where(col("Vouch_Date").isNotNull())
            print(SLSCH.count())
            SLSCH.cache()
            if(SLSCH.count() > 0):
                SLSCH = SLSCH.withColumn("Vouch_Date",SLSCH['Vouch_Date'].cast(DateType()))
                SLSCH.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLSCH")
                #SLSCH.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLSCH','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLSCH.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            cdm = str(datetime.today().year)
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
            
            if(month <= 9):
                cdm = cdm + '0' + str(month)
                # print("111111111111111111111111111111111111111111111111111111111111")
            else:
                cdm = cdm + str(month)
                # print("222222222222222222222222222222222222222222222222222")
            print("#"*100)
            print(cdm)
            try:
                SLSCH = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Code,sh.Vouch_Date,sc.Code_Type,sc.Sch_Det_Code,sc.SL_Txn_Code,sc.CD,sc.TD,sc.CALC_CD,sc.CALC_TD,st.Calc_Gross_Amt,st.Tot_Qty,\
                                    sh.Vouch_Num,st.Calc_Commission,st.Calc_Net_Amt as NET_SALE_VALUE,st.calc_sp_commission,st.calc_rdf,st.calc_scheme_u,st.calc_scheme_rs,(st.calc_tax_1) as Total_GST_Amount_Paid,st.Calc_Tax_2,st.Calc_Tax_3,st.calc_sur_on_tax3,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,\
                                    sc.Calc_Sch_Unit, sc.Calc_Sch_Rs,sh.Net_Amt,st.Calc_Adjust_RS,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Freight,st.Calc_Adjust,st.calc_excise_u,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,st.Item_Det_Code,st.Lot_Code,st.Calc_Adjustment_u,\
                            st.Sale_Or_SR as Type,st.Deleted,sh.Cust_Code,sh.Branch_Code,sh.Stock_Trans from SL_Head"+FY+" sh left join SL_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code inner join SL_SCH"+FY+" sc on sc.SL_Txn_Code = st.Code where month(sh.vouch_date) = "+str(month)+" ) as slsch")

                SLSCH = SLSCH.where(col("Vouch_Date").isNotNull())
                print('1')
                SLSCH.cache()
                if(SLSCH.count() > 0):
                    # cdm = str(datetime.today().year)
                    # if(month <= 9):
                        # cdm = cdm + '0' + str(month)
                    # else:
                        # cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/SLSCH/YearMonth="+cdm)
                    SLSCH = SLSCH.withColumn("Vouch_Date",SLSCH['Vouch_Date'].cast(DateType()))
                    #SLSCH.show(1)
                    SLSCH.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLSCH")            
                    #SLSCH.show(1)
                    print('SLSCH')
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLSCH','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLSCH.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLSCH','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
print(datetime.now())


##Price Drop
Start_Year = 2018
try:   
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/PriceDrop") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            print(FY)
            PriceDrop = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Code,sh.Vouch_Date,sc.Code_Type,sc.Sch_Det_Code,sc.SL_Txn_Code,sc.CD,sc.TD,sc.CALC_CD,sc.CALC_TD,st.Calc_Gross_Amt,st.Tot_Qty,\
                                    st.Calc_Commission,st.Calc_Net_Amt as NET_SALE_VALUE,st.calc_sp_commission,st.calc_rdf,st.calc_scheme_u,st.calc_scheme_rs,(st.calc_tax_1) as Total_GST_Amount_Paid,st.Calc_Tax_2,st.Calc_Tax_3,st.calc_sur_on_tax3,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,\
                                    sh.Net_Amt,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Freight,st.Calc_Adjust,st.calc_excise_u,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,st.Item_Det_Code,st.Lot_Code,st.Calc_Adjustment_u,\
                                   st.Calc_Adjust_RS,st.Sale_Or_SR as Type,st.Deleted,sh.Cust_Code,sh.Branch_Code,sh.Stock_Trans from SL_Head"+FY+" sh left join SL_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code left join SL_SCH"+FY+" sc on sc.SL_Txn_Code = st.Code ) as PriceDrop")

            PriceDrop = PriceDrop.where(col("Vouch_Date").isNotNull())
            print(PriceDrop.count())
            PriceDrop.cache()
            if(PriceDrop.count() > 0):
                PriceDrop = PriceDrop.withColumn("Vouch_Date",PriceDrop['Vouch_Date'].cast(DateType()))
                PriceDrop.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PriceDrop")
                #PriceDrop.show(1)
                print('Data done')
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PriceDrop','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(PriceDrop.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            cdm = str(datetime.today().year)
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
            
            if(month <= 9):
                cdm = cdm + '0' + str(month)
                # print("111111111111111111111111111111111111111111111111111111111111")
            else:
                cdm = cdm + str(month)
                # print("222222222222222222222222222222222222222222222222222")
            print("#"*100)
            print(cdm)
            try:
                PriceDrop = read_data_sql("(SELECT CONVERT(VARCHAR(4),YEAR(sh.Vouch_Date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.Vouch_Date)),2) as YearMonth,sh.Vouch_Code,sh.Vouch_Date,sc.Code_Type,sc.Sch_Det_Code,sc.SL_Txn_Code,sc.CD,sc.TD,sc.CALC_CD,sc.CALC_TD,st.Calc_Gross_Amt,st.Tot_Qty,\
                                    st.Calc_Commission,st.Calc_Net_Amt as NET_SALE_VALUE,st.calc_sp_commission,st.calc_rdf,st.calc_scheme_u,st.calc_scheme_rs,(st.calc_tax_1) as Total_GST_Amount_Paid,st.Calc_Tax_2,st.Calc_Tax_3,st.calc_sur_on_tax3,st.Free_Qty,st.Repl_Qty,st.Sample_Qty,\
                                    sh.Net_Amt,st.Calc_Adjust_RS,st.Calc_MFees,st.Calc_Labour,st.calc_round,st.Calc_Freight,st.Calc_Adjust,st.calc_excise_u,st.Calc_Spdisc,st.Calc_DN,st.Calc_cn,st.Calc_Display,st.Calc_Handling,st.Calc_Postage,st.Item_Det_Code,st.Lot_Code,st.Calc_Adjustment_u,\
                            st.Sale_Or_SR as Type,st.Deleted,sh.Cust_Code,sh.Branch_Code,sh.Stock_Trans from SL_Head"+FY+" sh left join SL_Txn"+FY+" st on sh.Vouch_Code = st.Vouch_Code left join SL_SCH"+FY+" sc on sc.SL_Txn_Code = st.Code where month(sh.vouch_date) = "+str(month)+" ) as PriceDrop")

                PriceDrop = PriceDrop.where(col("Vouch_Date").isNotNull())
                print('1')
                PriceDrop.cache()
                if(PriceDrop.count() > 0):
                    # cdm = str(datetime.today().year)
                    # if(month <= 9):
                        # cdm = cdm + '0' + str(month)
                    # else:
                        # cdm = cdm + str(month)
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/PriceDrop/YearMonth="+cdm)
                    PriceDrop = PriceDrop.withColumn("Vouch_Date",PriceDrop['Vouch_Date'].cast(DateType()))
                    #PriceDrop.show(1)
                    PriceDrop.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/PriceDrop")            
                    #PriceDrop.show(1)
                    print('PriceDrop')
            except Exception as e:
                print(e)         
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PriceDrop','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(PriceDrop.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'PriceDrop','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append")   
    print("\U0001F600")
print(datetime.now())







#SLMPM
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage1/SLMPM") !=0):
        month = int(datetime.today().month)
        if(month<=3):
            End_Year = (datetime.today().year) - 1
        else:
            End_Year = int(datetime.today().year)
        for i in range(Start_Year,End_Year+1):
            FY = str(i)+str(i+1)
            print(FY)
            SLMPM = read_data_sql("(SELECT sh.Net_Amt,sh.MPM_Code,st.Credit_Amount,sh.vouch_date, sh.Branch_Code ,sh.pay_mode,st.CC_Amount,st.Cash_Amount,st.Gift_Vouch_Amount, st.Code,\
                    a.CC_No_Code,sh.vouch_code,CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth\
                        from  sl_head"+FY+" sh    left join Sl_MPM"+FY+" st  on  sh.vouch_code = st.vouch_code \
                            left join Sl_CC_Det"+FY+" a on sh.vouch_code = a.vouch_code ) as sha")
            SLMPM = SLMPM.filter(SLMPM["pay_mode"]=='CH')
            # SLMPM.show()
            # print(SLMPM.count())
            # exit()
            
            
            
            SLMPM = SLMPM.where(col("vouch_date").isNotNull())   
            print(SLMPM.count())
            SLMPM.cache()
            if(SLMPM.count() > 0):
                SLMPM = SLMPM.withColumn("vouch_date",SLMPM['vouch_date'].cast(DateType()))
                SLMPM.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLMPM")
                end_time = datetime.now()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLMPM','DB':DB,'EN':Etn,
                        'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(SLMPM.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
            print(FY)
            cdm = str(datetime.today().year)
            if(month == 0):
                month = 12
                cdm = str(datetime.today().year - 1)
            if(month == -1):
                month = 11
                cdm = str(datetime.today().year - 1)
                
            if(month <= 9):
                cdm = cdm + '0' + str(month)
                # print("111111111111111111111111111111111111111111111111111111111111")
            else:
                cdm = cdm + str(month)
                # print("222222222222222222222222222222222222222222222222222")
            print("#"*100)
            print(cdm)
            try:
                SLMPM = read_data_sql("(SELECT sh.Net_Amt,sh.MPM_Code,st.Credit_Amount,sh.vouch_date, sh.Branch_Code ,sh.pay_mode,st.CC_Amount,st.Cash_Amount,st.Gift_Vouch_Amount, st.Code,\
                    a.CC_No_Code,sh.vouch_code,CONVERT(VARCHAR(4),YEAR(sh.vouch_date))+RIGHT('0' + CONVERT(VARCHAR(2), MONTH(sh.vouch_date)),2) as YearMonth\
                        from  sl_head"+FY+" sh    left join Sl_MPM"+FY+" st  on  sh.vouch_code = st.vouch_code \
                            left join Sl_CC_Det"+FY+" a on sh.vouch_code = a.vouch_code where month(sh.vouch_date) = "+str(month)+") as sha")
            
                
                SLMPM = SLMPM.filter(SLMPM["pay_mode"]=='CH')
                SLMPM = SLMPM.where(col("vouch_date").isNotNull())
                SLMPM.cache()
                if(SLMPM.count() > 0):
                    # cdm = str(datetime.today().year)
                    # if(month <= 9):
                        # cdm = cdm + '0' + str(month)
                    # else:
                        # cdm = cdm + str(month)
                    #hdfs://103.248.60.14:9000
                    os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage1/SLMPM/YearMonth="+cdm)
                    SLMPM = SLMPM.withColumn("vouch_date",SLMPM['vouch_date'].cast(DateType()))
                    SLMPM.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage1/SLMPM")
                    print('SLMPM')
            except Exception as e:
                print(e)
        # SLMPM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SLMPM")
        # SLMPM = SLMPM.drop("YearMonth")
        # write_data_sql(SLMPM,"SLMPM",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLMPM','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(SLMPM.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SLMPM','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append") 
    print("\U0001F600")
    

