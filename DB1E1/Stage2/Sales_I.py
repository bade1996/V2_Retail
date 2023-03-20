from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import sum as sumf, first as firstf, year as yearf
from pyspark.sql.types import *
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime
import pyodbc
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

def delete_data(dqry):
    server = '103.234.187.190,2499' 
    database = 'KOCKPIT' 
    username = 'sa' 
    password = 'Market!@999' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
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
conf = SparkConf().setMaster(smaster).setAppName("Sales")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                 .set("spark.executor.cores","4")\
                 .set("spark.executor.memory","5g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.local.dir", "/tmp/spark-temp")
                 #     .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                 #     set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                 #     set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                 #     set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                 #     set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                 #     set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g").getOrCreate()
                            # .config("spark.sql.broadcastTimeout", "1800")\
                            # .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                            
sqlctx = SQLContext(sc)

print(datetime.now())
Start_Year = 2018
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Sales") !=0):
        print("if")
        #exit()
        sales = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
        sales = sales.select('YearMonth','NET_SALE_VALUE','vouch_num','Type', 'vouch_date', 'Branch_Code', 'item_det_code', 'lot_code', 'Tot_Qty', 'Total_GST_Amount_Paid', 'Stock_Trans','Discount_Amount','Cashier_Code','Vouch_Time','vouch_code','rate','cust_code','Code','series_code')
        sales = sales.filter(sales['Stock_Trans'] == 0)
        sales = sales.drop('Stock_Trans')
        sales.cache()
        if(sales.count() > 0):
            sales = sales.withColumn('Tot_Qty',when(sales.Type=='SL',sales.Tot_Qty).otherwise(sales.Tot_Qty*-1))
            sales = sales.withColumn('Gross_Amount',sales['Tot_Qty']*sales['rate'])
            sales = sales.withColumn('NET_SALE_VALUE',when(sales.Type=='SL',sales.NET_SALE_VALUE).otherwise(sales.NET_SALE_VALUE*-1))
            sales = sales.where(col("vouch_date").isNotNull())
            sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
            sales = sales.groupby('vouch_date','Branch_Code','lot_code','vouch_num','YearMonth','Cashier_Code','Vouch_Time','vouch_code','cust_code','Code','series_code').agg({"Tot_Qty":'sum',"Total_GST_Amount_Paid":'sum',"NET_SALE_VALUE":'sum',"Discount_Amount":'sum',"Gross_Amount":'sum'})
            sales = sales.withColumnRenamed('sum(Tot_Qty)','Sales_Quantity').withColumnRenamed('sum(Total_GST_Amount_Paid)','Total_GST_Amount_Paid').withColumnRenamed('sum(NET_SALE_VALUE)','NET_SALE_VALUE').withColumnRenamed('sum(Discount_Amount)','Discount_Amount').withColumnRenamed('sum(Gross_Amount)','Gross_Amount')
            
            sales.cache()
            print(sales.count())
            sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Sales")
            End_Year = (datetime.today().year)
            month = 4
            while(Start_Year <= End_Year):
                if(month <= 9):
                    cdm = str(Start_Year) +'0'+str(month)
                else:
                    cdm = str(Start_Year) + str(month)
                if(month == 12):
                    month = 0
                    Start_Year = Start_Year + 1
                month = month + 1
                print(cdm)
                try:
                    sales_temp = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Sales/YearMonth="+cdm)
                    sales_temp = sales_temp.withColumn('YearMonth',lit(cdm))
                    if(cdm == '201804'):
                        write_data_sql(sales_temp,"SalesI",owmode)
                    else:
                        write_data_sql(sales_temp,"SalesI",apmode)
                except Exception as e:
                    print(e)
                if((int(datetime.today().month)+1 == month) & (int(datetime.today().year) == Start_Year)):
                    exit()
            print("Sales Done")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SalesI','DB':DB,'EN':Etn,
                'Status':'Completed','ErrorLineNo':'NA','Operation':'FullReload','Rows':str(sales_temp.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
            log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
            write_data_sql(log_df,"Logs",mode="append")
    else:
        print("else")
        #exit()
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
                sales = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales/YearMonth="+cdm)
                sales = sales.select('NET_SALE_VALUE', 'Type', 'vouch_date','vouch_num','Branch_Code', 'item_det_code', 'lot_code', 'Tot_Qty', 'Total_GST_Amount_Paid', 'Stock_Trans','Discount_Amount','Cashier_Code','Vouch_Time','vouch_code','rate','cust_code','Code','series_code')
                sales = sales.filter(sales['Stock_Trans'] == 0)
                sales = sales.drop('Stock_Trans')
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Sales/YearMonth="+cdm)
                sales = sales.withColumn('YearMonth',lit(cdm))
                sales = sales.withColumn('Tot_Qty',when(sales.Type=='SL',sales.Tot_Qty).otherwise(sales.Tot_Qty*-1))
                sales = sales.withColumn('Gross_Amount',sales['Tot_Qty']*sales['rate'])
                sales = sales.withColumn('NET_SALE_VALUE',when(sales.Type=='SL',sales.NET_SALE_VALUE).otherwise(sales.NET_SALE_VALUE*-1))
                sales = sales.where(col("vouch_date").isNotNull())
                sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
                sales = sales.groupby('vouch_date','Branch_Code','lot_code','vouch_num','YearMonth','Cashier_Code','Vouch_Time','vouch_code','cust_code','Code','series_code').agg({"Tot_Qty":'sum',"Total_GST_Amount_Paid":'sum',"NET_SALE_VALUE":'sum',"Discount_Amount":'sum',"Gross_Amount":'sum'})
                sales = sales.withColumnRenamed('sum(Tot_Qty)','Sales_Quantity').withColumnRenamed('sum(Total_GST_Amount_Paid)','Total_GST_Amount_Paid').withColumnRenamed('sum(NET_SALE_VALUE)','NET_SALE_VALUE').withColumnRenamed('sum(Discount_Amount)','Discount_Amount').withColumnRenamed('sum(Gross_Amount)','Gross_Amount')
                
                sales.cache()
                print(sales.count())
                sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Sales")
                delete_data(''' DELETE FROM KOCKPIT.dbo.SalesI WHERE YearMonth = ''' + cdm)
                write_data_sql(sales,"SalesI",apmode)
                print("Sales Done")
                
            except Exception as e:
                print(e)
                print('jai')
    print(datetime.now())
    end_time = datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SalesI','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Rows':str(sales.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",apmode)
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SalesI','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",apmode)