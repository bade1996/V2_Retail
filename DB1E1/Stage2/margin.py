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
conf = SparkConf().setMaster(smaster).setAppName("Margin")\
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


item = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Item")
item=item.select('GST_CATEGORY','item_det_code')
item=item.withColumn('GST1', regexp_replace(col("GST_CATEGORY"), "GST ", ""))
item=item.withColumn('GST', (regexp_replace(col("GST1"), "%", "")/100))

item=item.drop('GST1').drop('GST_CATEGORY')
item= item.withColumn('GST2',1+item['GST'])
#item.show()
# exit()
lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
lot = lot.select('item_det_code','lot_code','mrp_lot','pur_rate_lot','sale_rate_lot','basic_rate_lot')
lot = lot.withColumnRenamed('pur_rate_lot','PurRate')

lot=lot.join(item,'item_det_code','left').drop('item_det_code')

print(datetime.now())
Start_Year = 2018

##Margin
print(datetime.now())
try:
    if(os.system("/home/padmin/hadoop-3.2.2/bin/hdfs dfs -test -d /KOCKPIT/Market/Stage2/Margin") !=0):
        sales = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales")
        #sales = sales.select('YearMonth','NET_SALE_VALUE','vouch_num','Type', 'vouch_date', 'Branch_Code', 'item_det_code', 'lot_code', 'Tot_Qty', 'Total_GST_Amount_Paid', 'Stock_Trans','Discount_Amount','Cashier_Code','Vouch_Time','vouch_code','rate','cust_code','Code','series_code')
        
        
        sales.cache()
        if(sales.count() > 0):
            sales=sales.join(lot,'lot_code','left')
            sales = sales.filter(sales['Stock_Trans'] == 0)
            sales = sales.drop('Stock_Trans')
            
            sales= sales.withColumn('GST_INPUT',sales['basic_rate_lot']*sales['GST'])
            sales= sales.withColumn('CostPrice',sales['basic_rate_lot']+sales['GST_INPUT'])
            # sales= sales.withColumn('test',1+sales['GST'])
            # sales= sales.withColumn('test1',sales['basic_rate_lot']-sales['Discount_Amount'])
            # sales= sales.withColumn('BasicSellingPrice',sales['test1']/sales['test'])
            sales= sales.withColumn('BASIC_SELLING_PRICE',((sales['mrp_lot']-sales['Discount_Amount'])/sales['GST2']))
            
            sales= sales.withColumn('GST_OUTPUT',sales['BASIC_SELLING_PRICE']*sales.GST)
            sales= sales.withColumn('GST_Diff',sales['GST_OUTPUT']-sales['GST_INPUT'])
            sales= sales.withColumn('MarginValue',sales['mrp_lot']-sales['Discount_Amount']-sales['CostPrice'])
            sales= sales.withColumn('BUSINESS_VALUE_PER_UNIT_%_TO_SP',((sales['mrp_lot']-sales['CostPrice'])/sales['mrp_lot']))
            sales= sales.withColumn('Net_Margin_Value',sales['MarginValue']-sales['GST_Diff'])
            sales= sales.withColumn('MARGIN_VALUE_PER_UNIT_%_TO_SP',(sales['Net_Margin_Value']/sales['mrp_lot']))
            
            sales = sales.withColumn('Tot_Qty',when(sales.Type=='SL',sales.Tot_Qty).otherwise(sales.Tot_Qty*-1))
            sales = sales.withColumn('Gross_Amount',sales['Tot_Qty']*sales['rate'])
            sales = sales.withColumn('NET_SALE_VALUE',when(sales.Type=='SL',sales.NET_SALE_VALUE).otherwise(sales.NET_SALE_VALUE*-1))
            sales = sales.where(col("vouch_date").isNotNull())
            sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
            sales = sales.groupby('vouch_date','Branch_Code','lot_code','vouch_num','YearMonth','Cashier_Code','basic_rate_lot','GST',\
                                  'PurRate','Vouch_Time','vouch_code','cust_code','Code','series_code','mrp_lot')\
                                  .agg(sum('Tot_Qty').alias('Sales_Quantity'),sum('NET_SALE_VALUE').alias('NET_SALE_VALUE'),sum('Discount_Amount').alias('Discount_Amount'),\
                                        sum('Total_GST_Amount_Paid').alias('Tax1'),sum('Calc_tax_3').alias('Tax3'),sum('Gross_Amount').alias('Gross_Amount'),sum('BASIC_SELLING_PRICE').alias('BASIC_SELLING_PRICE')\
                                         ,sum('GST_INPUT').alias('GST_INPUT'),sum('GST_OUTPUT').alias('GST_OUTPUT'),sum('GST_Diff').alias('GST_Diff'),sum('MarginValue').alias('MarginValue'),\
                                         sum('CostPrice').alias('CostPrice'),sum('BUSINESS_VALUE_PER_UNIT_%_TO_SP').alias('BUSINESS_VALUE_PER_UNIT_%_TO_SP'),sum('Net_Margin_Value').alias('Net_Margin_Value')\
                                         ,sum('MARGIN_VALUE_PER_UNIT_%_TO_SP').alias('MARGIN_VALUE_PER_UNIT_%_TO_SP'))
                      
            sales.cache()
            print(sales.count())
            sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Margin")
            end_time = datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Margin','DB':DB,'EN':Etn,
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
                sales = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Sales/YearMonth="+cdm)
                #TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/PUHead1/YearMonth="+cdm)
                os.system("/home/padmin/hadoop-3.2.2/bin/hadoop fs -rm -r /KOCKPIT/Market/Stage2/Margin/YearMonth="+cdm)
               
                sales=sales.join(lot,'lot_code','left')
                sales = sales.filter(sales['Stock_Trans'] == 0)
                sales = sales.drop('Stock_Trans')
                sales = sales.withColumn('YearMonth',lit(cdm))
                sales= sales.withColumn('GST_INPUT',sales['basic_rate_lot']*sales['GST'])
                sales= sales.withColumn('CostPrice',sales['basic_rate_lot']+sales['GST_INPUT'])
                # sales= sales.withColumn('test',1+sales['GST'])
                # sales= sales.withColumn('test1',sales['basic_rate_lot']-sales['Discount_Amount'])
                # sales= sales.withColumn('BasicSellingPrice',sales['test1']/sales['test'])
                sales= sales.withColumn('BASIC_SELLING_PRICE',((sales['mrp_lot']-sales['Discount_Amount'])/sales['GST2']))
                
                # sales.show()
                # exit()
                sales= sales.withColumn('GST_OUTPUT',sales['BASIC_SELLING_PRICE']*sales.GST)
                sales= sales.withColumn('GST_Diff',sales['GST_OUTPUT']-sales['GST_INPUT'])
                sales= sales.withColumn('MarginValue',sales['mrp_lot']-sales['Discount_Amount']-sales['CostPrice'])
                sales= sales.withColumn('BUSINESS_VALUE_PER_UNIT_%_TO_SP',((sales['mrp_lot']-sales['CostPrice'])/sales['mrp_lot']))
                sales= sales.withColumn('Net_Margin_Value',sales['MarginValue']-sales['GST_Diff'])
                sales= sales.withColumn('MARGIN_VALUE_PER_UNIT_%_TO_SP',sales['Net_Margin_Value']/sales['mrp_lot'])
                
                sales = sales.withColumn('Tot_Qty',when(sales.Type=='SL',sales.Tot_Qty).otherwise(sales.Tot_Qty*-1))
                sales = sales.withColumn('Gross_Amount',sales['Tot_Qty']*sales['rate'])
                sales = sales.withColumn('NET_SALE_VALUE',when(sales.Type=='SL',sales.NET_SALE_VALUE).otherwise(sales.NET_SALE_VALUE*-1))
                sales = sales.where(col("vouch_date").isNotNull())
                sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
                sales = sales.groupby('vouch_date','Branch_Code','lot_code','vouch_num','YearMonth','Cashier_Code','basic_rate_lot','GST',\
                                      'PurRate','Vouch_Time','vouch_code','cust_code','Code','series_code','mrp_lot')\
                                      .agg(sum('CostPrice').alias('CostPrice'),sum('Tot_Qty').alias('Sales_Quantity'),sum('NET_SALE_VALUE').alias('NET_SALE_VALUE'),sum('Discount_Amount').alias('Discount_Amount'),\
                                           sum('Total_GST_Amount_Paid').alias('Tax1'),sum('Calc_tax_3').alias('Tax3'),sum('Gross_Amount').alias('Gross_Amount')),sum('BASIC_SELLING_PRICE').alias('BASIC_SELLING_PRICE')\
                                            ,sum('GST_INPUT').alias('GST_INPUT'),sum('GST_OUTPUT').alias('GST_OUTPUT'),sum('GST_Diff').alias('GST_Diff'),sum('MarginValue').alias('MarginValue'),\
                                            sum('BUSINESS_VALUE_PER_UNIT_%_TO_SP').alias('BUSINESS_VALUE_PER_UNIT_%_TO_SP'),sum('Net_Margin_Value').alias('Net_Margin_Value')\
                                            ,sum('MARGIN_VALUE_PER_UNIT_%_TO_SP').alias('MARGIN_VALUE_PER_UNIT_%_TO_SP')
                
                # sales.cache()
                # print(sales.count())
                sales.coalesce(1).write.mode("append").partitionBy("YearMonth").save(hdfspath+"/Market/Stage2/Margin")
            
                print("Margin Done")
            except Exception as e:
                print(e)
        print(datetime.now())
        TXN = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Margin")
        #TXN.show()
        TXN = TXN.drop("YearMonth")
        write_data_sql(TXN,"Margin",owmode)
        end_time = datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Margin','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo':'NA','Operation':'IncrementalReload','Columns':len(TXN.columns),'Rows':str(TXN.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Margin','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
    write_data_sql(log_df,"Logs",mode="append") 
