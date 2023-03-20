from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import sum as sumf, first as firstf
import smtplib,sys,os
from email import encoders
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from pyspark.sql.window import Window
import os,pandas as pd
from functools import reduce
from datetime import datetime, timedelta
import json

#print(Stop by Rahul Khanna)

def mail(df,Sdate,Edate):
    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()
        server.login("automailer@market99.com", "Mailer@M99")
        sender = "automailer@market99.com"
        recipient = ["ravi.sharma@market99.com"]
        Cc = ["akash.shukla@market99.com","prashant.gupta@market99.com","prashant.s@kockpit.in","rahul.khanna@kockpit.in"]
        # recipient = ["rahul.khanna@kockpit.in"]
        # Cc = ["rahul.khanna@kockpit.in","prashant.s@kockpit.in"]
        msg = MIMEMultipart()
        msg['Subject'] = 'MER Report - '+str(Edate)
        msg['From'] = sender
        msg['To'] = ', '.join(recipient)
        msg['Cc'] = ', '.join(Cc)
        msg.attach(MIMEText('Hi User, \n\nPlease find attached DGR form '+str(Sdate)+' to '+str(Edate)+' \n\n Thanks, \n Auto Mailer(KOCKPIT)'))
        
        df.toPandas().to_excel("mailer.xlsx")
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open("mailer.xlsx", "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="Kockpit_Automailer.xlsx"')
        
        #part = MIMEApplication(df.toPandas().to_csv(), Name="Sales.csv")
        #part['Content-Disposition'] = 'attachment; filename="%s"' % 'Sales_Stock.csv'
        
        msg.attach(part)
        server.sendmail(sender, (recipient + Cc) , msg.as_string())
        server.close()
    except Exception as e:
        print(e)
    
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
    
#For reading data from the SQL server
def read_data_sql_kockpit(table_string):
    database = "KOCKPIT"
    user = "sa"
    password  = "Market!@999"
    SQLurl = "jdbc:sqlserver://103.234.187.190:2499;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

def column_add(a,b):
    return a.__add__(b)

config = os.path.dirname(os.path.realpath(__file__))
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
    
#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("mailer")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set("spark.executor.cores","4")\
                .set("spark.executor.memory","5g")\
                .set("spark.driver.maxResultSize","0")\
                .set("spark.sql.debug.maxToStringFields", "1000")\
                .set("spark.executor.instances", "20")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.sql.parquet.enableVectorizedReader","false")\
                .set("spark.local.dir", "/tmp/spark-temp")\
                     .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                     set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                     set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                     set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                     set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)
try:
    load = sys.argv[1]
    df_jobs = read_data_sql("(SELECT JobId , UserId , Args , SubmittedOn , StartedOn , EndedOn, JobStatus FROM [KOCKPIT].[dbo].[_Jobs] where EndedOn IS NULL) as jobs")
    if(df_jobs.count() == 0):
        exit()
    df_jobs.sort('JobId')
    script_run_or_not = df_jobs.collect()[0]['StartedOn']
    jobid = df_jobs.collect()[0]['JobId']
    if(script_run_or_not != None):
        exit()
    var_json = df_jobs.collect()[0]['Args']
    var_json = json.loads(var_json)
    df_jobs = read_data_sql("(SELECT JobId , UserId , Args , SubmittedOn , StartedOn , EndedOn, JobStatus FROM [KOCKPIT].[dbo].[_Jobs]) as jobs")
    df_jobs = df_jobs.withColumn('StartedOn',when(df_jobs['JobId'] == jobid,datetime.now()).otherwise(df_jobs['StartedOn']))
    df_jobs.cache()
    print(df_jobs.count())
    write_data_sql(df_jobs,"_jobs",owmode)
except Exception as e :
    print(e)
    load = "Manual"

if(load == 'APP'):
    Sdate = var_json['StartDate']
    Edate = var_json['EndDate']
    if(var_json['Branch'] == 'ALL'):
        Branch = var_json['Branch']
    else:
        Branch = list(eval(var_json['Branch']))
    if(var_json['Department'] == 'ALL'):
        Department = var_json['Department']
    else:
        Department = list(eval(var_json['Department']))
    if(var_json['NewCompanyName'] == 'ALL'):
        New_Company_name = var_json['NewCompanyName']
    else:
        New_Company_name = list(eval(var_json['NewCompanyName']))
    if(var_json['BranchGroup'] == 'ALL'):
        Branch_Group = var_json['BranchGroup']
    else:
        Branch_Group = list(eval(var_json['BranchGroup']))
    if(var_json['Category'] == 'ALL'):
        Category = var_json['Category']
    else:
        Category = list(eval(var_json['Category']))
    if(var_json['Godown'] == 'ALL'):
        Godown = var_json['Godown']
    else:
        Godown = list(eval(var_json['Godown']))
    if(var_json['LocalImport'] == 'ALL'):
        Local_import = var_json['LocalImport']
    else:
        Local_import = list(eval(var_json['LocalImport']))
    if(var_json['SubCategory'] == 'ALL'):
        SubCategory = var_json['SubCategory']
    else:
        SubCategory = list(eval(var_json['SubCategory']))
    if(var_json['GodownGroup'] == 'ALL'):
        GodownGroup = var_json['GodownGroup']
    else:
        GodownGroup = list(eval(var_json['GodownGroup']))
    if(var_json['ItemCode'] == 'ALL'):
        ItemCode = var_json['ItemCode']
    else:
        ItemCode = list(eval(var_json['ItemCode']))
    if(var_json['EmailTo'] == ''):
        Emailto = var_json['EmailTo']
    else:
        Emailto = list(eval(var_json['EmailTo']))
    if(var_json['EmailCC'] == ''):
        Emailcc = var_json['EmailCC']
    else:
        Emailcc = list(eval(var_json['EmailCC']))
    if(var_json['EmailBCC'] == ''):
        EmailBcc = var_json['EmailBCC']
    else:
        EmailBcc = list(eval(var_json['EmailBCC']))
    cdm = str(Edate.split('-')[0]) + str(Edate.split('-')[1])
else:
    Sdate = datetime.today() - timedelta(days=30)
    Edate = datetime.today() - timedelta(days=1)
    Sdate = Sdate.date()
    Edate = Edate.date()
    Branch = 'ALL'
    Department = 'ALL'
    New_Company_name = 'ALL'
    Branch_Group = 'ALL'
    Category = 'ALL'
    Godown = ['MAIN','(NIL)']
    Local_import = 'ALL'
    SubCategory = 'ALL'
    GodownGroup = ['MAIN STOCK LOCATION']
    ItemCode = 'ALL'
    Emailto = ["rahul.khanna@kockpit.in"]
    Emailcc = ["rahul.khanna@kockpit.in"]
    EmailBcc = ["rahul.khanna@kockpit.in"]
    cdm = str(Edate.year) + str(Edate.strftime("%m"))

sales = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Sales")
sales = sales.drop('YearMonth')

stock_daily = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Stock/YearMonth="+cdm)
lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
branch = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Branch")
item = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Item")
Godowndf = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Godown")
POQTY = sqlctx.read.parquet(hdfspath+"/Market/Stage2/PendingPO")
GIT = sqlctx.read.parquet(hdfspath+"/Market/Stage2/GIT/YearMonth="+cdm)
PC = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Pending_Challan/YearMonth="+cdm)
AQ = sqlctx.read.parquet(hdfspath+"/Market/Stage2/AQ_PQ")
PQ = sqlctx.read.parquet(hdfspath+"/Market/Stage2/AQ_PQ")

sales = sales.filter((sales['vouch_date'] >= Sdate) & (sales['vouch_date'] <= Edate))
# sales.show()
# exit()
sales = sales.groupby('Branch_Code','lot_code').agg({'Sales_Quantity':'sum',"Total_GST_Amount_Paid":'sum',"NET_SALE_VALUE":'sum'})
sales = sales.withColumnRenamed('sum(Sales_Quantity)','Sales_Quantity').withColumnRenamed('sum(Total_GST_Amount_Paid)','Total_GST_Amount_Paid').withColumnRenamed('sum(NET_SALE_VALUE)','NET_SALE_VALUE')

stock_daily = stock_daily.filter(stock_daily['date_'] == Edate)

GIT = GIT.filter(GIT['Date'] == Edate)
GIT = GIT.select('item_det_code','GIT')
GIT = GIT.groupby('item_det_code').agg({'GIT':'sum'})
GIT = GIT.withColumnRenamed('sum(GIT)','IN_TRANSIT_Quantity')

PC = PC.filter(PC['vouch_date'] == Edate)
PC = PC.select('item_det_code','Tot_Qty','Branch_Code')
PC = PC.groupby('item_det_code','Branch_Code').agg({'Tot_Qty':'sum'})
PC = PC.withColumnRenamed('sum(Tot_Qty)','Pending_Challan')
PC = PC.withColumn('Key',concat_ws('_','Branch_Code','item_det_code'))
PC = PC.drop('Branch_Code','item_det_code')

AQ = AQ.select('Item_Det_Code','Allocated_Qty')
AQ = AQ.groupby('Item_Det_Code').agg({'Allocated_Qty':'sum'})
AQ = AQ.withColumnRenamed('sum(Allocated_Qty)','AllocatedQuantity')

PQ = PQ.select('Item_Det_Code_1','Packed_Qty')
PQ = PQ.withColumnRenamed('Item_Det_Code_1','Item_Det_Code')
PQ = PQ.groupby('Item_Det_Code').agg({'Packed_Qty':'sum'})
PQ = PQ.withColumnRenamed('sum(Packed_Qty)','PackQuantity')

if(POQTY.count()):
    POQTY = POQTY.select('Item_Det_Code','QTY')
    POQTY = POQTY.groupby('Item_Det_Code').agg({'QTY':'sum'})
    POQTY = POQTY.withColumnRenamed('sum(QTY)','Pending_PO_Quantity')

                                        
if(Branch != 'ALL'):
    branch = branch.filter(branch['Branch_Name'].isin(Branch))
if(Branch_Group != 'ALL'):
    branch = branch.filter(branch['Group_Name'].isin(Branch_Group))
if(Department != 'ALL'):
    item = item.filter(item['DEPARTMENT'].isin(Department))
if(New_Company_name != 'ALL'):
    item = item.filter(item['NEW_COMPANY_NAME'].isin(New_Company_name))
if(Category != 'ALL'):
    item = item.filter(item['CATEGORY'].isin(Category))
if(Godown != 'ALL'):
    Godowndf = Godowndf.filter(Godowndf['godown_name'].isin(Godown))
    Godownlist = list(set(Godowndf.select('godown_code').toPandas()['godown_code']))
    print(Godownlist)
    stock_daily = stock_daily.filter(stock_daily['Godown_Code'].isin(Godownlist))
if(Local_import != 'ALL'):
    item = item.filter(item['IMP_LOCAL'].isin(Local_import))
if(SubCategory != 'ALL'):
    item = item.filter(item['SUB_CATEGORY'].isin(SubCategory))
if(GodownGroup != 'ALL'):
    Godowndf = Godowndf.filter(Godowndf['Group_Name'].isin(GodownGroup))
    Godownlist = list(set(Godowndf.select('godown_code').toPandas()['godown_code']))
    print(Godownlist)
    stock_daily = stock_daily.filter(stock_daily['Godown_Code'].isin(Godownlist))
if(ItemCode != 'ALL'):
    item = item.filter(item['Item_Code'].isin(ItemCode))

stock_daily = stock_daily.groupby('date_','Branch_Code','Item_Det_Code').agg({'CQTY':'sum','CLOSING_VALUE_ON_CP_Lot':'sum','CLOSING_VALUE_ON_SP_Lot':'sum'})
stock_daily = stock_daily.withColumnRenamed('sum(CQTY)','CQTY').withColumnRenamed('sum(CLOSING_VALUE_ON_CP_Lot)','CLOSING_VALUE_ON_CP_Lot').withColumnRenamed('sum(CLOSING_VALUE_ON_SP_Lot)','CLOSING_VALUE_ON_SP_Lot')

branch_5002 = branch.filter(branch['Branch_Code'] >= 5002)
branch_5002 = list(branch_5002.select('Branch_Name').toPandas()['Branch_Name'])
branch_5002_QTY = branch.filter(branch['Branch_Code'] >= 5002)
branch_5002_QTY = list(branch_5002_QTY.select('Branch_Name').toPandas()['Branch_Name'])
branch_PC = branch.filter(branch['Branch_Code'] >= 5002)
branch_PC = list(branch_PC.select('Branch_Name').toPandas()['Branch_Name'])
branch_all = list(branch.select('Branch_Name').toPandas()['Branch_Name'])

sales = sales.join(lot, 'lot_code', how = 'inner')
sales = branch.join(sales, 'Branch_Code', how = 'left')
sales = item.join(sales, 'Item_Det_Code', how = 'left')
sales.cache()

stock_daily = branch.join(stock_daily, 'Branch_Code', how = 'left')
stock_daily = item.join(stock_daily, 'Item_Det_Code', how = 'left')
stock_daily.cache()

stock_without_branch = stock_daily.groupby('Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name').agg({"CLOSING_VALUE_ON_CP_Lot":'sum',"CLOSING_VALUE_ON_SP_Lot":'sum'})
stock_without_branch = stock_without_branch.withColumnRenamed('sum(CLOSING_VALUE_ON_CP_Lot)','CLOSING_VALUE_ON_CP_Lot').withColumnRenamed('sum(CLOSING_VALUE_ON_SP_Lot)','CLOSING_VALUE_ON_SP_Lot')
stock_without_branch = stock_without_branch.withColumn('Key', concat_ws("_",'Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name'))
stock_without_branch = stock_without_branch.drop('Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name')
stock_daily = stock_daily.select('date_','Branch_Code','Item_Det_Code','CQTY')
item = item.drop('Pur_Rate','Sale_Rate','MRP','Basic_Rate')
sales_without_branch = sales.join(GIT, 'Item_Det_Code', how = 'left')
sales_without_branch = sales_without_branch.join(POQTY, 'Item_Det_Code', how = 'left')
sales_without_branch = sales_without_branch.join(AQ, 'Item_Det_Code', how = 'left')
sales_without_branch = sales_without_branch.join(PQ, 'Item_Det_Code', how = 'left')
sales_without_branch = sales_without_branch.groupby('Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name').agg({"IN_TRANSIT_Quantity":'avg',"Total_GST_Amount_Paid":'sum',"Pur_Rate":'avg',"Sale_Rate":'avg',"Basic_Rate":'avg',"MRP":'avg',"Pending_PO_Quantity":'avg',"NET_SALE_VALUE":'sum',"mrp_lot":'avg',"pur_rate_lot":'avg','AllocatedQuantity':'avg','PackQuantity':'avg'})
sales_without_branch = sales_without_branch.withColumnRenamed('sum(Total_GST_Amount_Paid)','Total_GST_Amount_Paid').withColumnRenamed('avg(Pur_Rate)','Pur_Rate').withColumnRenamed('avg(Sale_Rate)','Sale_Rate').withColumnRenamed('avg(Basic_Rate)','Basic_Rate').withColumnRenamed('avg(MRP)','MRP').withColumnRenamed('avg(Pending_PO_Quantity)','Pending_PO_Quantity').withColumnRenamed('avg(IN_TRANSIT_Quantity)','IN_TRANSIT_Quantity').withColumnRenamed('sum(NET_SALE_VALUE)','NET_SALE_VALUE').withColumnRenamed('avg(mrp_lot)','MRP_Lot').withColumnRenamed('avg(pur_rate_lot)','Pur_Rate_Lot').withColumnRenamed('avg(AllocatedQuantity)','AllocatedQuantity').withColumnRenamed('avg(PackQuantity)','PackQuantity')
sales_without_branch = sales_without_branch.withColumn('Key', concat_ws("_",'Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name'))
sales_without_branch = sales_without_branch.drop('Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name')
sales_without_branch.cache()
sales = sales.select('Item_Det_Code','Branch_Code','Sales_Quantity')
sales = sales.groupby('Item_Det_Code','Branch_Code').agg({'Sales_Quantity':'sum'})
sales = sales.withColumnRenamed('sum(Sales_Quantity)','Sales_Quantity')
sales = sales.withColumn('Key',concat_ws('_','Branch_Code','Item_Det_Code'))
sales = sales.drop('Branch_Code','Item_Det_Code')

branch = branch.withColumn('a',lit('a'))
item = item.withColumn('a',lit('a'))
master = branch.join(item,'a','left')
branch = branch.drop('a')
item = item.drop('a')
master = master.withColumn('Key',concat_ws('_','Branch_Code','Item_Det_Code'))
stock_daily = stock_daily.withColumn('Key',concat_ws('_','Branch_Code','Item_Det_Code'))
stock_daily = stock_daily.drop('Branch_Code','Item_Det_Code')
stock_max_date = master.join(stock_daily,'Key','left')
stock_max_date = stock_max_date.join(sales,'Key','left')
stock_max_date = stock_max_date.join(PC,'Key','left')
stock_max_date.cache()
print(stock_max_date.count())

stock_max_date = stock_max_date.groupby('Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name').agg({"Sales_Quantity":'sum','CQTY':'sum','Pending_Challan':'sum'})
stock_max_date = stock_max_date.withColumnRenamed('sum(Sales_Quantity)','Sales_Quantity').withColumnRenamed('sum(CQTY)','Stock-Available').withColumnRenamed('sum(Pending_Challan)','Pending_Challan')

stock_max_date = stock_max_date.withColumn('Key', concat_ws("_",'Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price_Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Company_Name'))
stock_max_date = stock_max_date.join(sales_without_branch, 'Key', how = 'left')
stock_max_date = stock_max_date.join(stock_without_branch, 'Key', how = 'left')
stock_max_date = stock_max_date.drop('date_','Key')
stock_max_date = stock_max_date.filter((stock_max_date['Sales_Quantity'].isNotNull()) | (stock_max_date['Stock-Available'].isNotNull()) | (stock_max_date['Pending_PO_Quantity'].isNotNull()) | (stock_max_date['IN_TRANSIT_Quantity'].isNotNull()) | (stock_max_date['Pending_Challan'].isNotNull()))
stock_max_date.cache()
print(stock_max_date.count())
branch = list(set(stock_max_date.select('Branch_Name').toPandas()['Branch_Name']))
stock_max_date = stock_max_date.groupBy('Company_Name', 'DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS', 'Item_Name','Item_Code', 'Price_Range','Pur_Rate','Pur_Rate_Lot','Sale_Rate','MRP','MRP_Lot','Basic_Rate', 'Total_GST_Amount_Paid','Pending_PO_Quantity','IN_TRANSIT_Quantity','NET_SALE_VALUE','CLOSING_VALUE_ON_CP_Lot','CLOSING_VALUE_ON_SP_Lot').pivot('Branch_Name').agg(sumf("Sales_Quantity").alias("Sales_Quantity"),sumf("Stock-Available").alias("Stock-Available"),sumf("Pending_Challan").alias("Pending_Challan"),sumf("AllocatedQuantity").alias("AllocatedQuantity"),sumf("PackQuantity").alias("PackQuantity"))

branch_all = [x for x in branch_all if x in branch]
branch_all = [x + "_Stock-Available" for x in branch_all]
branch_5002 = [x for x in branch_5002 if x in branch]
branch_5002 = [x + "_Stock-Available" for x in branch_5002]
branch_5002_QTY = [x for x in branch_5002_QTY if x in branch]
branch_5002_QTY = [x + "_Sales_Quantity" for x in branch_5002_QTY]
branch_PC = [x for x in branch_PC if x in branch]
branch_PC = [x + "_Pending_Challan" for x in branch_PC]
stock_max_date.cache()
print(stock_max_date.count())
stock_max_date = stock_max_date.fillna(0)
if(stock_max_date.count() != 0):
    stock_max_date = stock_max_date.withColumn('Total_Store_Stock', reduce(column_add, ( stock_max_date[col] for col in branch_5002)))
    stock_max_date = stock_max_date.withColumn('Total_Closing_Stock', reduce(column_add, ( stock_max_date[col] for col in branch_all)))
    stock_max_date = stock_max_date.withColumn('NET_SALE', reduce(column_add, ( stock_max_date[col] for col in branch_5002_QTY)))
    stock_max_date = stock_max_date.withColumn('Pending_Challan_Total', reduce(column_add, ( stock_max_date[col] for col in branch_PC)))
    stock_max_date = stock_max_date.withColumn('Net_SALE_Cost_Value', stock_max_date['NET_SALE'] * stock_max_date['Pur_Rate'])
    stock_max_date = stock_max_date.withColumn('CLOSING_VALUE_ON_CP', stock_max_date['Total_Closing_Stock'] * stock_max_date['Pur_Rate'])
    stock_max_date = stock_max_date.withColumn('CLOSING_VALUE_ON_SP', stock_max_date['Total_Closing_Stock'] * stock_max_date['MRP'])
    stock_max_date = stock_max_date.withColumn('PENDING_CHALLAN_Value_on_SP', stock_max_date['Pending_Challan_Total'] * stock_max_date['MRP'])
    stock_max_date = stock_max_date.withColumn('IN_TRANSIT_Value_SP', stock_max_date['IN_TRANSIT_Quantity'] * stock_max_date['MRP'])
    stock_max_date = stock_max_date.withColumn('Total_PENDING_PO_Value', stock_max_date['Pending_PO_Quantity'] * stock_max_date['MRP'])
    stock_max_date = stock_max_date.withColumn('Net_SALE_Cost_Value_lot', stock_max_date['NET_SALE'] * stock_max_date['Pur_Rate_Lot'])
    stock_max_date = stock_max_date.withColumn('Over_All_Closing_Stock_Qty', stock_max_date['Pending_Challan_Total'] + stock_max_date['Total_Closing_Stock'] + stock_max_date['IN_TRANSIT_Quantity'])
    stock_max_date = stock_max_date.withColumn('Over_All_Closing_Stock_Value_on_SP', stock_max_date['CLOSING_VALUE_ON_SP'] + stock_max_date['PENDING_CHALLAN_Value_on_SP'] + stock_max_date['IN_TRANSIT_Value_SP'])
    stock_max_date = stock_max_date.withColumn('Avg_Sale_Qty_per_Day', stock_max_date['NET_SALE']/30)
    stock_max_date = stock_max_date.withColumn('Stock_Days', stock_max_date['Over_All_Closing_Stock_Qty'] / stock_max_date['Avg_Sale_Qty_per_Day'])
    mail(stock_max_date,Sdate,Edate)
    if(load == 'APP'):
        df_jobs = read_data_sql("(SELECT JobId , UserId , Args , SubmittedOn , StartedOn , EndedOn , JobStatus FROM [KOCKPIT].[dbo].[_Jobs]) as jobs")
        df_jobs.sort('JobId')
        df_jobs = df_jobs.withColumn('EndedOn',when(df_jobs['JobId'] == jobid,datetime.now()).otherwise(df_jobs['EndedOn']))
        df_jobs = df_jobs.withColumn('JobStatus',when(df_jobs['JobId'] == jobid,'DONE').otherwise(df_jobs['JobStatus']))
        df_jobs.cache()
        print(df_jobs.count())
        write_data_sql(df_jobs,"_Jobs",owmode)
else:
    df_jobs = read_data_sql("(SELECT JobId , UserId , Args , SubmittedOn , StartedOn , EndedOn, JobStatus FROM [KOCKPIT].[dbo].[_Jobs]) as jobs")
    df_jobs.sort('JobId')
    df_jobs = df_jobs.withColumn('EndedOn',when(df_jobs['JobId'] == jobid,datetime.now()).otherwise(df_jobs['EndedOn']))
    df_jobs = df_jobs.withColumn('JobStatus',when(df_jobs['JobId'] == jobid,'No Data').otherwise(df_jobs['JobStatus']))
    df_jobs.cache()
    print(df_jobs.count())
    write_data_sql(df_jobs,"_Jobs",owmode)