from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import sum as sumf, first as firstf
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window

def mail(df,Branch_Name):
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login("automailer@market99.com", "Mailer@M99")
    sender = "automailer@market99.com"
    #recipient = ["ravi.sharma@market99.com"]
    #Cc = ["abhishek.p@kockpit.in","ruchir.vasishth@kockpit.in","rahul.khanna@kockpit.in"]
    recipient = ["rahul.khanna@kockpit.in"]
    Cc = ["rahul.khanna@kockpit.in"]
    msg = MIMEMultipart()
    msg['Subject'] = 'MER Report for ' + Branch_Name + " Branches"
    msg['From'] = sender
    msg['To'] = ', '.join(recipient)
    msg['Cc'] = ', '.join(Cc)
    msg.attach(MIMEText('Hi User, \n\nPlease find attached Sales and Stock Available for December 2020 \n\n Thanks, \n Auto Mailer(KOCKPIT)'))
    part = MIMEApplication(df.toPandas().to_excel(), Name="Sales.xlsx")
    part['Content-Disposition'] = 'attachment; filename="%s"' % 'Sales_Stock.xlsx'
    msg.attach(part)
    server.sendmail(sender, (recipient + Cc) , msg.as_string())
    server.close()
    
#For reading data from the SQL server
def read_data_sql(table_string):
    database = "LOGICDBS99"
    user = "sa"
    password  = "M99@321"
    SQLurl = "jdbc:sqlserver://MARKET-NINETY3\MARKET99BI;databaseName="+database
    SQLprop= {"user":user,"password":password}
    df = spark.read.jdbc(url = SQLurl , table = table_string, properties = SQLprop)
    return df

#Intializing Spark Session
conf = SparkConf().setMaster("local[*]").setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()

sales = read_data_sql("(SELECT sh.vouch_date, sh.Branch_Code, st.item_det_code, st.lot_code, st.Tot_Qty from sl_head20202021 sh left join sl_txn20202021 st on sh.vouch_code = st.vouch_code where sh.vouch_date between '2020-12-01' and '2020-12-31') as sh")
sales = sales.where(col("vouch_date").isNotNull())
sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
sales.cache()
sales = sales.groupby('Branch_Code','lot_code').agg({"Tot_Qty":'sum'})
sales = sales.withColumnRenamed('sum(Tot_Qty)','Sales_Quantity')

stock = read_data_sql("(SELECT date_,Branch_Code,lot_code,net_qty FROM stk_dtxn20202021 ) as stock")
stock = stock.groupby('date_','Branch_Code','lot_code').agg(sumf('net_qty').alias('Stock_Quantity'))
stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
stock.cache()

lot = read_data_sql("(SELECT item_det_code as Item_Det_Code,lot_code from Lot_Mst) as lot")

branch = read_data_sql("(Select bm.Branch_Name, bm.Branch_Code from Branch_Mst as bm inner join Branch_Groups as bg on bm.Group_Code1 = bg.Group_Code) as branch")
branch = branch.filter(branch.Branch_Code>=0)
branch.cache()

item = read_data_sql("(Select  imh.item_hd_code,imd.Item_Det_Code, imd.User_Code as Item_Code, imn.Item_Name from It_Mst_O_Det imod inner join It_Mst_Det imd on imod.Code = imd.Item_O_Det_Code inner join It_Mst_Hd imh on imh.item_hd_code = imd.Item_Hd_Code inner join It_Mst_Names imn on imn.Item_Name_Code = imh.Item_Name_Code) as item ")

GM = read_data_sql("(Select group_code,group_name,level_h from  Group_Mst where group_name != '(NIL)') as gm")
GM = GM.withColumn("level_h",GM['level_h'].cast(IntegerType()))
IMH = read_data_sql("(Select item_hd_code, group_code as Group_Code1, Group_Code2, Group_Code3, Group_Code4, Group_Code7, Group_Code8, Group_Code11, Group_Code_12 as Group_Code12, Group_Code16, Group_Code20, Group_Code21 from  It_Mst_Hd) as imh")
cond= [(IMH["Group_Code1"]==GM["group_code"]) | (IMH["Group_Code2"]==GM["group_code"]) | (IMH["Group_Code3"]==GM["group_code"]) | (IMH["Group_Code4"]==GM["group_code"]) | (IMH["Group_Code7"]==GM["group_code"]) | (IMH["Group_Code8"]==GM["group_code"]) | (IMH["Group_Code11"]==GM["group_code"]) | (IMH["Group_Code12"]==GM["group_code"]) | (IMH["Group_Code16"]==GM["group_code"]) | (IMH["Group_Code20"]==GM["group_code"]) | (IMH["Group_Code21"]==GM["group_code"])]
GM = GM.join(IMH,cond,"inner")
GM = GM.drop("group_code","Group_Code1","Group_Code2","Group_Code3","Group_Code4","Group_Code7","Group_Code8","Group_Code11","Group_Code12","Group_Code16","Group_Code20","Group_Code21")
GM = GM.groupBy('item_hd_code').pivot('level_h').agg(firstf("group_name").alias("Group_Name"))
GM = GM.withColumnRenamed("1","DEPARTMENT").withColumnRenamed("2","CATEGORY").withColumnRenamed("3","SUB_CATEGORY").withColumnRenamed("4","MATERIAL").withColumnRenamed("7","Price Range").withColumnRenamed("8","GST_CATEGORY").withColumnRenamed("11","ACTIVE_INACTIVE").withColumnRenamed("12","IMP_LOCAL").withColumnRenamed("16","LAUNCH_MONTH").withColumnRenamed("20","NEW_COMPANY_NAME").withColumnRenamed("21","SUB_CLASS")

item = item.join(GM, 'item_hd_code', 'left')
item = item.drop('item_hd_code')

stock = stock.join(lot, 'lot_code', how = 'left')
stock = stock.join(branch, 'Branch_Code', how = 'left')
stock = stock.join(item, 'Item_Det_Code', how = 'left')

sales = sales.join(lot, 'lot_code', how = 'left')
sales = sales.join(branch, 'Branch_Code', how = 'left')
sales = sales.join(item, 'Item_Det_Code', how = 'left')
sales = sales.groupby('Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS').agg({"Sales_Quantity":'sum'})
sales = sales.withColumnRenamed('sum(Sales_Quantity)','Sales_Quantity')
sales = sales.withColumn('Key', concat_ws("_",'Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS'))
sales = sales.drop('Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS')
sales.show()

my_window = Window.partitionBy('Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS').orderBy("date_")
stock = stock.withColumn('Stock-Available', sumf('Stock_Quantity').over(my_window))

stock = stock.filter((stock['date_'] >= '2020-12-01') & (stock['date_'] <= '2020-12-31'))
stock = stock.drop('Branch_Code','lot_code','Item_Det_Code','Stock_Quantity')
stock = stock.withColumn('Key', concat_ws("_",'date_','Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS'))
stock.show()

stock_max_date = stock.groupby('Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS').agg({"date_":'max'})
stock_max_date = stock_max_date.withColumn('Key', concat_ws("_",'max(date_)','Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS'))
stock_max_date = stock_max_date.drop('Branch_Name','Item_Name','max(date_)','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS')
stock_max_date = stock_max_date.join(stock, 'Key', how = 'left')
stock_max_date = stock_max_date.withColumn('Key', concat_ws("_",'Branch_Name','Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS'))
stock_max_date = stock_max_date.join(sales, 'Key', how = 'left')
stock_max_date = stock_max_date.drop('date_','Key')
stock_max_date.show()

stock_max_date = stock_max_date.groupBy('Item_Name','Item_Code','DEPARTMENT', 'CATEGORY', 'SUB_CATEGORY', 'MATERIAL', 'Price Range', 'GST_CATEGORY', 'ACTIVE_INACTIVE', 'IMP_LOCAL', 'LAUNCH_MONTH', 'NEW_COMPANY_NAME', 'SUB_CLASS').pivot('Branch_Name').agg(sumf("Sales_Quantity").alias("Sales_Quantity"),sumf("Stock-Available").alias("Stock-Available"))
mail(stock_max_date, "ALL")