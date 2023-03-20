from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType
from pyspark.sql.functions import sum as sumf
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
    #Cc = ["abhishek.p@kockpit.in","rahul.khanna@kockpit.in"]
    recipient = ["rahul.khanna@kockpit.in"]
    Cc = ["rahul.khanna@kockpit.in"]
    msg = MIMEMultipart()
    msg['Subject'] = 'Stock and Sales for ' + Branch_Name + " Branch"
    msg['From'] = sender
    msg['To'] = ', '.join(recipient)
    msg['Cc'] = ', '.join(Cc)
    msg.attach(MIMEText('Hi User, \n\nPlease find attached Date and Item wise Sales and Stock Available for ' + Branch_Name + '\n \n Thanks, \n Auto Mailer(KOCKPIT)'))
    part = MIMEApplication(df.toPandas().to_csv(), Name="Sales.csv")
    part['Content-Disposition'] = 'attachment; filename="%s"' % 'Sales_Stock.csv'
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
conf = SparkConf().setMaster("spark://103.248.60.14:7077").setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()
def Branch_Wise(Branch_Code,Branch_Name):
    sales = read_data_sql("(SELECT sh.vouch_date, sh.net_amt, sh.Branch_Code, st.item_det_code, st.lot_code, st.Tot_Qty from sl_head20202021 sh left join sl_txn20202021 st on sh.vouch_code = st.vouch_code where sh.Branch_Code = '"+str(Branch_Code)+"') as sh")
    sales = sales.where(col("vouch_date").isNotNull())
    sales = sales.withColumn("vouch_date",sales['vouch_date'].cast(DateType()))
    sales.cache()
    sales = sales.groupby('vouch_date','Branch_Code','lot_code').agg({"Tot_Qty":'sum', "net_amt":'sum'})
    sales = sales.withColumnRenamed('sum(net_amt)','Amount').withColumnRenamed('sum(Tot_Qty)','Sales_Quantity')
    sales.cache()
    sales = sales.withColumn('Key', concat_ws("_",'vouch_date','Branch_Code','lot_code'))
    
    stock = read_data_sql("(SELECT date_,Branch_Code,lot_code,net_qty FROM stk_dtxn20202021 where Branch_Code = '"+str(Branch_Code)+"') as stock")
    stock = stock.groupby('date_','Branch_Code','lot_code').agg(sumf('net_qty').alias('Stock_Quantity'))
    stock = stock.withColumn("date_",stock['date_'].cast(DateType()))
    stock.cache()
    stock = stock.withColumn('Key', concat_ws("_",'date_','Branch_Code','lot_code'))
    
    lot = read_data_sql("(SELECT item_det_code as Item_Det_Code,lot_code from Lot_Mst) as lot")
    
    branch = read_data_sql("(Select bm.Branch_Name, bm.Branch_Code from Branch_Mst as bm inner join Branch_Groups as bg on bm.Group_Code1 = bg.Group_Code where bm.Branch_Code = '"+str(Branch_Code)+"') as branch")
    branch = branch.filter(branch.Branch_Code>=0)
    branch.cache()
    
    item = read_data_sql("(Select  imd.Item_Det_Code, imn.Item_Name from It_Mst_O_Det imod inner join It_Mst_Det imd on imod.Code = imd.Item_O_Det_Code inner join It_Mst_Hd imh on imh.item_hd_code = imd.Item_Hd_Code inner join It_Mst_Names imn on imn.Item_Name_Code = imh.Item_Name_Code) as item ")
    
    stock = stock.join(lot, 'lot_code', how = 'left')
    stock = stock.join(branch, 'Branch_Code', how = 'left')
    stock = stock.join(item, 'Item_Det_Code', how = 'left')
    stock = stock.drop('date_','Branch_Code','lot_code','Item_Det_Code')
    sales_stock = stock.join(sales, 'Key', how = 'left')
    
    sales_stock = sales_stock.groupby('vouch_date','Branch_Name','Item_Name').agg({"Sales_Quantity":'sum',"Amount":'sum' ,"Stock_Quantity":'sum'})
    sales_stock = sales_stock.withColumnRenamed('sum(Stock_Quantity)','Stock_Quantity').withColumnRenamed('sum(Sales_Quantity)','Sales_Quantity').withColumnRenamed('sum(Amount)','Amount')
    sales_stock = sales_stock.fillna({'vouch_date':'2020-03-31'})
    my_window = Window.partitionBy('Branch_Name','Item_Name').orderBy("vouch_date")
    sales_stock = sales_stock.withColumn('Stock-Available', sumf('Stock_Quantity').over(my_window))
    sales_stock = sales_stock.orderBy("vouch_date")
    sales_stock = sales_stock.drop('Amount', 'Stock_Quantity')
    sales_stock = sales_stock.filter(sales_stock['vouch_date'] != '2020-03-31')
    sales_stock = sales_stock.withColumnRenamed('vouch_date','Date')
    mail(sales_stock,Branch_Name)

if __name__ == "__main__":
    branch = read_data_sql("(Select bm.Branch_Name, bm.Branch_Code from Branch_Mst as bm inner join Branch_Groups as bg on bm.Group_Code1 = bg.Group_Code) as branch")
    branch = branch.filter(branch.Branch_Code>=0)
    branch.cache()
    branch = branch.filter(branch['Branch_Name'] == 'AKSHARDHAM')
    for i in range(0,branch.count()):
        Branch_Code = branch.collect()[i][1]
        Branch_Name = branch.collect()[i][0]
        Branch_Wise(Branch_Code,Branch_Name)