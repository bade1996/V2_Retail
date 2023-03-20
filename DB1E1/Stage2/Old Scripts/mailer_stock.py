from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType,IntegerType
from pyspark.sql.functions import sum as sumf,first as firstf
import smtplib,os
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import datetime

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
conf = SparkConf().setMaster('local[*]').setAppName("mailer").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()

present = datetime.datetime.now()

if present.month<=3:
    year1=str(present.year-1)
    year2=str(present.year)
    presentFY=year1+year2
else:
    year1=str(present.year)
    year2=str(present.year+1)
    presentFY=year1+year2

if(os.system("hadoop fs -ls /KOCKPIT/M99/Stock") == 256):
    GM = read_data_sql("(Select group_code,group_name,level_h from  Group_Mst where group_name != '(NIL)') as gm")
    GM = GM.withColumn("level_h",GM['level_h'].cast(IntegerType()))
    IMH = read_data_sql("(Select item_hd_code, group_code as Group_Code1, Group_Code2, Group_Code3, Group_Code4, Group_Code7, Group_Code8, Group_Code11, Group_Code_12 as Group_Code12, Group_Code16, Group_Code20, Group_Code21 from  It_Mst_Hd) as imh")
    cond= [(IMH["Group_Code1"]==GM["group_code"]) | (IMH["Group_Code2"]==GM["group_code"]) | (IMH["Group_Code3"]==GM["group_code"]) | (IMH["Group_Code4"]==GM["group_code"]) | (IMH["Group_Code7"]==GM["group_code"]) | (IMH["Group_Code8"]==GM["group_code"]) | (IMH["Group_Code11"]==GM["group_code"]) | (IMH["Group_Code12"]==GM["group_code"]) | (IMH["Group_Code16"]==GM["group_code"]) | (IMH["Group_Code20"]==GM["group_code"]) | (IMH["Group_Code21"]==GM["group_code"])]
    GM = GM.join(IMH,cond,"inner")
    GM = GM.drop("group_code","Group_Code1","Group_Code2","Group_Code3","Group_Code4","Group_Code7","Group_Code8","Group_Code11","Group_Code12","Group_Code16","Group_Code20","Group_Code21")
    GM = GM.groupBy('item_hd_code').pivot('level_h').agg(firstf("group_name").alias("Group_Name"))
    GM = GM.withColumnRenamed("1","DEPARTMENT").withColumnRenamed("2","CATEGORY").withColumnRenamed("3","SUB_CATEGORY").withColumnRenamed("4","MATERIAL").withColumnRenamed("7","Price Range").withColumnRenamed("8","GST_CATEGORY").withColumnRenamed("11","ACTIVE_INACTIVE").withColumnRenamed("12","IMP_LOCAL").withColumnRenamed("16","LAUNCH_MONTH").withColumnRenamed("20","NEW_COMPANY_NAME").withColumnRenamed("21","SUB_CLASS")
    GM.show()
else:
    print("no")