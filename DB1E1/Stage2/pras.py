from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.functions import sum as sumf, first as firstf,year as yearf , month as monthf,when, substring as suf
from pyspark.sql.types import *
import smtplib,sys
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from pyspark.sql.window import Window
import os,pandas as pd
from datetime import datetime, timedelta

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

config = os.path.dirname(os.path.realpath(__file__))
path = config = config[0:config.rfind("DB")]
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

#Intializing Spark Session
conf = SparkConf().setMaster(smaster).setAppName("DO").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.executor.memory", "64g").config("spark.driver.memory", "64g")\
                            .config("spark.sql.broadcastTimeout", "1800")\
                            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
sqlctx = SQLContext(sc)
print(datetime.now())

DO = sqlctx.read.parquet(hdfspath+"/Market/Stage1/DO")
lot = sqlctx.read.parquet(hdfspath+"/Market/Stage2/Lot")
DO = DO.select('vouch_Code','vouch_date','Godown_Code','Branch_Code','Code','Lot_Code','Quantity','Link_Code')
lot = lot.select('lot_code','Item_Det_Code')
DO = DO.join(lot,'lot_code','left')

DO_temp = DO.select('Link_Code')
DO_temp = DO_temp.filter(DO_temp['Link_Code'] != 0)
DO_temp = DO_temp.withColumnRenamed('Link_Code','code')
DO_temp = DO_temp.withColumn('Flag',lit('Found'))
DO = DO.join(DO_temp,'code','left')
DO_A = DO.where(col("Flag").isNull())
DO_A.filter(DO_A['Vouch_Date'] =='2021-05-28').show()
# exit()
DO_P = DO.where(col("Flag").isNotNull())
DO_P.filter(DO_P['Vouch_Date'] =='2021-05-28').show()
exit()
# .withColumn('CancelPOQty',when((PO['Cancel_Item']== 1),PO['Pend_Qty']).otherwise(0))
# DO_A = DO.where(col("Link_Code").isNull())
# DO_A.show()
# print(DO_A.count())
# exit()
# DO_P = DO.where(col("Link_Code").isNotNull())

print(DO_A.count())
print(DO_P.count())
exit()
#DO_A.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/AllocatedQuantity")
write_data_sql(DO_A,"Aq",owmode)
#DO_P.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/PackQuantity")
write_data_sql(DO_P,"Pq",owmode)