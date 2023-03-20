#import findspark
#findspark.init()

import pyspark
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
    
def RenameDuplicateColumns(dataframe):
    columns = dataframe.columns
    column_indices = [(columns[idx], idx) for idx in range(len(columns))]
    dict = {}
    newNames = []
    for cl in column_indices:
        if cl[0] not in dict:
            dict[cl[0]] = []
        else:
            dict[cl[0]].append(cl[1])

        if len(dict[cl[0]]) == 0:
            newNames.append(cl[0])
        else:
            newName = cl[0] + '_' + str(len(dict[cl[0]]))
            newNames.append(newName)
    dataframe = dataframe.toDF(*newNames)
    return dataframe

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


Sql_query="(select Code,Quantity,Pending_Qty as Allocated_Qty,Vouch_Code,Lot_Code,Deleted_ from DO_Txn) as c"
Do_Txn=read_data_sql(Sql_query)

Lot=read_data_sql("(select Lot_Code,Item_Det_Code from Lot_Mst) as d")
Do_Txn=Do_Txn.join(Lot,"Lot_Code",how="left")
Do_Txn_LC = Do_Txn.filter(Do_Txn['Deleted_'] == 0)\
                .filter(Do_Txn['Lot_Code']==0)
                
Do_Txn_query="(select Code,Link_Code,Quantity as Rem_Qty,Pending_Qty as Packed_Qty,Vouch_Code,Lot_Code,Deleted_ from DO_Txn) as c"
Do_Txn_N=read_data_sql(Do_Txn_query)
Do_Txn_N=Do_Txn_N.join(Lot,"Lot_Code",how="left")          
Do_Txn_LCN = Do_Txn_N.filter(Do_Txn_N['Deleted_'] == 0)\
                .filter(Do_Txn_N['Lot_Code']!=0)

do_head_query="(select Vouch_Number,Vouch_Date,Vouch_Code,Cust_Code from do_head)as c"
do_head=read_data_sql(do_head_query)
act_query="(select act_code,ST_Branch_Code from [dbo].[Accounts])as c"
act=read_data_sql(act_query)
#act = act.filter(act['ST_Branch_Code'] == 5032)
Do_act=act.join(do_head,act["act_Code"]==do_head["Cust_Code"],how="left")
Txn_act_LC=Do_act.join(Do_Txn_LC,"Vouch_Code",how="left")

print(Txn_act_LC.count())#37
Txn_act_LCN=Do_act.join(Do_Txn_LCN,"Vouch_Code",how="left")

Packed_Qty=Txn_act_LC.join(Txn_act_LCN,Txn_act_LC["Code"]==Txn_act_LCN["Link_Code"],how="left")

# Packed_Qty = Packed_Qty.filter(Packed_Qty['Link_Code'] == 4304)
Packed_Qty.show()
Packed_Qty=RenameDuplicateColumns(Packed_Qty)
Packed_Qty.show()
Packed_Qty.coalesce(1).write.mode("overwrite").save(hdfspath+"/Market/Stage2/AQ_PQ")
write_data_sql(Packed_Qty, 'AQ_PQ', 'overwrite')
exit()
# Packed_Qty.to_Pandas()
# Packed_Qty.to_csv('Qty.csv')
exit()
print(Txn_act_LCN.count())#37
print("Ater join")

exit()
Txn_act = Txn_act.filter(Txn_act['Deleted_'] == 0)\
                .filter(Txn_act['Lot_Code']==0)
# Txn_act.show()
print(Txn_act.count())#22
print("After Filter")



