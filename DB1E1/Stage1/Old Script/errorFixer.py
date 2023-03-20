#Snapshot of Stock Query script
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,col,month,year,lit,concat,max as max_,min as min_,sum as sum_,datediff,to_date,col,when
import re,os,datetime#,keyring
import time,sys
from pyspark.sql.types import *
import udl
import pandas as pd
from numpy.core._multiarray_umath import empty
from dateutil import relativedelta
from pyspark.storagelevel import StorageLevel   
import psycopg2
import dateutil.relativedelta
import pyspark.sql.functions as F
import pyodbc
#import pandas as pd
#from sqlserverport import lookup
#from Tools.scripts.objgraph import ignore


now = datetime.datetime.now()
stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')

present=datetime.datetime.now()#.strftime('%Y-%m-%d')   ###### PRESENT DATE
# present='2020-04-01'
# present=datetime.datetime.strptime(present,"%Y-%m-%d")
if present.month<=3:
    year1=str(present.year-1)
    year2=str(present.year)
    presentFY=year1+year2
else:
    year1=str(present.year)
    year2=str(present.year+1)
    presentFY=year1+year2

print(present)
CM=present.month    ## CM = CURRENT MONTH
CM = str(CM)
#print(CM)
'''
yr= Datelog.split('-')[0]
oyr=int(yr)-1
newFY=str(oyr)+yr
''
print("CurrentFiscalYr:"+newFY)
'''
if present.month==3:
    rollD=28
else:
    rollD=30

#previous=present-datetime.timedelta(days=rollD)
previous=present-dateutil.relativedelta.relativedelta(months=1)   ############
#previous=present-datetime.timedelta(months=1)
previous = previous.strftime('%Y-%m-%d')
#print(previous)
previous = previous[:-2]
previous=previous+"01"
previous=datetime.datetime.strptime(previous,"%Y-%m-%d")  ######## PREVIOUS MONTH DAY 1
#print(previous)

PM=previous.month  ## PM = PAST MONTH/ PREVIOUS MONTH
PM = str(PM)
#print(PM)
if previous.month<=3:
    year1=str(previous.year-1)
    year2=str(previous.year)
    previousFY=year1+year2
else:
    year1=str(previous.year)
    year2=str(previous.year+1)
    previousFY=year1+year2

past=previous.strftime('%Y-%m-%d')
present=present.strftime('%Y-%m-%d')
pastFY=previousFY

# pastStr="'"+past+"'"
# print("baal"+past)
# print(PM) 
# print(pastFY)
# print(present)
# print(CM)
# print(presentFY)
# exit()

#dquery =  """DELETE FROM market99.StockAllYearSnapshot WHERE "Date_" > """+pastStr

config = os.path.dirname(os.path.realpath(__file__))
DBET = config[config.rfind("DB"):config.rfind("/")]
Etn = DBET[DBET.rfind("E"):]
DB = DBET[:DBET.rfind("E")]
path = config = config[0:config.rfind("DB")]
path = "file://"+path
config = pd.read_csv(config+"/Config/conf.csv")

for i in range(0,len(config)):
    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
    
conf = SparkConf().setMaster(smaster).setAppName("ERRORFIX")\
       .set("spark.sql.shuffle.partitions",60)\
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .set("spark.driver.memory","30g")\
    .set("spark.executor.memory","30g")\
    #.set("spark.executor.cores",23)
    #.set("spark.driver.maxResultSize","4g")
#        .set("spark.driver.cores",4)\
#         .set("spark.sql.debug.maxToStringFields", 9000)\
#         .set("spark.network.timeout", "6000s")

sc = SparkContext(conf = conf)
spark = SparkSession.builder.appName("ERRORFIX").config("spark.network.timeout", "100000001")\
.config("spark.executor.heartbeatInterval", "100000000").getOrCreate()
sqlctx = SQLContext(sc)

Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"

Postgresurl2 = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
Postgresprop2 = {
    "user":"postgres",
    "password":"sa123",
    "driver": "org.postgresql.Driver" 
}

Postgresurl = "jdbc:postgresql://103.248.60.5:5432/kockpit"
Postgresprop= {
    "user":"postgres",
    "password":"sa@123",
    "driver": "org.postgresql.Driver" 
}
## PASSWORD Changed to sa123 for LINUX Postgres
    


######################## MATERIALS OF Stk_Concat SCRIPT #################  MAR 2 20
stk_dtxn = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/stk_dtxn")
stk_dtxn = stk_dtxn.drop_duplicates()
stk_dtxn.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/stk_dtxn2")

print("COCKPIT")

