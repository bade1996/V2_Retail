'''
Created on 15 Nov 2016
@author: Abhishek
'''

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import last_day,month,year,concat,when,regexp_replace,col
from datetime import timedelta, date
import datetime,os,calendar   #keyring
import time,sys
from dateutil.utils import today
import pandas as pd

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()#.strftime('%H:%M:%S')	

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

try:
	config = os.path.dirname(os.path.realpath(__file__))
	DBET = config[config.rfind("DB"):config.rfind("/")]
	Etn = DBET[DBET.rfind("E"):]
	DB = DBET[:DBET.rfind("E")]
	path = config = config[0:config.rfind("DB")]
	path = "file://"+path
	config = pd.read_csv(config+"/Config/conf.csv")
	
	for i in range(0,len(config)):
	    exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
	    
	
	#Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
	Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
	Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    
	conf = SparkConf().setMaster('local[15]').setAppName("Calendar")
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	conf.set("spark.executor.memory","30g")
	conf.set("spark.driver.memory","30g")
	conf.set("spark.driver.cores","16")
	#conf.set("spark.scheduler.mode","FAIR")
	#conf.set("spark.serializer","org.apache.spark.serializer.kyroSerializer")
	sc = SparkContext(conf = conf)

	sqlctx = SQLContext(sc)

	spark = SparkSession.builder.appName("Calendar").getOrCreate()

	####################### DB Credentials  ###########################
	owmode = "overwrite"
	opmode = "append"
	csvpath=os.path.dirname(os.path.realpath(__file__))
	csvpath=csvpath[0:len(csvpath)-3]

	dbentity=csvpath[len(csvpath)-6:len(csvpath)-1]
	dbentity=dbentity.lower()

	#ConfigVariables = sqlctx.read.csv(path=csvpath+"conf\ConfigVariables.csv",header="true")
	#NoofRows= ConfigVariables.count()
	#for i in range(0,NoofRows):
	#	exec(ConfigVariables.select(ConfigVariables.Variable).collect()[i]["Variable"]+"="+ConfigVariables.select(ConfigVariables.Value).collect()[i]["Value"])
	
	Years=3
	MonthStart=4
	
	#connections = sqlctx.read.csv(path=csvpath+"conf\connections.csv",header="true")
	#connections.cache()
	#NoofRows= connections.count()

	#for i in range(0,NoofRows):
	#	exec(connections.select(connections.Variable).collect()[i]["Variable"]+"="+chr(34)+connections.select(connections.Value).collect()[i]["Value"]+chr(34)) 

	path=os.path.realpath(__file__)
	index=path.find("src")
	index=path[index-6:index-1]

	#Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\POC;\
	#databaseName="+SQLDB+";\
	#user=sa;\
	#password=kockpitpoc@12345"
	
	#Configurl="jdbc:sqlserver://localhost:1433;\
	#databaseName="+CONFIGDB+";\
	#user=sa;\
	#password=sa@123"
	
	Postgresurl = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
    
	
	Postgresprop= {
	    "user":"postgres",
	    "password":"sa123",
	    "driver": "org.postgresql.Driver" 
	}
	
	Postgresurl2 = "jdbc:postgresql://103.248.60.5:5432/kockpit"
	Postgresprop2= {
	    "user":"postgres",
	    "password":"sa@123",
	    "driver": "org.postgresql.Driver" 
	}
	## password changed from sa@123  24 feb
	####################### DB Credentials  ###########################
	DB='DB1'
	Company='E1'

	'''
	query= "(SELECT StartDate,EndDate,DBName+CompanyName as Entity FROM [Vw_getcompanyDetails]\
	where CompanyName='"+Company+"' and DBName='"+DB+"') as data1"
	df = sqlctx.read.format("jdbc").options(url=,dbtable=query, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
	'''

	#data_path = csvpath[:csvpath.find("DB")]
	#table = sqlctx.read.parquet(data_path+"\data\CompanyDetails")
	#table = table.withColumn("Entity",concat(table['DBName'],table['CompanyName']))
	
	#df = table.filter(table['DBName'] == DB ).filter(table['EntityName'] == Company)
	
	#company_master = df.select("EntityName","DBName","CompanyName")
	#company_master.write.jdbc(url=Postgresurl, table=dbentity+".company_master", mode=owmode, properties=Postgresprop)
	#df = df.select("StartDate","EndDate","Entity")

	StartDate = '01/04/2018'
	#Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]    
	Calendar_StartDate = datetime.datetime.strptime(StartDate,'%d/%m/%Y').date()
	#StartFile=csvpath[0:len(csvpath)-6]+"CommonBatch\StartFlag.txt"
	
	Calendar_EndDate = str(datetime.datetime.now())
	Calendar_EndDate = Calendar_EndDate.split(' ')[0]
	#print(Calendar_EndDate)
	#sys.exit()
	Calendar_EndDate = datetime.datetime.strptime(Calendar_EndDate,'%Y-%m-%d').date()
	#Calendar_EndDate_conf=df.select(df.EndDate).collect()[0]["EndDate"]
	#Calendar_EndDate_file=datetime.datetime.fromtimestamp(os.path.getmtime(StartFile)).date()
	#Calendar_EndDate=datetime.datetime.fromtimestamp(os.path.getmtime(StartFile)).date()
	#Calendar_EndDate=min(Calendar_EndDate_conf,Calendar_EndDate_file)
	

	if datetime.date.today().month>MonthStart-1:
		UIStartYr=datetime.date.today().year-Years+1
	else:
		UIStartYr=datetime.date.today().year-Years
	UIStartDate=datetime.date(UIStartYr,MonthStart,1)
	Calendar_StartDate=max(Calendar_StartDate,UIStartDate)
	
	#print(Calendar_StartDate)
	#sys.exit()
	
	def daterange(start_date, end_date):
		for n in range(int ((end_date - start_date).days)+1):
			yield start_date + timedelta(n)
	data =[]
	for single_date in daterange(Calendar_StartDate, Calendar_EndDate):
		data.append({'Link_date':single_date})

	schema = StructType([
	StructField("Link_date", DateType(),True)
	])
    
	records=spark.createDataFrame(data,schema)
    
	records=records.select(records.Link_date.alias('Link Date'),month(records.Link_date).alias('Month'),year(records.Link_date).alias('Year')).distinct().sort('Link Date')
		
	records.createOrReplaceTempView("Table")

	records=sqlctx.sql("Select *,CASE When Month>%d"%MonthStart+"-1 THEN Month-%d"%MonthStart+"+1 ELSE Month+(13-%d"%MonthStart+") END AS Fiscal_Monthno,\
		CASE When Month>%d"%MonthStart+"-1 THEN CONCAT(Year, '-',SUBSTRING(Year+1,3,2)) ELSE CONCAT(Year-1, '-',SUBSTRING(Year,3,2)) END AS Fiscal_Year,\
		CASE When Month>%d"%MonthStart+"-1 THEN CONCAT(Year,Year+1) ELSE CONCAT(Year-1,Year) END AS Fiscal_Year_FULL,\
		CASE When Month>%d"%MonthStart+"-1 THEN Year ELSE Year-1 END AS FY_Year FROM Table")

	records.createOrReplaceTempView('TableZ')

	records=sqlctx.sql("SELECT *,\
	CASE When Month>%d"%MonthStart+"-1 THEN CONCAT('Q',Ceil((Month-%d"%MonthStart+"+1)/3)) ELSE CONCAT('Q',Ceil((Month+(13-%d"%MonthStart+"))/3)) END AS Fiscal_Quater,\
	CONCAT("+chr(39)+DB+chr(39)+",'|',"+chr(39)+Company+chr(39)+",'|',`Link Date`) as `Link Date Key`,"\
	+chr(39)+DB+chr(39)+" as DBName,"+chr(39)+Company+chr(39)+" as EntityName from TableZ")
	records = records.withColumn("Fiscal_Month",when(records.Month == 1,"Jan").when(records.Month == 2,"Feb").when(records.Month == 3,"Mar").when(records.Month == 4,"Apr").when(records.Month == 5,"May").when(records.Month == 6,"Jun").when(records.Month == 7,"Jul").when(records.Month == 8,"Aug").when(records.Month == 9,"Sep").when(records.Month == 10,"Oct").when(records.Month == 11,"Nov").when(records.Month == 12,"Dec").otherwise('null'))
	
	records.cache()
	
	#records.write.jdbc(url=Postgresurl, table="market99"+".Calendar", mode="overwrite", properties=Postgresprop)  #LINUX
	#records.write.jdbc(url=Postgresurl2, table="market99"+".Calendar", mode="overwrite", properties=Postgresprop2)   ##WINDOWS
	
	#Sqlurlwrite="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=kockpit;user=sa;password=M99@321"
	records.write.jdbc(url=Sqlurlwrite, table="Calendar", mode="overwrite")
    
	#FiscalYear=records.select("Fiscal_Year_FULL").distinct().orderBy("Fiscal_Year_FULL")
	#FiscalYear=FiscalYear.select(regexp_replace(col("Fiscal_Year_FULL"),'-','20')).withColumnRenamed("regexp_replace(Fiscal_Year_FULL, -, 20)","CalendarFY")
	#FiscalYear.write.mode(owmode).save(hdfspath+"/Stage1/CalendarFY")
	########----------------ADDED LOGS APR 27 -----------####
	end_time = datetime.datetime.now()
	endtime = end_time.strftime('%H:%M:%S')
	etime = str(end_time-start_time)
	etime = etime.split('.')[0]
	log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Calendar','DB':DB,'EN':Etn,
	    'Status':'Completed','ErrorLineNo':'NA','Operation':'Full','Rows':str(records.count()),'BeforeETLRows':'NA','AfterETLRows':'NA'}]
	log_df = sqlctx.createDataFrame(log_dict)#,schema_log)
	log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
	print("****************************SUCCESSFUL*******************************")
	
except Exception as ex:
	print(ex)
	exc_type,exc_value,exc_traceback=sys.exc_info()
	print("Error",ex)
	print("type - ",str(exc_type))
	print("line "+str(exc_traceback.tb_lineno))
	exc_type,exc_value,exc_traceback=sys.exc_info()
	########----------------ADDED LOGS APR 27 -----------####
	end_time = datetime.datetime.now()
	endtime = end_time.strftime('%H:%M:%S')
	etime = str(end_time-start_time)
	etime = etime.split('.')[0]
	log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Calendar','DB':DB,'EN':Etn,
	    'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
	log_df = sqlctx.createDataFrame(log_dict,schema_log)
	log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
	
	