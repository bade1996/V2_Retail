from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udf
import pandas as pd
from pyspark.sql.window import Window

current_date = datetime.datetime.today().date()
#current_date = datetime.date(2019,12,25)
cy = current_date.year
cm = current_date.month
if cm==1:
	pm = 12
	py = cy-1
else:
	pm = cm-1
	py = cy
previous_date = datetime.date(py, pm, 1)
#print(previous_date)

if cm<10:
	cym = str(cy)+"0"+str(cm)
else:
	cym = str(cy)+str(cm)
if pm<10:
	pym = str(py)+"0"+str(pm)
else:
	pym = str(py)+str(pm)

config = pd.read_csv("file:///home/hadoopusr/KOCKPIT/conf.csv")
for i in range(0,len(config)):
	exec(str(config.iloc[i]["Var"])+"="+chr(34)+str(config.iloc[i]["Val"])+chr(34))

inc_vars = pd.read_csv("file:///home/hadoopusr/KOCKPIT/incvar.csv")
for i in range(0,len(inc_vars)):
	exec(str(inc_vars.iloc[i]["Var"])+"="+chr(34)+str(inc_vars.iloc[i]["Val"])+chr(34))

conf = SparkConf().setMaster('local[16]').setAppName("Incremental_Dev")\
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
			.set("spark.executor.memory","20g")

sc = SparkContext(conf=conf)
sqlctx = SQLContext(sc)
spark = SparkSession.builder.appName("Incremental_Dev").getOrCreate()

#vedf = sqlctx.read.parquet('hdfs://192.168.2.31:9000/test/DB1/Stage1/ValueEntry')
vedf = sqlctx.read.parquet('hdfs://192.168.2.31:9000/test/DB1/Stage1/ValueEntry/YM=202002')
vedf.cache()
#vdd.cache()
vecsv = vedf.select('EntityName').groupBy('EntityName').count()
#vecsv.toPandas().to_csv('/home/hadoopusr/vecsv.csv')
#print(vedf.select('EntityName').distinct().show(60))
#print(vdd.select('YM').distinct().show())
#exit()

Sqlurl = "jdbc:sqlserver://192.168.2.9;databasename=BFPLDB17;user=qlik;password=BI@2018"

CmpDt = sqlctx.read.parquet(hdfspath+"/Data/CompanyDetails").filter(col("newdbname")=="DB1").filter(col("Inactive")==0)


cd = CmpDt.select('EntityName','CompanyName').withColumnRenamed('EntityName','EN')
vecond = [vecsv.EntityName==cd.EN]
vecsv = vecsv.join(cd,vecond,'left').drop('EN')
vecsv.toPandas().to_csv('/home/hadoopusr/vecsv.csv')
exit()

df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1").filter(col("Table_Name").isin(["Value Entry"]))
stage3 = sqlctx.read.parquet(hdfspath+"/Stage1/column")
stage3.cache()
for j in range(0,df.count()):
	tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
	tbname1 = tbname.replace(' ','')
	for k in range(0,CmpDt.count()):
		TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[k]["CompanyName"] + '$'
		if(tbname1 == 'VendorPostingGroup' or tbname1 == 'CustomerPostingGroup' or tbname1 == 'State'):
			TB_Name = ''
		else:
			TB_Name = TB_Name.replace('.','_')
		tbname_temp = TB_Name+tbname
		Etn = CmpDt.select(CmpDt.EntityName).collect()[k]["EntityName"]
		schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname_temp+chr(39)+") AS data"
		schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
		temp = ''
		for i in range(0,len(schema)):
			col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
			if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
				col_name_temp = col_name_temp
			elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
				col_name_temp = "," + col_name_temp
			if schema[i]['DATA_TYPE']=='sql_variant':
				temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
			else:
				temp = temp + col_name_temp
		print(k)
		print(temp)
		print(tbname_temp)
		#tables = "(SELECT "+temp+" FROM ["+tbname_temp+"]) AS data1"
		if tbname1 == "ValueEntry":
			if VE=='0':
				tables = "(SELECT "+temp+" FROM ["+tbname_temp+"]) AS data1"
				table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,\
					driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",\
					partitionColumn="Item Ledger Entry No_",\
					lowerBound=0, upperBound=50000000, numPartitions=10).load()
			else:
				tables = "(SELECT "+temp+" FROM ["+tbname_temp+"] WHERE [Posting Date]>="+chr(39)+str(previous_date)+chr(39)+") AS data1"
				table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,\
					driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
		elif tbname1 == "G_LEntry":
			if GE=='0':
				tables = "(SELECT "+temp+" FROM ["+tbname_temp+"]) AS data1"
				table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,\
                                        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",\
                                        partitionColumn="Entry No_",\
                                        lowerBound=0, upperBound=50000000, numPartitions=10).load()
			else:
				tables = "(SELECT "+temp+" FROM ["+tbname_temp+"] WHERE [Posting Date]>="+chr(39)+str(previous_date)+chr(39)+") AS data1"
				table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,\
                                        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
		else:
			tables = "(SELECT "+temp+" FROM ["+tbname_temp+"]) AS data1"
			table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,\
					driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
		table = table.withColumn("EntityName",lit(Etn)).withColumn("DBName",lit("DB1"))
		table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
		table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
		table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))

		col_name = stage3.filter(col("table_name") == tbname1).select("column").rdd.flatMap(lambda x: x).collect()
		table = table.select(col_name)
		if(k == 0):
			table1 = table
		else:
			table1 = table1.union(table)
	table = table1
	if tbname1 == "ValueEntry" or "G_LEntry":
		#table = table.withColumn("FY",when(to_date(table.PostingDate)<"2018-04-01",lit(20172018))\
		#	.otherwise(when((to_date(table.PostingDate)>="2018-04-01")&(to_date(table.PostingDate)<"2019-04-01"),lit(20182019))\
		#	.otherwise(lit(20192020))))
		table = table.withColumn("Y",year(table.PostingDate))
		table = table.withColumn("M",when(month(table.PostingDate)<10,concat(lit(0),month(table.PostingDate))).otherwise(month(table.PostingDate)))
		table = table.withColumn("YM",concat(table.Y,table.M)).drop("Y","M")
	#table.select('YM').distinct().show()
	#print(VE)
	#exit()
	#table.persist()
	if tbname1 == "ValueEntry":
		if VE=='0':
			table.persist()
			table.repartition(200).write.partitionBy("YM").mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1)
			table.unpersist()
		else:
			tbl1 = table.filter(table.YM==pym)
			tbl2 = table.filter(table.YM==cym)
			tbl1.cache()
			tbl2.cache()
			tbl1.repartition(200).write.mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1+"/YM="+pym)
			tbl2.repartition(200).write.mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1+"/YM="+cym)
	elif tbname1 == "G_LEntry":
		if GE=='0':
			table.persist()
			table.repartition(2000).write.partitionBy("YM").mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1)
			table.unpersist()
		else:
			tbl1 = table.filter(table.YM==pym)
			tbl2 = table.filter(table.YM==cym)
			tbl1.cache()
			tbl2.cache()
			tbl1.repartition(200).write.mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1+"/YM="+pym)
			tbl2.repartition(200).write.mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1+"/YM="+cym)
	else:
		table.persist()
		table.repartition(2000).write.mode(owmode).save("hdfs://192.168.2.31:9000/test/DB1/Stage1/"+tbname1)
		table.unpersist()
