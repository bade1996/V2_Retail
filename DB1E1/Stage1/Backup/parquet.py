'''
Created on 15 Jan 2019
@author: Ashish
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import Row
import csv, io,os, re
import datetime, time, pyspark
import pandas as pd
import shutil


#import org.apache.hadoop.fs.{FileSystem, Path}


#import org.apache.hadoop.fs

config = os.path.dirname(os.path.realpath(__file__))
DBET = config[config.rfind("DB"):config.rfind("/")]
path = config = config[0:config.rfind("DB")]
path = "file://"+path
config = pd.read_csv(config+"/Config/conf.csv")
for i in range(0,len(config)):
        exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))

conf = SparkConf().setMaster("local[8]").setAppName("Testing").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.appName("test").getOrCreate()

sqlctx = SQLContext(sc)

# os.rename("hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/shit","hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/Boom")
# exit()


# import org.apache.hadoop#.fs.{FileSystem, Path}
# import org.apache.spark.sql.SparkSession
# fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
# fs.rename("hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/shit","hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/Boom")
# exit()
#   #// Base path where Spark will produce output file
# # val basePath = "/bigtadata_etl/spark/output"
# #   val newFileName = "renamed_spark_output"
# #   // Change file name from Spark generic to new one
# fs.rename(new Path(s"$basePath/part-00000"), new Path(s"$basePath/$newFileName"))


#shutil.rmtree("hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/Stage2")
# 
# from hdfs3 import HDFileSystem
#  
# hdfs = HDFileSystem(host="hdfs://103.248.60.14", port="9000")
# HDFileSystem.rm("hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/Stage2")


# from pyarrow import hdfs
# fs = hdfs.connect(host, port)
# fs.delete(some_path, recursive=True)

# df1=({"Col1":["hello1","bello1"],"Col2":["hello2","bello2"]})
# df1=pd.DataFrame(df1)
# df11=spark.createDataFrame(df1)
# df11.write.mode(owmode).save(hdfspath+"DB1E1/Stage2/shit")
# exit()

#fs = sqlctx.read.parquet(hdfspath+"DB1E1/Stage2/shit")

#bashCommand = "hadoop fs -rm /KOCKPITDB1E1/Stage2/shit/*"
import subprocess
#bashCommand = "hadoop fs -mv hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/shit hdfs://103.248.60.14:9000/KOCKPITDB1E1/Stage2/Boom"
bashCommand = "hadoop fs -mv /KOCKPITDB1E1/Stage2/Cock /KOCKPITDB1E1/Stage2/Shit"
process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
exit()

fs = fs.withColumn('Name',lit('X'))
fs.write.mode(owmode).save(path+"DB1E1/Stage2/Stage2")
fs.show()
print("bal")
'''
#####################Reading required files###################################
stage2 = sqlctx.read.csv(path+"/Config/stage2.csv",header="true")
#####################Transformations#########################################
stage1 = stage2.select('Table_Name').distinct()
stage2 = stage2.select(col("Table_Name"),col("Col_Name"),regexp_replace(col("Script_Name"),' ','')).withColumnRenamed("regexp_replace(Script_Name,  , )","Script_Name")
stage2 = stage2.select(col("Script_Name"),col("Col_Name"),regexp_replace(col("Table_Name"),' ','')).withColumnRenamed("regexp_replace(Table_Name,  , )","Table_Name")
stage2 = stage2.select(col("Script_Name"),col("Table_Name"),regexp_replace(col("Col_Name"),' ','')).withColumnRenamed("regexp_replace(Col_Name,  , )","Col_Name")
####################Writing to HDFS in parquet###############################
stage1.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Stage1")
stage2.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage2/Stage2")
'''
