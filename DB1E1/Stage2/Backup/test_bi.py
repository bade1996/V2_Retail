from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext

conf=SparkConf().setMaster('local[16]').setAppName('tets_bi')
sc=SparkContext(conf=conf)
sqlctx=SQLContext(sc)

df = sqlctx.read.parquet('hdfs://103.248.60.14:9000/KOCKPIT/DB1E1/Stage1/Lot_Mst')
df.cache()
df.write.mode('overwrite').format('csv').save('hdfs://103.248.60.14:9000/lotmst1')
#df.rdd.saveAsTextFile('hdfs://103.248.60.14:9000/lotmst1.txt')
