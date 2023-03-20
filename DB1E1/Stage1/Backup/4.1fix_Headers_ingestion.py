## This is for fixing headers i.e lower case the headers of year tables then union by name
##  Also for adding Fiscal year respectively


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import last
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udl
import pandas as pd

#import org.apache.hadoop.fs.{FileSystem, Path}
#from PIL.JpegImagePlugin import COM
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
schema_log = StructType([
        StructField('Date',StringType(),True),
        StructField('Start_Time',StringType(),True),
        StructField('End_Time', StringType(),True),
        StructField('Run_Time',StringType(),True),
        StructField('File_Name',StringType(),True),
        StructField('DB',StringType(),True),
        StructField('EN', StringType(),True),
        StructField('Status',StringType(),True),
        StructField('Log_Status',StringType(),True),
        StructField('ErrorLineNo.',StringType(),True),
        StructField('Rows',IntegerType(),True),
        StructField('Columns',IntegerType(),True),
        StructField('Source',StringType(),True)
        ])

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')

#print("Today:"+Datelog)

'''
yr= Datelog.split('-')[0]
oyr=int(yr)-1
newFY=str(oyr)+yr
'''
#print("CurrentFiscalYr:"+newFY)


try:
    config = os.path.dirname(os.path.realpath(__file__))
    DBET = config[config.rfind("DB"):config.rfind("/")]
    Etn = DBET[DBET.rfind("E"):]
    DB = DBET[:DBET.rfind("E")]
    config_path = config[0:config.rfind("DB")]
    path = config = config[0:config.rfind("DB")]
    path = "file://"+path
    config = pd.read_csv(config+"/Config/conf.csv")
    
    
    for i in range(0,len(config)):
        exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
        
    cdate_old = config.iloc[6]['Val']
    cdate_new = str(datetime.datetime.today().date())
    config = config.replace(cdate_old,cdate_new)
    config.to_csv(config_path+"/Config/conf.csv",index=False)

    conf = SparkConf().setMaster(smaster).setAppName("4Fix_headers+FiscalYear").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")\
                .set("spark.driver.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("4Fix_headers+FiscalYear").getOrCreate()
    #AccDt = sqlctx.read.parquet(hdfspath+"/Data/AccessDetails").filter(col('NewDBName')==DB).collect()
    #CmpDt = sqlctx.read.parquet(hdfspath+"/Data/CompanyDetails").filter(col('DBName')==DB).filter(col('EntityName')==Etn)

    #Company = CmpDt.select(CmpDt.EntityName).collect()[0]['EntityName']
    #Sqlurl="jdbc:sqlserver://"+AccDt[0]['serverip']+port+";databaseName="+AccDt[0]['databasename']+";user="+AccDt[0]['userid']+";password=koc@P2019"
    Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    
    
    prevTab = SQLContext.inferSchema(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3)
    
    print(prevTab)
    exit()
    
    
    
    table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables\
        where schema_name(schema_id) = 'dbo' and name like '%sl_txn________'-- put your schema name here) AS data1"
    
    table_max = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    
        
    
    tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                 +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                 +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                 +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                 +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                 +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                 +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE SH.vouch_date >="+past+") AS data1"
    
    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",\
                                               partitionColumn="Entry No_",lowerBound=0, upperBound=10000000, numPartitions=3).load()
    
    
    tbname1="Sl_Head"
    tbwOyr = tbname1
    #####29 feb
    tbname3 = "Sl_Txn"   #29 Feb
    tbwOyr3 = tbname3
    
    prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3)
    prevTab=prevTab.filter(prevTab.vouch_date=='2019-11-23')
    prevTab=prevTab.withColumn("yearMonth",concat(prevTab.fiscalyear,month(prevTab.vouch_date)))
    prevTab.select('yearMonth').distinct().show()
    exit()
    
    ##############Fiscal Check###################
    df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_1")  #FY  checked
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    for j in range(0,df.count()):
        start_time = datetime.datetime.now()
        stime = start_time.strftime('%H:%M:%S')
        tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
        tbname1 = tbname.replace(' ','')
        tbwOyr =tbname1
        '''
        tbname1=tbname1+"20182019"
        #tbname1=tbname1+str(newFY)
        tbname=tbname1
        #tbname = TB_Name+tbname
        
        schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
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
        print(temp)
        print(tbname)
        tables = "(SELECT "+temp+" FROM ["+tbname+"]) AS data1"
        if tbname1 == 'G_LEntry':
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Entry No_",\
                lowerBound=0, upperBound=10000000, numPartitions=3).load()
        elif tbname1 == 'ValueEntry':
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",partitionColumn="Item Ledger Entry No_",\
                                lowerBound=0, upperBound=3000000, numPartitions=3).load()
        else:
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        
        table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
        table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
        table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
        table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
        table.cache()
        
        table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbname1)
        '''
        
        prev1718 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"20172018")  #Previous Table Read
        
        ############# FISCAL YEAR ADDED
        prev1718 = prev1718.withColumn("FiscalYear",lit("20172018"))
        prev1718=prev1718.toDF(*[c.lower() for c in prev1718.columns])
        prev1718.cache()
        
        
        prev1819 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"20182019")  #Previous Table Read
        ############# FISCAL YEAR ADDED
        prev1819 = prev1819.withColumn("FiscalYear",lit("20182019"))
        prev1819=prev1819.toDF(*[c.lower() for c in prev1819.columns])
        prev1819.cache()
        
        
        prevTab=prev1718.unionByName(prev1819)
        prevTab.cache()
        print(tbname1+"Previous")
        #prevTab.show()
        prevTab.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")
        
        ####### ADDED FOR LATEST YEAR
        
        
        
        print("SUCESSSSSSSSSSSSSSSS"+tbwOyr)
        #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
        '''
        table=table.union(prevTab)
        
#         prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
#         prevTab.cache()
        
        table=table.cache()
        table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")    # before ......tbname1
        
        
        
        
        
        
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
                'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
        log_df = sqlctx.createDataFrame(log_dict,schema_log)
        log_df.write.mode(apmode).save(hdfspath+"/Logs")
        '''
except Exception as ex:
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Stage1','DB':DB,'EN':Etn,'Status':'Failed',\
            'Log_Status':ex,'ErrorLineNo.':str(exc_traceback.tb_lineno),'Rows':0,'Columns':0,'Source':IDEorBatch}]
    log_df = spark.createDataFrame(log_dict,schema_log)
    log_df.write.mode(apmode).save(hdfspath+"/Logs")
