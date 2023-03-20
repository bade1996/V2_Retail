'''
Created on 2 Jan 2019
@author: Abhishek,Aniket
'''
### JOIN OF SL_HEAD AND SL_TXN then load
### IF PARQUET NOT EXIST LOAD FULL IF EXIST LOAD LAST  2 MONTHS
### ALSO COVERS WHETHER FISCAL YEAR SAME OR DIFFERENT
### WRITES PARWUET IN PARTION 
## SCRIPT DEPENDENT ON THIS STAGE2: sales2  

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
from _datetime import date
import dateutil.relativedelta
#from PIL.JpegImagePlugin import COM
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')


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
# start_time = datetime.datetime.now()
# stime = start_time.strftime('%H:%M:%S')

#present=Datelog
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
CM1=CM
CM = str(CM)
#print(CM)
##------Added May 4 FiscalM------##
if CM1>3:
    FM=CM1-3
else:
    FM=CM1+9

Mcount = 24+FM
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
# print("baal"+past)
# print(PM) 
# print(pastFY)
# print(present)
# print(CM)
# print(presentFY)



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
    
    conf = SparkConf().setMaster(smaster).setAppName("Stage1:11_IngLast2Mth").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:11_IngLast2Mth").getOrCreate()

    #AccDt = sqlctx.read.parquet(hdfspath+"/Data/AccessDetails").filter(col('NewDBName')==DB).collect()
    #CmpDt = sqlctx.read.parquet(hdfspath+"/Data/CompanyDetails").filter(col('DBName')==DB).filter(col('EntityName')==Etn)

    #Company = CmpDt.select(CmpDt.EntityName).collect()[0]['EntityName']
    #Sqlurl="jdbc:sqlserver://"+AccDt[0]['serverip']+port+";databaseName="+AccDt[0]['databasename']+";user="+AccDt[0]['userid']+";password=koc@P2019"
    #sSqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    '''
    CalendarFY=sqlctx.read.parquet(hdfspath+"/Stage1/CalendarFY")
    CalendarFY.show()
    Rno=CalendarFY.count()
    oldFY= CalendarFY.select('Fiscal_Year_FULL').collect()[Rno-1]["Fiscal_Year_FULL"]

    #.groupBy('CalendarFY').agg(F.first('CalendarFY'))
    #print(newFY)
    print("OldFiscalYr"+oldFY)
    '''
    
    
    ##############Fiscal Year Check###################
    df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_Rolling")  #FY checked
    #cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1").count()
    rowsW=0
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    #------Testing May 4
    #lol=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1").select("yearmonth").distinct().count()
    #lol=lol.select(lol.yearmonth).distinct().count()
    
    
    for j in range(0,df.count()):
        
        tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
        tbname1 = tbname.replace(' ','')
        
        #tbname1=tbname1+"20172018"
        #################PAST DATE##################
        # Sl_Head  Sl_Txn
        if tbname1=="Sl_Head":
            tbwOyr = tbname1
            
            tbname3 = "Sl_Txn"   #29 Feb
            tbwOyr3 = tbname3
            
            ####### CONDITION WHERE PARQUET DOES NOT EXIST #######
            #### SLHAEDSL_TXN1
            #check=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1").select("yearmonth").distinct().count()
            #if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn1")==256 or check!=Mcount:
            if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn1")==256:
                ######### =256 means Path does not exist##### PARQUET DOES NOT EXIST
                print("NOT EXIST")
                ############### IF PARQUET DOES NOT EXIST#####################
                print("LOAD FULL PARQUET")
                #os.system("hdfs dfs -rm -r /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn1")
                #tab=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Sl_HeadSl_Txn1")
                #tab.printSchema()
                
                #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1")
                ########################## MAR 3 TESTING ###############
                '''
                if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn45")==256:
                    ######### Path does not exist##### PARQUET DOES NOT EXIST
                    print("NOT EXIST")
                    print(os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Sl_HeadSl_Txn45"))
                
                '''
                
                table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                        +" where schema_name(schema_id) = 'dbo' and name like '%Sl_Txn________') AS data1"
                SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                SQLyear=SQLyear.collect()[0]["table_name"]
                #print(SQLyear)
                
                for FY in range(20182019,(int(SQLyear)+100),10001):
                    print(FY)
                    
                    tbname1="Sl_Head"
                    tbwOyr = tbname1
                    tbname3 = "Sl_Txn"   
                    tbwOyr3 = tbname3
                    
                    tbname1=tbname1+str(FY)
                    tbname=tbname1
                    # 29 feb
                    tbname3=tbname3+str(FY)
                    
                    '''
                    schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
                    
                    schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                    temp = ''
                    for i in range(0,len(schema)):
                        col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                        if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                            col_name_temp = "SH."+col_name_temp
                        elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                            col_name_temp = ","+"SH." + col_name_temp
                        if schema[i]['DATA_TYPE']=='sql_variant':
                            temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                        else:
                            temp = temp + col_name_temp
                    '''
                    
                    print(tbname)
                    print(tbname3)
                    '''
                    SH.[vouch_date],SH.[vouch_num],SH.[vouch_code],SH.[tax_reg_code],SH.[pay_mode],SH.[act_code],SH.[cust_code],SH.[agent_code],
                    SH.[sp_discount],SH.[freight],SH.[adjustment_b],SH.[credit_days],SH.[total_packs],SH.[gross_amt],SH.[net_amt],SH.[tot_commission_p],
                    SH.[tot_labour_u],SH.[tot_mfees_p],SH.[tot_sp_commission_p],SH.[tot_tax_1],SH.[tot_tax_2],SH.[tot_tax_3],SH.[tot_sur_on_tax3],
                    SH.[tot_adjustment_u],SH.[tot_rdf_p],SH.[round_amt],SH.[config_code],SH.[series_code],SH.[number_],SH.[bill_or_item],SH.Actual_Packs[],
                    SH.[Loose_Packs],SH.[Indicator],SH.[Deleted],SH.[Bill_Type],SH.[Total_Kgs],SH.[Tot_Excise_U],SH.[Tot_Scheme_U],SH.[Tot_Scheme_Rs],
                    SH.[Tot_Adjust_Rs],SH.[Tot_DN],SH.[Tot_CN],SH.[Tot_Display],SH.[Tot_Handling],SH.[Tot_Postage],SH.[TX_Amount],SH.[TF_Amount],
                    SH.[TP_Amount],SH.[TX_ST_Tot],SH.[TP_ST_Tot],SH.[TX_Sur_Tot],SH.[TP_Sur_Tot],SH.[Bill_Cust_Code],SH.[Form_Code],SH.[Link_Vouch_Code],
                    SH.[Opt_RQ],SH.[Tot_TComm],SH.[Chq_No],SH.[Chq_Date],SH.[MPM_Code],SH.[Member_Code],SH.[PCount],SH.[Comm_Calc_Code],SH.[Vw_Link_Code],
                    SH.[Stock_Trans],SH.[Delv_Vouch_Code],SH.[Branch_Code],SH.[DataTrfLog_Code],SH.[Cost_Code],SH.[OR_Code],SH.[Excise_Reg_Code],
                    SH.[Skip_Stock],SH.[Other_Settings],SH.[Doctor_Code],SH.[Currency_Code],SH.[Act_Code_For_Txn_X],SH.[Comp_Code],SH.[Vouch_Time],
                    SH.[Default_Acts_Mst_Code],SH.[Cashier_Code],SH.[Shift_Code],SH.[Skip_Pcs_Info],SH.[Exp_Imp_Doc],SH.[Doc_Nature],SH.[Manager_Code],
                    SH.[Store_Code],SH.[UnApproved_Doc],SH.[Exc_Sale_Nature],SH.[Transit_Status],SH.[Link_DataTrfLog_Code],SH.[Transit_Indward_Date],
                    SH.[New_Vouch_Num],SH.[Bill_To_Party_As]
                    '''
                    temp="""SH.[vouch_date],SH.[vouch_num],SH.[vouch_code],SH.[tax_reg_code],SH.[act_code],SH.[cust_code],SH.[agent_code],
                    SH.[gross_amt],SH.[net_amt],SH.[config_code],SH.[series_code],SH.[number_],SH.[Deleted],SH.[Bill_Type],
                    SH.[Tot_Excise_U],SH.[Tot_Scheme_U],SH.[Tot_Scheme_Rs],SH.[Tot_Adjust_Rs],SH.[Tot_DN],SH.[Tot_CN],SH.[Bill_Cust_Code],
                    SH.[Member_Code],SH.[Comm_Calc_Code],SH.[Stock_Trans],SH.[Branch_Code],SH.[Currency_Code],SH.[Comp_Code],SH.[Vouch_Time],
                    SH.[Transit_Indward_Date],SH.[Transit_Status]
                    """
                    
                    tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code ) AS data1"
                    
                    ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                    #print(tables)
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    
                    table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                    ############FISCAL YAER ADDED
                    table = table.withColumn("fiscalyear",lit(FY))     ##changed from newFY
                    ######## YAER MONTH FLAG ADD
                    table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
                    
                    ######################-PLAIN VALUE DICT ERROR APR 14#########################
                    table=table.withColumn("gross_amt",table["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",table["net_amt"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Excise_U",table["Tot_Excise_U"].cast("decimal(19,4)")).withColumn("Tot_Scheme_U",table["Tot_Scheme_U"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Scheme_Rs",table["Tot_Scheme_Rs"].cast("decimal(19,4)")).withColumn("Tot_Adjust_Rs",table["Tot_Adjust_Rs"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_DN",table["Tot_DN"].cast("decimal(19,4)")).withColumn("Tot_CN",table["Tot_CN"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Qty",table["Tot_Qty"].cast("decimal(19,4)")).withColumn("Calc_Gross_Amt",table["Calc_Gross_Amt"].cast("decimal(19,4)"))
                    table=table.withColumn("Calc_commission",table["Calc_commission"].cast("decimal(19,4)")).withColumn("Calc_Net_Amt",table["Calc_Net_Amt"].cast("decimal(19,4)"))
                    table=table.withColumn("calc_sp_commission",table["calc_sp_commission"].cast("decimal(19,4)")).withColumn("udamt",table["udamt"].cast("decimal(19,4)"))
                    
                    
                    table=table.withColumn("Vouch_code",table["Vouch_code"].cast("integer")).withColumn("tax_reg_code",table["tax_reg_code"].cast("integer"))
                    table=table.withColumn("act_code",table["act_code"].cast("integer")).withColumn("cust_code",table["cust_code"].cast("integer"))
                    table=table.withColumn("agent_code",table["agent_code"].cast("integer")).withColumn("config_code",table["config_code"].cast("integer"))
                    table=table.withColumn("series_code",table["series_code"].cast("integer")).withColumn("number_",table["number_"].cast("integer"))
                    table=table.withColumn("Bill_Cust_Code",table["Bill_Cust_Code"].cast("integer")).withColumn("Member_Code",table["Member_Code"].cast("integer"))
                    table=table.withColumn("Comm_Calc_Code",table["Comm_Calc_Code"].cast("integer")).withColumn("Branch_Code",table["Branch_Code"].cast("integer"))
                    table=table.withColumn("Currency_Code",table["Currency_Code"].cast("integer")).withColumn("Comp_Code",table["Comp_Code"].cast("integer"))
                    table=table.withColumn("Item_Det_Code",table["Item_Det_Code"].cast("integer")).withColumn("Code",table["Code"].cast("integer"))
                    table=table.withColumn("Lot_Code",table["Lot_Code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))
                    
                    
                    table=table.withColumn("vouch_date",table["vouch_date"].cast("timestamp")).withColumn("Vouch_Time",table["Vouch_Time"].cast("timestamp"))
                    table=table.withColumn("Transit_Indward_Date",table["Transit_Indward_Date"].cast("timestamp"))#.withColumn("",table[""].cast("timestamp"))
                    
                    
                    
                    table=table.withColumn("vouch_num",table["vouch_num"].cast("string")).withColumn("Bill_Type",table["Bill_Type"].cast("string"))\
                            .withColumn("EntityName",table["entityname"].cast("string")).withColumn("DBName",table["DBName"].cast("string"))
                    table=table.withColumn("Sale_Or_SR",table["Sale_Or_SR"].cast("string")).withColumn("yearmonth",table["yearmonth"].cast("string"))
                            
                            
                    table=table.withColumn("Deleted",table["Deleted"].cast("boolean")).withColumn("Stock_Trans",table["Stock_Trans"].cast("boolean"))
                    table=table.withColumn("SLTXNDeleted",table["SLTXNDeleted"].cast("boolean")).withColumn("Sa_Subs_Lot",table["Sa_Subs_Lot"].cast("boolean"))
                    
                    
                    table=table.withColumn("Transit_Status",table["Transit_Status"].cast("short")).withColumn("Stock_Trans",table["Stock_Trans"].cast("short"))
                    
                    print("Type Casting")
                    ######################################################################################
                    table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                    
                    #tableAll = tableAll.unionByName(table)
                    table.cache()
                    table.write.partitionBy("yearmonth").mode("append").save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1")
                    rowsW=rowsW+table.count()
                #tableAll.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1") ##
                end_time = datetime.datetime.now()
                print("SUCCESSFULLY RAN FULL YEARS RELOAD")
                cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1").count()
                endtime = end_time.strftime('%H:%M:%S')
                etime = str(end_time-start_time)
                etime = etime.split('.')[0]
                log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'11IngLastTwoMth','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo.':'NA','Operation':'Full','Rows':str(rowsW),'BeforeETLRows':'0','AfterETLRows':str(cnt)}]
                log_df = sqlctx.createDataFrame(log_dict,schema_log)
                log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
                ###########################EINDING#####################
                '''
                pastFY='20172018'
                test=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+pastFY)    #
                test.show()
                print(test.count)
                
                '''
                '''
                ############### CONCAT 20172018 20182019 20192020 THENDUMP TO FILENAME FILE########
                prev1718 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"20172018")
                prev1718.cache()
                prev1819 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"20182019")
                conTab = prev1718.unionByName(prev1819).cache()
                prev1920 = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"20192020")
                conTab=conTab.unionByName(prev1920).cache()
                conTab.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3)
                '''
            else:
                ############# IF PARQUET EXISTS #############
                '''
                prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1")
                prevTab.show()
                '''
                print("ONLY LAST TWO MONTHS LOAD")
                
                
                if pastFY==presentFY:
                    print("LOAD LAST TWO MONTHS SAME FY")
                    #list = ['20192020']#,'20182019','20192020']
                    #for i in list:
                        #pastFY=i
                    bcnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1").count()
                    
                    tbname1=tbname1+str(pastFY)
                    tbname=tbname1                  ### Sl_Head
                    # 29 feb
                    tbname3=tbname3+str(pastFY)     ### Sl_Txn
                    #tbname = TB_Name+tbname
                    #changed Below1
                    #if tbname1 =='Sl_Txn20192020':
                    
                    '''
                    schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
                    
                    schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                    temp = ''
                    for i in range(0,len(schema)):
                        col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                        if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                            col_name_temp = "SH."+col_name_temp
                        elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                            col_name_temp = ","+"SH." + col_name_temp
                        if schema[i]['DATA_TYPE']=='sql_variant':
                            temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                        else:
                            temp = temp + col_name_temp
                    '''
                    print(tbname)
                    print(tbname3)
                    
                    temp="""SH.[vouch_date],SH.[vouch_num],SH.[vouch_code],SH.[tax_reg_code],SH.[act_code],SH.[cust_code],SH.[agent_code],
                    SH.[gross_amt],SH.[net_amt],SH.[config_code],SH.[series_code],SH.[number_],SH.[Deleted],SH.[Bill_Type],
                    SH.[Tot_Excise_U],SH.[Tot_Scheme_U],SH.[Tot_Scheme_Rs],SH.[Tot_Adjust_Rs],SH.[Tot_DN],SH.[Tot_CN],SH.[Bill_Cust_Code],
                    SH.[Member_Code],SH.[Comm_Calc_Code],SH.[Stock_Trans],SH.[Branch_Code],SH.[Currency_Code],SH.[Comp_Code],SH.[Vouch_Time],
                    SH.[Transit_Indward_Date],SH.[Transit_Status]
                    """
                    
                    ############### PREVIOUS MONTH
                    tablesPM = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE MONTH(SH.vouch_date) ="+PM+") AS data1"
                    
                    
                    tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    ############FISCAL YAER ADDED
                    tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.vouch_date)))
                    
                    ######################-PLAIN VALUE DICT ERROR APR 14#########################
                    tablePM=tablePM.withColumn("gross_amt",tablePM["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",tablePM["net_amt"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_Excise_U",tablePM["Tot_Excise_U"].cast("decimal(19,4)")).withColumn("Tot_Scheme_U",tablePM["Tot_Scheme_U"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_Scheme_Rs",tablePM["Tot_Scheme_Rs"].cast("decimal(19,4)")).withColumn("Tot_Adjust_Rs",tablePM["Tot_Adjust_Rs"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_DN",tablePM["Tot_DN"].cast("decimal(19,4)")).withColumn("Tot_CN",tablePM["Tot_CN"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_Qty",tablePM["Tot_Qty"].cast("decimal(19,4)")).withColumn("Calc_Gross_Amt",tablePM["Calc_Gross_Amt"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Calc_commission",tablePM["Calc_commission"].cast("decimal(19,4)")).withColumn("Calc_Net_Amt",tablePM["Calc_Net_Amt"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("calc_sp_commission",tablePM["calc_sp_commission"].cast("decimal(19,4)")).withColumn("udamt",tablePM["udamt"].cast("decimal(19,4)"))
                    
                    
                    tablePM=tablePM.withColumn("Vouch_code",tablePM["Vouch_code"].cast("integer")).withColumn("tax_reg_code",tablePM["tax_reg_code"].cast("integer"))
                    tablePM=tablePM.withColumn("act_code",tablePM["act_code"].cast("integer")).withColumn("cust_code",tablePM["cust_code"].cast("integer"))
                    tablePM=tablePM.withColumn("agent_code",tablePM["agent_code"].cast("integer")).withColumn("config_code",tablePM["config_code"].cast("integer"))
                    tablePM=tablePM.withColumn("series_code",tablePM["series_code"].cast("integer")).withColumn("number_",tablePM["number_"].cast("integer"))
                    tablePM=tablePM.withColumn("Bill_Cust_Code",tablePM["Bill_Cust_Code"].cast("integer")).withColumn("Member_Code",tablePM["Member_Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Comm_Calc_Code",tablePM["Comm_Calc_Code"].cast("integer")).withColumn("Branch_Code",tablePM["Branch_Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Currency_Code",tablePM["Currency_Code"].cast("integer")).withColumn("Comp_Code",tablePM["Comp_Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Item_Det_Code",tablePM["Item_Det_Code"].cast("integer")).withColumn("Code",tablePM["Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Lot_Code",tablePM["Lot_Code"].cast("integer")).withColumn("fiscalyear",tablePM["fiscalyear"].cast("integer"))
                    
                    
                    tablePM=tablePM.withColumn("vouch_date",tablePM["vouch_date"].cast("timestamp")).withColumn("Vouch_Time",tablePM["Vouch_Time"].cast("timestamp"))
                    tablePM=tablePM.withColumn("Transit_Indward_Date",tablePM["Transit_Indward_Date"].cast("timestamp"))#.withColumn("",tablePM[""].cast("timestamp"))
                    
                    
                    tablePM=tablePM.withColumn("vouch_num",tablePM["vouch_num"].cast("string")).withColumn("Bill_Type",tablePM["Bill_Type"].cast("string"))\
                            .withColumn("EntityName",tablePM["entityname"].cast("string")).withColumn("DBName",tablePM["DBName"].cast("string"))
                    tablePM=tablePM.withColumn("Sale_Or_SR",tablePM["Sale_Or_SR"].cast("string")).withColumn("yearmonth",tablePM["yearmonth"].cast("string"))
                            
                            
                    tablePM=tablePM.withColumn("Deleted",tablePM["Deleted"].cast("boolean")).withColumn("Stock_Trans",tablePM["Stock_Trans"].cast("boolean"))
                    tablePM=tablePM.withColumn("SLTXNDeleted",tablePM["SLTXNDeleted"].cast("boolean")).withColumn("Sa_Subs_Lot",tablePM["Sa_Subs_Lot"].cast("boolean"))
                    
                    
                    tablePM=tablePM.withColumn("Transit_Status",tablePM["Transit_Status"].cast("short")).withColumn("Stock_Trans",tablePM["Stock_Trans"].cast("short"))
                    
                    #####################################################
                    tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
                    
                    #tablePM.cache()
                    tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1"+"/yearmonth="+pastFY+PM)
                    
                    
                    ################ CURRENT MONTH
                    
                    tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE MONTH(SH.vouch_date) ="+CM+") AS data1"
                    
                    ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                    
                    
                    
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    
                    table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                    ############FISCAL YAER ADDED
                    table = table.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
                    ######################-PLAIN VALUE DICT ERROR APR 14#########################
                    table=table.withColumn("gross_amt",table["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",table["net_amt"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Excise_U",table["Tot_Excise_U"].cast("decimal(19,4)")).withColumn("Tot_Scheme_U",table["Tot_Scheme_U"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Scheme_Rs",table["Tot_Scheme_Rs"].cast("decimal(19,4)")).withColumn("Tot_Adjust_Rs",table["Tot_Adjust_Rs"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_DN",table["Tot_DN"].cast("decimal(19,4)")).withColumn("Tot_CN",table["Tot_CN"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Qty",table["Tot_Qty"].cast("decimal(19,4)")).withColumn("Calc_Gross_Amt",table["Calc_Gross_Amt"].cast("decimal(19,4)"))
                    table=table.withColumn("Calc_commission",table["Calc_commission"].cast("decimal(19,4)")).withColumn("Calc_Net_Amt",table["Calc_Net_Amt"].cast("decimal(19,4)"))
                    table=table.withColumn("calc_sp_commission",table["calc_sp_commission"].cast("decimal(19,4)")).withColumn("udamt",table["udamt"].cast("decimal(19,4)"))
                    
                    
                    table=table.withColumn("Vouch_code",table["Vouch_code"].cast("integer")).withColumn("tax_reg_code",table["tax_reg_code"].cast("integer"))
                    table=table.withColumn("act_code",table["act_code"].cast("integer")).withColumn("cust_code",table["cust_code"].cast("integer"))
                    table=table.withColumn("agent_code",table["agent_code"].cast("integer")).withColumn("config_code",table["config_code"].cast("integer"))
                    table=table.withColumn("series_code",table["series_code"].cast("integer")).withColumn("number_",table["number_"].cast("integer"))
                    table=table.withColumn("Bill_Cust_Code",table["Bill_Cust_Code"].cast("integer")).withColumn("Member_Code",table["Member_Code"].cast("integer"))
                    table=table.withColumn("Comm_Calc_Code",table["Comm_Calc_Code"].cast("integer")).withColumn("Branch_Code",table["Branch_Code"].cast("integer"))
                    table=table.withColumn("Currency_Code",table["Currency_Code"].cast("integer")).withColumn("Comp_Code",table["Comp_Code"].cast("integer"))
                    table=table.withColumn("Item_Det_Code",table["Item_Det_Code"].cast("integer")).withColumn("Code",table["Code"].cast("integer"))
                    table=table.withColumn("Lot_Code",table["Lot_Code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))
                    
                    
                    table=table.withColumn("vouch_date",table["vouch_date"].cast("timestamp")).withColumn("Vouch_Time",table["Vouch_Time"].cast("timestamp"))
                    table=table.withColumn("Transit_Indward_Date",table["Transit_Indward_Date"].cast("timestamp"))#.withColumn("",table[""].cast("timestamp"))
                    
                    
                    table=table.withColumn("vouch_num",table["vouch_num"].cast("string")).withColumn("Bill_Type",table["Bill_Type"].cast("string"))\
                            .withColumn("EntityName",table["entityname"].cast("string")).withColumn("DBName",table["DBName"].cast("string"))
                    table=table.withColumn("Sale_Or_SR",table["Sale_Or_SR"].cast("string")).withColumn("yearmonth",table["yearmonth"].cast("string"))
                            
                            
                    table=table.withColumn("Deleted",table["Deleted"].cast("boolean")).withColumn("Stock_Trans",table["Stock_Trans"].cast("boolean"))
                    table=table.withColumn("SLTXNDeleted",table["SLTXNDeleted"].cast("boolean")).withColumn("Sa_Subs_Lot",table["Sa_Subs_Lot"].cast("boolean"))
                    
                    
                    table=table.withColumn("Transit_Status",table["Transit_Status"].cast("short")).withColumn("Stock_Trans",table["Stock_Trans"].cast("short"))
                    ##############################################################
                    table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                    
                    table.cache()
                    table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1"+"/yearmonth="+pastFY+CM)
                    
                    ###table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+pastFY)    # mar 1
                    #changed below
                    #prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"Previous")  #Previous Table Read
                    '''
                    prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3)
                    #prevTab.show()
                    prevTab=prevTab.filter(prevTab.vouch_date<past)
                    #prevTab.cache()
                    #table=concatenate(table,prevTab,spark,sqlctx)    #Concat previous table and New table
                    table=prevTab.unionByName(table)      ## Union Previous and current Table
                    table.cache()
                    
                    table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"5")   #Separate Storing location
                    ################DELETE COMPLETELY THE CONTENTS OF TABLE READ FROM HDFS########26Feb
                    ###################DUMP TO SEPARATE LOCATION & RENAME  28 Feb
                    bashCommand = "hadoop fs -mv /KOCKPIT/DB1E1/Stage1/"+tbwOyr+tbwOyr3+"5"+" "+"/KOCKPIT/DB1E1/Stage1/"+tbwOyr+tbwOyr3
                    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                    output, error = process.communicate()
                    '''
                    cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1").count()
                    #####READ FROM THE SEPARATE LOCATION
                    #table=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"5")
                    
                    #table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+"2")    # before ......tbname1
                    #end
                    print("SUCCESSSSSSSSSSSSSS LAST TWO MONTHS")
                    Tcount=tablePM.count()+table.count()
                    end_time = datetime.datetime.now()
                    endtime = end_time.strftime('%H:%M:%S')
                    etime = str(end_time-start_time)
                    etime = etime.split('.')[0]
                    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'11IngLastTwoMth','DB':DB,'EN':Etn,
                            'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(Tcount),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
                    log_df = sqlctx.createDataFrame(log_dict,schema_log)
                    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
                    
#                     end_time = datetime.datetime.now()
#                     endtime = end_time.strftime('%H:%M:%S')
#                     etime = str(end_time-start_time)
#                     etime = etime.split('.')[0]
#                     try:
#                         IDEorBatch = sys.argv[1]
#                     except Exception as e :
#                         IDEorBatch = "IDLE"
#                     log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
#                             'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
#                     log_df = sqlctx.createDataFrame(log_dict,schema_log)
#                     log_df.write.mode(apmode).save(hdfspath+"/Logs")
                    
                else:
                    #################################1st part PREVIOUS MONTH OF PASTFY###################
                    print("LOAD LAST TWO MONTHS DIFFERENT FY")
                    bcnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1").count()
                    
                    tbname1=tbname1+str(pastFY)
                    tbname=tbname1                  ### Sl_Head
                    # 29 feb
                    tbname3=tbname3+str(pastFY)     ### Sl_Txn
                    #tbname = TB_Name+tbname
                    #changed Below1
                    #if tbname1 =='Sl_Txn20192020':
                    '''
                    schema = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+tbname+chr(39)+") AS data"
                    
                    schema = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=schema,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()
                    temp = ''
                    for i in range(0,len(schema)):
                        col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                        if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                            col_name_temp = "SH."+col_name_temp
                        elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                            col_name_temp = ","+"SH." + col_name_temp
                        if schema[i]['DATA_TYPE']=='sql_variant':
                            temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                        else:
                            temp = temp + col_name_temp
                    '''
                    
                    print(tbname)
                    print(tbname3)
                    
                    '''
                    tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE SH.vouch_date >="+past+") AS data1"
                    '''
                    
                    ############### PREVIOUS MONTH
                    temp="""SH.[vouch_date],SH.[vouch_num],SH.[vouch_code],SH.[tax_reg_code],SH.[act_code],SH.[cust_code],SH.[agent_code],
                    SH.[gross_amt],SH.[net_amt],SH.[config_code],SH.[series_code],SH.[number_],SH.[Deleted],SH.[Bill_Type],
                    SH.[Tot_Excise_U],SH.[Tot_Scheme_U],SH.[Tot_Scheme_Rs],SH.[Tot_Adjust_Rs],SH.[Tot_DN],SH.[Tot_CN],SH.[Bill_Cust_Code],
                    SH.[Member_Code],SH.[Comm_Calc_Code],SH.[Stock_Trans],SH.[Branch_Code],SH.[Currency_Code],SH.[Comp_Code],SH.[Vouch_Time],
                    SH.[Transit_Indward_Date],SH.[Transit_Status]
                    """
                    tablesPM = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE MONTH(SH.vouch_date) ="+PM+") AS data1"
                    
                    
                    tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
                    ############FISCAL YAER ADDED
                    tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.vouch_date)))
                    ######################-PLAIN VALUE DICT ERROR APR 14#########################
                    tablePM=tablePM.withColumn("gross_amt",tablePM["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",tablePM["net_amt"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_Excise_U",tablePM["Tot_Excise_U"].cast("decimal(19,4)")).withColumn("Tot_Scheme_U",tablePM["Tot_Scheme_U"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_Scheme_Rs",tablePM["Tot_Scheme_Rs"].cast("decimal(19,4)")).withColumn("Tot_Adjust_Rs",tablePM["Tot_Adjust_Rs"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_DN",tablePM["Tot_DN"].cast("decimal(19,4)")).withColumn("Tot_CN",tablePM["Tot_CN"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Tot_Qty",tablePM["Tot_Qty"].cast("decimal(19,4)")).withColumn("Calc_Gross_Amt",tablePM["Calc_Gross_Amt"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("Calc_commission",tablePM["Calc_commission"].cast("decimal(19,4)")).withColumn("Calc_Net_Amt",tablePM["Calc_Net_Amt"].cast("decimal(19,4)"))
                    tablePM=tablePM.withColumn("calc_sp_commission",tablePM["calc_sp_commission"].cast("decimal(19,4)")).withColumn("udamt",tablePM["udamt"].cast("decimal(19,4)"))
                    
                    
                    tablePM=tablePM.withColumn("Vouch_code",tablePM["Vouch_code"].cast("integer")).withColumn("tax_reg_code",tablePM["tax_reg_code"].cast("integer"))
                    tablePM=tablePM.withColumn("act_code",tablePM["act_code"].cast("integer")).withColumn("cust_code",tablePM["cust_code"].cast("integer"))
                    tablePM=tablePM.withColumn("agent_code",tablePM["agent_code"].cast("integer")).withColumn("config_code",tablePM["config_code"].cast("integer"))
                    tablePM=tablePM.withColumn("series_code",tablePM["series_code"].cast("integer")).withColumn("number_",tablePM["number_"].cast("integer"))
                    tablePM=tablePM.withColumn("Bill_Cust_Code",tablePM["Bill_Cust_Code"].cast("integer")).withColumn("Member_Code",tablePM["Member_Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Comm_Calc_Code",tablePM["Comm_Calc_Code"].cast("integer")).withColumn("Branch_Code",tablePM["Branch_Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Currency_Code",tablePM["Currency_Code"].cast("integer")).withColumn("Comp_Code",tablePM["Comp_Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Item_Det_Code",tablePM["Item_Det_Code"].cast("integer")).withColumn("Code",tablePM["Code"].cast("integer"))
                    tablePM=tablePM.withColumn("Lot_Code",tablePM["Lot_Code"].cast("integer")).withColumn("fiscalyear",tablePM["fiscalyear"].cast("integer"))
                    
                    
                    tablePM=tablePM.withColumn("vouch_date",tablePM["vouch_date"].cast("timestamp")).withColumn("Vouch_Time",tablePM["Vouch_Time"].cast("timestamp"))
                    tablePM=tablePM.withColumn("Transit_Indward_Date",tablePM["Transit_Indward_Date"].cast("timestamp"))#.withColumn("",tablePM[""].cast("timestamp"))
                    
                    
                    
                    tablePM=tablePM.withColumn("vouch_num",tablePM["vouch_num"].cast("string")).withColumn("Bill_Type",tablePM["Bill_Type"].cast("string"))\
                            .withColumn("EntityName",tablePM["entityname"].cast("string")).withColumn("DBName",tablePM["DBName"].cast("string"))
                    tablePM=tablePM.withColumn("Sale_Or_SR",tablePM["Sale_Or_SR"].cast("string")).withColumn("yearmonth",tablePM["yearmonth"].cast("string"))
                            
                            
                    tablePM=tablePM.withColumn("Deleted",tablePM["Deleted"].cast("boolean")).withColumn("Stock_Trans",tablePM["Stock_Trans"].cast("boolean"))
                    tablePM=tablePM.withColumn("SLTXNDeleted",tablePM["SLTXNDeleted"].cast("boolean")).withColumn("Sa_Subs_Lot",tablePM["Sa_Subs_Lot"].cast("boolean"))
                    
                    
                    tablePM=tablePM.withColumn("Transit_Status",tablePM["Transit_Status"].cast("short")).withColumn("Stock_Trans",tablePM["Stock_Trans"].cast("short"))
                    
                    #####################################################
                    tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
                    #tablePM.cache()
                    tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1"+"/yearmonth="+pastFY+PM)
                    
                    
                    
                    ########################### 2ND PART CURRENT MONTH OF PRESENT FY ################
                    ################ CURRENT MONTH
                    '''
                    table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                        +" where schema_name(schema_id) = 'dbo' and name like '%Sl_Txn________') AS data1"
                    SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    SQLyear=SQLyear.collect()[0]["table_name"]
                    presentFY = SQLyear
                    print(SQLyear)
                    '''
                    tbname1="Sl_Head"
                    tbname3="Sl_Txn"
                    
                    tbname1=tbname1+str(presentFY)
                    tbname=tbname1                  ### Sl_Head
                    # 29 feb
                    tbname3=tbname3+str(presentFY)     ### Sl_Txn
                    #tbname = TB_Name+tbname
                    #changed Below1
                    #if tbname1 =='Sl_Txn20192020':
                    
                    print(tbname)
                    print(tbname3)
                    
                    tables = "(SELECT "+temp+",SL_TXN.Sale_Or_SR,SL_TXN.Tot_Qty, SL_TXN.Calc_Gross_Amt, SL_TXN.Calc_commission, SL_TXN.Item_Det_Code,"\
                    +" SL_TXN.Calc_Net_Amt, SL_TXN.calc_sp_commission, SL_TXN.Code, SL_TXN.Deleted AS SLTXNDeleted, SL_TXN.Lot_Code, SL_TXN.Sa_Subs_Lot," \
                     +" (0 + SL_TXN.Calc_Gross_Amt + SL_TXN.Calc_commission + SL_TXN.calc_sp_commission + SL_TXN.calc_rdf "\
                     +"+ SL_TXN.calc_scheme_u + SL_TXN.calc_scheme_rs + SL_TXN.Calc_Tax_1 + SL_TXN.Calc_Tax_2 + SL_TXN.Calc_Tax_3 "\
                     +"+ SL_TXN.calc_sur_on_tax3 + SL_TXN.calc_mfees + SL_TXN.calc_excise_u + SL_TXN.Calc_adjustment_u + "\
                     +"SL_TXN.Calc_adjust_rs + SL_TXN.Calc_freight + SL_TXN.calc_adjust + SL_TXN.Calc_Spdisc + SL_TXN.Calc_DN + "\
                     +"SL_TXN.Calc_CN + SL_TXN.Calc_Display + SL_TXN.Calc_Handling + SL_TXN.calc_Postage + SL_TXN.calc_Round + "\
                     +"SL_TXN.calc_Labour) AS udamt FROM ["+tbname3+"] AS SL_TXN LEFT JOIN ["+tbname+"] AS SH ON SH.Vouch_Code = SL_TXN.Vouch_Code WHERE MONTH(SH.vouch_date) ="+CM+") AS data1"
                    
                    ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
                    
                    table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    
                    table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
                    table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
                    table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
                    ############FISCAL YAER ADDED
                    table = table.withColumn("fiscalyear",lit(presentFY))     ##changed from newFY
                    
                    ######## YAER MONTH FLAG ADD
                    table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.vouch_date)))
                    ######################-PLAIN VALUE DICT ERROR APR 14#########################
                    table=table.withColumn("gross_amt",table["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",table["net_amt"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Excise_U",table["Tot_Excise_U"].cast("decimal(19,4)")).withColumn("Tot_Scheme_U",table["Tot_Scheme_U"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Scheme_Rs",table["Tot_Scheme_Rs"].cast("decimal(19,4)")).withColumn("Tot_Adjust_Rs",table["Tot_Adjust_Rs"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_DN",table["Tot_DN"].cast("decimal(19,4)")).withColumn("Tot_CN",table["Tot_CN"].cast("decimal(19,4)"))
                    table=table.withColumn("Tot_Qty",table["Tot_Qty"].cast("decimal(19,4)")).withColumn("Calc_Gross_Amt",table["Calc_Gross_Amt"].cast("decimal(19,4)"))
                    table=table.withColumn("Calc_commission",table["Calc_commission"].cast("decimal(19,4)")).withColumn("Calc_Net_Amt",table["Calc_Net_Amt"].cast("decimal(19,4)"))
                    table=table.withColumn("calc_sp_commission",table["calc_sp_commission"].cast("decimal(19,4)")).withColumn("udamt",table["udamt"].cast("decimal(19,4)"))
                    
                    
                    table=table.withColumn("Vouch_code",table["Vouch_code"].cast("integer")).withColumn("tax_reg_code",table["tax_reg_code"].cast("integer"))
                    table=table.withColumn("act_code",table["act_code"].cast("integer")).withColumn("cust_code",table["cust_code"].cast("integer"))
                    table=table.withColumn("agent_code",table["agent_code"].cast("integer")).withColumn("config_code",table["config_code"].cast("integer"))
                    table=table.withColumn("series_code",table["series_code"].cast("integer")).withColumn("number_",table["number_"].cast("integer"))
                    table=table.withColumn("Bill_Cust_Code",table["Bill_Cust_Code"].cast("integer")).withColumn("Member_Code",table["Member_Code"].cast("integer"))
                    table=table.withColumn("Comm_Calc_Code",table["Comm_Calc_Code"].cast("integer")).withColumn("Branch_Code",table["Branch_Code"].cast("integer"))
                    table=table.withColumn("Currency_Code",table["Currency_Code"].cast("integer")).withColumn("Comp_Code",table["Comp_Code"].cast("integer"))
                    table=table.withColumn("Item_Det_Code",table["Item_Det_Code"].cast("integer")).withColumn("Code",table["Code"].cast("integer"))
                    table=table.withColumn("Lot_Code",table["Lot_Code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))
                    
                    
                    table=table.withColumn("vouch_date",table["vouch_date"].cast("timestamp")).withColumn("Vouch_Time",table["Vouch_Time"].cast("timestamp"))
                    table=table.withColumn("Transit_Indward_Date",table["Transit_Indward_Date"].cast("timestamp"))#.withColumn("",table[""].cast("timestamp"))
                    
                    
                    
                    table=table.withColumn("vouch_num",table["vouch_num"].cast("string")).withColumn("Bill_Type",table["Bill_Type"].cast("string"))\
                            .withColumn("EntityName",table["entityname"].cast("string")).withColumn("DBName",table["DBName"].cast("string"))
                    table=table.withColumn("Sale_Or_SR",table["Sale_Or_SR"].cast("string")).withColumn("yearmonth",table["yearmonth"].cast("string"))
                            
                            
                    table=table.withColumn("Deleted",table["Deleted"].cast("boolean")).withColumn("Stock_Trans",table["Stock_Trans"].cast("boolean"))
                    table=table.withColumn("SLTXNDeleted",table["SLTXNDeleted"].cast("boolean")).withColumn("Sa_Subs_Lot",table["Sa_Subs_Lot"].cast("boolean"))
                    
                    
                    table=table.withColumn("Transit_Status",table["Transit_Status"].cast("short")).withColumn("Stock_Trans",table["Stock_Trans"].cast("short"))
                    ##############################################################
                    table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
                    
                    table.cache()
                    table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1"+"/yearmonth="+presentFY+CM)
                    cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1").count()
                    
                    
                    print("SUCCESSSSSSSSSSSSSSSSSSS LAST TWO MONTHS")
                    Tcount=tablePM.count()+table.count()
                    end_time = datetime.datetime.now()
                    endtime = end_time.strftime('%H:%M:%S')
                    etime = str(end_time-start_time)
                    etime = etime.split('.')[0]
                    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'11IngLastTwoMth','DB':DB,'EN':Etn,
                            'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(Tcount),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
                    log_df = sqlctx.createDataFrame(log_dict,schema_log)
                    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
                    
                    
                    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'11IngLastTwoMth','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
