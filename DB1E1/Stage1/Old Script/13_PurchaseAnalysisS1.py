'''
Created on Apr 4 , 20
@author: Abhishek,Aniket

Purchase Analysis Query script   JOIN OF 3 PURCHASE TABLES
  Pur_Head FY     Pur_TxnFY     Pur_Head1FY
  
Scripts Dependent on this : PurchaseAnalysisS2, 2AStockInAnalysis
'''
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
import subprocess
import dateutil.relativedelta
#from PIL.JpegImagePlugin import COM
#import org.apache.log4j.Logger
#import org.apache.log4j.Level
#Logger.getLogger("org").setLevel(Level.OFF) Logger.getLogger("akka").setLevel(Level.OFF)

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

    conf = SparkConf().setMaster(smaster).setAppName("PurAnalysisS1").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("PurAnalysisS1").getOrCreate()

    #Sqlurl="jdbc:sqlserver://MARKET-NINETY3\MARKET99;databaseName=LOGICDBS99;user=sa;password=M99@321"
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS                
    
    ##############Fiscal Year Check###################
    #df = sqlctx.read.parquet(hdfspath+"/Stage1/Stage1_Rolling")  #FY checked
    #TB_Name = CmpDt.select(CmpDt.CompanyName).collect()[0]["CompanyName"] + '$'
    
    #for j in range(0,df.count()):
    start_time = datetime.datetime.now()
    stime = start_time.strftime('%H:%M:%S')
    #tbname = df.select(df.Table_Name).collect()[j]["Table_Name"]
    #tbname1 = tbname.replace(' ','')
    
    #tbname1=tbname1+"20172018"
    #################PAST DATE##################
    tbname1='Pur_Txn'
    tbname2='Pur_Head'
    tbname3='Pur_Head1'
    
    rowsW=0
    
        ####### CONDITION WHERE PARQUET DOES NOT EXIST #######
        #### SLHAEDSL_TXN1
    if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Pur_HeadPur_Txn")==256: # 256 Code Defines PARQUET DOES NOT EXIST
    #if os.system("hadoop fs -ls /KOCKPIT/DB1E1/Stage1/Po_Txn")==256:
        
        print('FULL LOAD YEARS FROM 1819')
        table_max = "(select max(RIGHT(name,8)) as table_name from sys.tables" \
                +" where schema_name(schema_id) = 'dbo' and name like '%Pur_Head________') AS data1"
        
        SQLyear = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=table_max,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
        SQLyear.cache()
        SQLyear=SQLyear.collect()[0]["table_name"]
        print("SQLMAX",SQLyear)
        
        for FY in range(20182019,(int(SQLyear)+100),10001):
            print(FY)
            
            tbname1='Pur_Txn'
            tbname2='Pur_Head'
            tbname3='Pur_Head1'
            #continue
            tbname1=tbname1+str(FY)
            tbname=tbname1
            tbname2=tbname2+str(FY)
            tbname3=tbname3+str(FY)
            # 29 feb
            
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
            
            print(tbname)
            
            tables = """(SELECT PUT.Tot_Qty as TotalPurQty,    PUT.CF_Qty as Qty,PUT.Calc_Tax_2 as Tax2,    PUT.Calc_Tax_3    as Tax3, PUT.Calc_DN as Debit_Note,
                    (0 + PUT.Calc_Gross_Amt + PUT.Calc_commission + PUT.calc_sp_commission + PUT.calc_rdf + PUT.calc_scheme_u + PUT.calc_scheme_rs + PUT.Calc_Tax_1 + PUT.Calc_Tax_2 + PUT.Calc_Tax_3 + PUT.calc_sur_on_tax3 + PUT.calc_mfees + PUT.calc_excise_u + PUT.Calc_adjustment_u + PUT.Calc_adjust_rs + PUT.Calc_freight + PUT.calc_adjust + PUT.Calc_Spdisc + PUT.Calc_DN + PUT.Calc_CN + PUT.Calc_Display + PUT.Calc_Handling + PUT.calc_Postage + PUT.calc_round)  
                    as userDefinedNetAmount, PUT.Calc_Commission as CD,    PUT.Calc_Sp_Commission as TD, PUT.Calc_Rdf AS SpCD, PUT.Calc_Labour AS Labour,
                    PUT.Free_Qty, PUT.Repl_Qty, PUT.calc_adjustment_u As Adjustment,PUT.Calc_Freight AS FREIGHT,PUT.Calc_Spdisc  as SPDiscount,PUT.Calc_Scheme_U  as schemePerunit,
                    PUT.Calc_Scheme_Rs as SchemeRs, PUT.CF_Qty AS Units_Per_Pack, PUT.Calc_MFees  as ExcisePercentage, PUT.Calc_Excise_U as ExcisePerUnit, 
                    PUT.Calc_cn  as CreditNote, PUT.Calc_Display as Display, PUT.Calc_Handling    as Handling, PUT.Calc_Postage as Postage, PUT.Sample_Qty, PUT.rate,    
                    PUT.Excise_Amt_Rate As Excise,  PUT.Calc_Adjustment_u  as AdjustmentPerUnit, PUT.Qty_Weight AS TotalWeight,PUT.vouch_code, 
                    PUT.Lot_Code, PUT.item_det_code,  PUT.Comm_Calc_Code, PUH.Tax_Reg_Code, PUT.Godown_Code, PUT.Comm_it_Desc_Code, PUH.cust_code, --Join Cond
                    PUH.act_code, PUH.agent_code,  --Join Cond
                    PUH.gross_amt, PUH.net_amt,  --Controv
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate,PUT.Calc_Gross_Amt as GrossAmount1, PUT.Calc_Net_Amt as NetAmount1,--POAna
                    PUT.Tot_Qty/PUT.CF_Qty as Total_Packs1, PUT.rate * PUT.CF_Qty as Rate_Per_Pack, PUH.net_amt-PUH.bill_amount as BillDifference,
                    
                    PUH.Vouch_Num AS VOUCHNO, PUH.Grn_Number, PUH.Bill_Amount, PUH.Goods_In_Transit,
                    PUH.Branch_Code,PUH.config_code,PUT.Lot_Sch_Code,PUT.Carton_Code,PUT.Calc_Sale_Amt,  --New Add1
                    PUH.total_packs,PUH.VouchType,PUT.Pur_Or_PR, --NewAdd2
                    PUH.Bill_No AS BillNo,PUH.GRN_PreFix,PUH.Stock_Trans, --Added Apr21
                    
                    PUH1.gr_date as GRDate, PUH1.gr_number as GRNo
                    FROM """+tbname1+""" AS PUT WITH (NOLOCK) LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK)
                    ON PUT.vouch_code = PUH.vouch_code 
                    LEFT JOIN """+tbname3+""" AS PUH1 ON PUT.vouch_code=PUH1.vouch_code WHERE PUH.Deleted_ = 0) AS data1""" 
            
            ##Pur_Txn20192020
            ##Pur_Head20192020
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(FY))     ##changed from newFY
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.VoucherDate)))
            
            ######################-PLAIN VALUE DICT ERROR APR 14#########################
            table=table.withColumn("totalpurqty",table["totalpurqty"].cast("decimal(19,4)")).withColumn("qty",table["qty"].cast("decimal(19,4)"))
            table=table.withColumn("tax2",table["tax2"].cast("decimal(19,4)")).withColumn("tax3",table["tax3"].cast("decimal(19,4)"))
            table=table.withColumn("debit_note",table["debit_note"].cast("decimal(19,4)")).withColumn("userdefinednetamount",table["userdefinednetamount"].cast("decimal(19,4)"))
            table=table.withColumn("cd",table["cd"].cast("decimal(19,4)")).withColumn("td",table["td"].cast("decimal(19,4)"))
            table=table.withColumn("spcd",table["spcd"].cast("decimal(19,4)")).withColumn("labour",table["labour"].cast("decimal(19,4)"))
            
            table=table.withColumn("free_qty",table["free_qty"].cast("decimal(19,4)")).withColumn("repl_qty",table["repl_qty"].cast("decimal(19,4)"))
            table=table.withColumn("adjustment",table["adjustment"].cast("decimal(19,4)")).withColumn("freight",table["freight"].cast("decimal(19,4)"))
            table=table.withColumn("spdiscount",table["spdiscount"].cast("decimal(19,4)")).withColumn("schemeperunit",table["schemeperunit"].cast("decimal(19,4)"))
            table=table.withColumn("schemers",table["schemers"].cast("decimal(19,4)")).withColumn("units_per_pack",table["units_per_pack"].cast("decimal(19,4)"))
            table=table.withColumn("excisepercentage",table["excisepercentage"].cast("decimal(19,4)")).withColumn("exciseperunit",table["exciseperunit"].cast("decimal(19,4)"))
            
            table=table.withColumn("creditnote",table["creditnote"].cast("decimal(19,4)")).withColumn("display",table["display"].cast("decimal(19,4)"))
            table=table.withColumn("handling",table["handling"].cast("decimal(19,4)")).withColumn("postage",table["postage"].cast("decimal(19,4)"))
            table=table.withColumn("sample_qty",table["sample_qty"].cast("decimal(19,4)")).withColumn("rate",table["rate"].cast("double"))
            table=table.withColumn("excise",table["excise"].cast("decimal(19,4)")).withColumn("adjustmentperunit",table["adjustmentperunit"].cast("decimal(19,4)"))
            table=table.withColumn("totalweight",table["totalweight"].cast("double")).withColumn("vouch_code",table["vouch_code"].cast("integer"))
            
            table=table.withColumn("lot_code",table["lot_code"].cast("integer")).withColumn("item_det_code",table["item_det_code"].cast("integer"))
            table=table.withColumn("comm_calc_code",table["comm_calc_code"].cast("integer")).withColumn("tax_reg_code",table["tax_reg_code"].cast("integer"))
            table=table.withColumn("godown_code",table["godown_code"].cast("integer")).withColumn("comm_it_desc_code",table["comm_it_desc_code"].cast("integer"))
            table=table.withColumn("cust_code",table["cust_code"].cast("integer")).withColumn("act_code",table["act_code"].cast("integer"))
            table=table.withColumn("agent_code",table["agent_code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))\
                    .withColumn("yearmonth",table["yearmonth"].cast("integer"))
            
            table=table.withColumn("gross_amt",table["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",table["net_amt"].cast("decimal(19,4)"))
            table=table.withColumn("grnno",table["grnno"].cast("decimal(19,4)"))
            table=table.withColumn("grossamount1",table["grossamount1"].cast("decimal(19,4)")).withColumn("netamount1",table["netamount1"].cast("decimal(19,4)"))
            table=table.withColumn("total_packs",table["total_packs"].cast("decimal(19,4)")).withColumn("billdifference",table["billdifference"].cast("decimal(19,4)"))
            table=table.withColumn("grn_number",table["grn_number"].cast("decimal(19,4)")).withColumn("bill_amount",table["bill_amount"].cast("decimal(19,4)"))
            
            
            table=table.withColumn("billdate",table["billdate"].cast("timestamp")).withColumn("voucherdate",table["voucherdate"].cast("timestamp"))
            table=table.withColumn("grdate",table["grdate"].cast("timestamp"))#.withColumn("",table[""].cast("timestamp"))
            
            table=table.withColumn("rate_per_pack",table["rate_per_pack"].cast("double"))
            
            table=table.withColumn("vouchno",table["vouchno"].cast("string")).withColumn("grno",table["grno"].cast("string"))\
                    .withColumn("entityname",table["entityname"].cast("string")).withColumn("dbname",table["dbname"].cast("string"))
            
            table=table.withColumn("goods_in_transit",table["goods_in_transit"].cast("boolean"))
            
            ##################-------PLAIN VALUE DICT ERROR APR 14 END---------###############
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table = table.drop_duplicates()     ####### 14 MAR
            table.cache()
            table.write.partitionBy("yearmonth").mode("append").save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn")
            print("WRITTEN",tbname1)
            rowsW=rowsW+table.count()
            #tableAll = tableAll.unionByName(table)
            #tableAll.cache()
        print("SUCCESSFULLY RAN FULL YEARS RELOAD")
        cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn").count()
        end_time = datetime.datetime.now()
        endtime = end_time.strftime('%H:%M:%S')
        etime = str(end_time-start_time)
        etime = etime.split('.')[0]
        log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'13_PurchaseAnalysisS1','DB':DB,'EN':Etn,
            'Status':'Completed','ErrorLineNo.':'NA','Operation':'Full','Rows':str(rowsW),'BeforeETLRows':'0','AfterETLRows':str(cnt)}]
        log_df = sqlctx.createDataFrame(log_dict,schema_log)
        log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
        #tableAll.write.partitionBy("yearmonth").mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/PohPotPuhPut") ##
        ###########################EINDING#####################
    else:
        ############# IF PARQUET EXISTS #############
        '''
        prevTab = sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/"+tbwOyr+tbwOyr3+"1")
        prevTab.show()
        '''
        print("LOAD LAST TWO MONTHS")
        
        if pastFY==presentFY:
            ########### SAME FISCAL YEAR  ##########
            print("SAME FISCAL YEAR")
            bcnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn").count()
            tbname1=tbname1+str(pastFY)
            tbname=tbname1                  ### Sl_Head
            tbname2=tbname2+str(pastFY)
            tbname3=tbname3+str(pastFY)
            # 29 feb
                                #if tbname1 =='Sl_Txn20192020':
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
            
            print(tbname)
            
            ############### PREVIOUS MONTH
            tablesPM = """(SELECT PUT.Tot_Qty as TotalPurQty,    PUT.CF_Qty as Qty,PUT.Calc_Tax_2 as Tax2,    PUT.Calc_Tax_3    as Tax3, PUT.Calc_DN as Debit_Note,
                    (0 + PUT.Calc_Gross_Amt + PUT.Calc_commission + PUT.calc_sp_commission + PUT.calc_rdf + PUT.calc_scheme_u + PUT.calc_scheme_rs + PUT.Calc_Tax_1 + PUT.Calc_Tax_2 + PUT.Calc_Tax_3 + PUT.calc_sur_on_tax3 + PUT.calc_mfees + PUT.calc_excise_u + PUT.Calc_adjustment_u + PUT.Calc_adjust_rs + PUT.Calc_freight + PUT.calc_adjust + PUT.Calc_Spdisc + PUT.Calc_DN + PUT.Calc_CN + PUT.Calc_Display + PUT.Calc_Handling + PUT.calc_Postage + PUT.calc_round)  
                    as userDefinedNetAmount, PUT.Calc_Commission as CD,    PUT.Calc_Sp_Commission as TD, PUT.Calc_Rdf AS SpCD, PUT.Calc_Labour AS Labour,
                    PUT.Free_Qty, PUT.Repl_Qty, PUT.calc_adjustment_u As Adjustment,PUT.Calc_Freight AS FREIGHT,PUT.Calc_Spdisc  as SPDiscount,PUT.Calc_Scheme_U  as schemePerunit,
                    PUT.Calc_Scheme_Rs as SchemeRs, PUT.CF_Qty AS Units_Per_Pack, PUT.Calc_MFees  as ExcisePercentage, PUT.Calc_Excise_U as ExcisePerUnit, 
                    PUT.Calc_cn  as CreditNote, PUT.Calc_Display as Display, PUT.Calc_Handling    as Handling, PUT.Calc_Postage as Postage, PUT.Sample_Qty, PUT.rate,    
                    PUT.Excise_Amt_Rate As Excise,  PUT.Calc_Adjustment_u  as AdjustmentPerUnit, PUT.Qty_Weight AS TotalWeight,PUT.vouch_code, 
                    PUT.Lot_Code, PUT.item_det_code,  PUT.Comm_Calc_Code, PUH.Tax_Reg_Code, PUT.Godown_Code, PUT.Comm_it_Desc_Code, PUH.cust_code, --Join Cond
                    PUH.act_code, PUH.agent_code,  --Join Cond
                    PUH.gross_amt, PUH.net_amt,  --Controv
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate,PUT.Calc_Gross_Amt as GrossAmount1, PUT.Calc_Net_Amt as NetAmount1,--POAna
                    PUT.Tot_Qty/PUT.CF_Qty as Total_Packs1, PUT.rate * PUT.CF_Qty as Rate_Per_Pack, PUH.net_amt-PUH.bill_amount as BillDifference,
                    
                    PUH.Vouch_Num AS VOUCHNO, PUH.Grn_Number, PUH.Bill_Amount, PUH.Goods_In_Transit,
                    PUH.Branch_Code,PUH.config_code,PUT.Lot_Sch_Code,PUT.Carton_Code,PUT.Calc_Sale_Amt,  --New Add1
                    PUH.total_packs,PUH.VouchType,PUT.Pur_Or_PR, --NewAdd2
                    PUH.Bill_No AS BillNo,PUH.GRN_PreFix,PUH.Stock_Trans, --Added Apr21
                    
                    PUH1.gr_date as GRDate, PUH1.gr_number as GRNo
                    FROM """+tbname1+""" AS PUT WITH (NOLOCK) LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK)
                    ON PUT.vouch_code = PUH.vouch_code 
                    LEFT JOIN """+tbname3+""" AS PUH1 ON PUT.vouch_code=PUH1.vouch_code WHERE PUH.Deleted_ = 0 AND MONTH(PUH.Vouch_Date)= """+PM+""") AS data1""" 
            '''
            tablesPM = """(SELECT POH.Order_No As PoOrderNo, POH.Order_Date As PoOrderDate, POH.Valid_Date AS PoValidDate, PUH.Bill_No AS BillNo, 
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate, --PUH.Grn_Number As GrnNo,
                    POH.Branch_Code, POH.Sup_Code, POT.Item_Det_Code, PUT.Lot_Code,
                    (CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END) As POQty, --Purchase_Order_Qty
                    PUT.Tot_Qty AS PurchaseQty,(POT.Tot_Qty * POT.Rate) As OrderAmount, 
                    (CASE WHEN (POH.Order_Type='PI' OR POT.Cancel_Item=0) THEN 0 ELSE POT.Pend_Qty END) As CancelPOQty,
                    (CASE WHEN POT.Cancel_Item=0 THEN POT.Pend_Qty ELSE 0 END) As PendingPOQty, --Pending_Purchase_Order_Qty 
                    (PUT.Tot_Qty/(CASE WHEN POH.Order_Type = 'PI' THEN 0 ELSE POT.Tot_Qty END))*100 AS FillRateQty, --26Mar
                    PUT.Calc_Tax_3 AS TAX, PUT.Rate as BasicRate,POT.Rate AS POTRate, POH.Tot_Tax AS TotalTAX, 
                    PUT.Calc_Net_Amt as NetAmount, PUT.Calc_Gross_Amt as GrossAmount,PUT.vouch_code
                    FROM Po_Txn AS POT LEFT JOIN """+tbname1+""" AS PUT WITH (NOLOCK) ON (POT.code = PUT.Order_Item_Code 
                    AND PUT.Order_item_code > 0 AND PUT.Challan_Code = 0)
                    LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK) ON PUT.vouch_code = PUH.vouch_code
                    INNER JOIN Po_Head AS POH ON POT.vouch_code=POH.vouch_code WHERE MONTH(PUH.Vouch_Date) = """+PM+") AS data1"
            '''
            #tablesPM = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+PM+") AS data1"
        
            
            tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
            ############FISCAL YAER ADDED
            tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.VoucherDate)))
            
            ######################-PLAIN VALUE DICT ERROR APR 14#########################
            tablePM=tablePM.withColumn("totalpurqty",tablePM["totalpurqty"].cast("decimal(19,4)")).withColumn("qty",tablePM["qty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("tax2",tablePM["tax2"].cast("decimal(19,4)")).withColumn("tax3",tablePM["tax3"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("debit_note",tablePM["debit_note"].cast("decimal(19,4)")).withColumn("userdefinednetamount",tablePM["userdefinednetamount"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("cd",tablePM["cd"].cast("decimal(19,4)")).withColumn("td",tablePM["td"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("spcd",tablePM["spcd"].cast("decimal(19,4)")).withColumn("labour",tablePM["labour"].cast("decimal(19,4)"))
            
            tablePM=tablePM.withColumn("free_qty",tablePM["free_qty"].cast("decimal(19,4)")).withColumn("repl_qty",tablePM["repl_qty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("adjustment",tablePM["adjustment"].cast("decimal(19,4)")).withColumn("freight",tablePM["freight"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("spdiscount",tablePM["spdiscount"].cast("decimal(19,4)")).withColumn("schemeperunit",tablePM["schemeperunit"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("schemers",tablePM["schemers"].cast("decimal(19,4)")).withColumn("units_per_pack",tablePM["units_per_pack"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("excisepercentage",tablePM["excisepercentage"].cast("decimal(19,4)")).withColumn("exciseperunit",tablePM["exciseperunit"].cast("decimal(19,4)"))
            
            tablePM=tablePM.withColumn("creditnote",tablePM["creditnote"].cast("decimal(19,4)")).withColumn("display",tablePM["display"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("handling",tablePM["handling"].cast("decimal(19,4)")).withColumn("postage",tablePM["postage"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("sample_qty",tablePM["sample_qty"].cast("decimal(19,4)")).withColumn("rate",tablePM["rate"].cast("double"))
            tablePM=tablePM.withColumn("excise",tablePM["excise"].cast("decimal(19,4)")).withColumn("adjustmentperunit",tablePM["adjustmentperunit"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("totalweight",tablePM["totalweight"].cast("double")).withColumn("vouch_code",tablePM["vouch_code"].cast("integer"))
            
            tablePM=tablePM.withColumn("lot_code",tablePM["lot_code"].cast("integer")).withColumn("item_det_code",tablePM["item_det_code"].cast("integer"))
            tablePM=tablePM.withColumn("comm_calc_code",tablePM["comm_calc_code"].cast("integer")).withColumn("tax_reg_code",tablePM["tax_reg_code"].cast("integer"))
            tablePM=tablePM.withColumn("godown_code",tablePM["godown_code"].cast("integer")).withColumn("comm_it_desc_code",tablePM["comm_it_desc_code"].cast("integer"))
            tablePM=tablePM.withColumn("cust_code",tablePM["cust_code"].cast("integer")).withColumn("act_code",tablePM["act_code"].cast("integer"))
            tablePM=tablePM.withColumn("agent_code",tablePM["agent_code"].cast("integer")).withColumn("fiscalyear",tablePM["fiscalyear"].cast("integer"))\
                    .withColumn("yearmonth",tablePM["yearmonth"].cast("integer"))
            
            tablePM=tablePM.withColumn("gross_amt",tablePM["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",tablePM["net_amt"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("grnno",tablePM["grnno"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("grossamount1",tablePM["grossamount1"].cast("decimal(19,4)")).withColumn("netamount1",tablePM["netamount1"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("total_packs",tablePM["total_packs"].cast("decimal(19,4)")).withColumn("billdifference",tablePM["billdifference"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("grn_number",tablePM["grn_number"].cast("decimal(19,4)")).withColumn("bill_amount",tablePM["bill_amount"].cast("decimal(19,4)"))
            
            
            tablePM=tablePM.withColumn("billdate",tablePM["billdate"].cast("timestamp")).withColumn("voucherdate",tablePM["voucherdate"].cast("timestamp"))
            tablePM=tablePM.withColumn("grdate",tablePM["grdate"].cast("timestamp"))#.withColumn("",tablePM[""].cast("timestamp"))
            
            tablePM=tablePM.withColumn("rate_per_pack",tablePM["rate_per_pack"].cast("double"))
            
            tablePM=tablePM.withColumn("vouchno",tablePM["vouchno"].cast("string")).withColumn("grno",tablePM["grno"].cast("string"))\
                    .withColumn("entityname",tablePM["entityname"].cast("string")).withColumn("dbname",tablePM["dbname"].cast("string"))
            
            tablePM=tablePM.withColumn("goods_in_transit",tablePM["goods_in_transit"].cast("boolean"))
            
            ##################-------PLAIN VALUE DICT ERROR APR 14 END---------###############
            
            tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
            
            tablePM = tablePM.drop_duplicates()     ####### 14 MAR
            tablePM.cache()
            tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn/yearmonth="+pastFY+PM)
            
            
            
            ################ CURRENT MONTH
            tables = """(SELECT PUT.Tot_Qty as TotalPurQty,    PUT.CF_Qty as Qty,PUT.Calc_Tax_2 as Tax2,    PUT.Calc_Tax_3    as Tax3, PUT.Calc_DN as Debit_Note,
                    (0 + PUT.Calc_Gross_Amt + PUT.Calc_commission + PUT.calc_sp_commission + PUT.calc_rdf + PUT.calc_scheme_u + PUT.calc_scheme_rs + PUT.Calc_Tax_1 + PUT.Calc_Tax_2 + PUT.Calc_Tax_3 + PUT.calc_sur_on_tax3 + PUT.calc_mfees + PUT.calc_excise_u + PUT.Calc_adjustment_u + PUT.Calc_adjust_rs + PUT.Calc_freight + PUT.calc_adjust + PUT.Calc_Spdisc + PUT.Calc_DN + PUT.Calc_CN + PUT.Calc_Display + PUT.Calc_Handling + PUT.calc_Postage + PUT.calc_round)  
                    as userDefinedNetAmount, PUT.Calc_Commission as CD,    PUT.Calc_Sp_Commission as TD, PUT.Calc_Rdf AS SpCD, PUT.Calc_Labour AS Labour,
                    PUT.Free_Qty, PUT.Repl_Qty, PUT.calc_adjustment_u As Adjustment,PUT.Calc_Freight AS FREIGHT,PUT.Calc_Spdisc  as SPDiscount,PUT.Calc_Scheme_U  as schemePerunit,
                    PUT.Calc_Scheme_Rs as SchemeRs, PUT.CF_Qty AS Units_Per_Pack, PUT.Calc_MFees  as ExcisePercentage, PUT.Calc_Excise_U as ExcisePerUnit, 
                    PUT.Calc_cn  as CreditNote, PUT.Calc_Display as Display, PUT.Calc_Handling    as Handling, PUT.Calc_Postage as Postage, PUT.Sample_Qty, PUT.rate,    
                    PUT.Excise_Amt_Rate As Excise,  PUT.Calc_Adjustment_u  as AdjustmentPerUnit, PUT.Qty_Weight AS TotalWeight,PUT.vouch_code, 
                    PUT.Lot_Code, PUT.item_det_code,  PUT.Comm_Calc_Code, PUH.Tax_Reg_Code, PUT.Godown_Code, PUT.Comm_it_Desc_Code, PUH.cust_code, --Join Cond
                    PUH.act_code, PUH.agent_code,  --Join Cond
                    PUH.gross_amt, PUH.net_amt,  --Controv
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate,PUT.Calc_Gross_Amt as GrossAmount1, PUT.Calc_Net_Amt as NetAmount1,--POAna
                    PUT.Tot_Qty/PUT.CF_Qty as Total_Packs1, PUT.rate * PUT.CF_Qty as Rate_Per_Pack, PUH.net_amt-PUH.bill_amount as BillDifference,
                    
                    PUH.Vouch_Num AS VOUCHNO, PUH.Grn_Number, PUH.Bill_Amount, PUH.Goods_In_Transit,
                    PUH.Branch_Code,PUH.config_code,PUT.Lot_Sch_Code,PUT.Carton_Code,PUT.Calc_Sale_Amt,  --New Add1
                    PUH.total_packs,PUH.VouchType,PUT.Pur_Or_PR, --NewAdd2
                    PUH.Bill_No AS BillNo,PUH.GRN_PreFix,PUH.Stock_Trans, --Added Apr21
                    
                    PUH1.gr_date as GRDate, PUH1.gr_number as GRNo
                    FROM """+tbname1+""" AS PUT WITH (NOLOCK) LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK)
                    ON PUT.vouch_code = PUH.vouch_code 
                    LEFT JOIN """+tbname3+""" AS PUH1 ON PUT.vouch_code=PUH1.vouch_code WHERE PUH.Deleted_ = 0 AND MONTH(PUH.Vouch_Date)= """+CM+") AS data1"
            #tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+CM+") AS data1"
            
            ##,SL_TXN.Calc_Commission  , SL_TXN.Vouch_Code
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.VoucherDate)))
            
            ######################-PLAIN VALUE DICT ERROR APR 14#########################
            table=table.withColumn("totalpurqty",table["totalpurqty"].cast("decimal(19,4)")).withColumn("qty",table["qty"].cast("decimal(19,4)"))
            table=table.withColumn("tax2",table["tax2"].cast("decimal(19,4)")).withColumn("tax3",table["tax3"].cast("decimal(19,4)"))
            table=table.withColumn("debit_note",table["debit_note"].cast("decimal(19,4)")).withColumn("userdefinednetamount",table["userdefinednetamount"].cast("decimal(19,4)"))
            table=table.withColumn("cd",table["cd"].cast("decimal(19,4)")).withColumn("td",table["td"].cast("decimal(19,4)"))
            table=table.withColumn("spcd",table["spcd"].cast("decimal(19,4)")).withColumn("labour",table["labour"].cast("decimal(19,4)"))
            
            table=table.withColumn("free_qty",table["free_qty"].cast("decimal(19,4)")).withColumn("repl_qty",table["repl_qty"].cast("decimal(19,4)"))
            table=table.withColumn("adjustment",table["adjustment"].cast("decimal(19,4)")).withColumn("freight",table["freight"].cast("decimal(19,4)"))
            table=table.withColumn("spdiscount",table["spdiscount"].cast("decimal(19,4)")).withColumn("schemeperunit",table["schemeperunit"].cast("decimal(19,4)"))
            table=table.withColumn("schemers",table["schemers"].cast("decimal(19,4)")).withColumn("units_per_pack",table["units_per_pack"].cast("decimal(19,4)"))
            table=table.withColumn("excisepercentage",table["excisepercentage"].cast("decimal(19,4)")).withColumn("exciseperunit",table["exciseperunit"].cast("decimal(19,4)"))
            
            table=table.withColumn("creditnote",table["creditnote"].cast("decimal(19,4)")).withColumn("display",table["display"].cast("decimal(19,4)"))
            table=table.withColumn("handling",table["handling"].cast("decimal(19,4)")).withColumn("postage",table["postage"].cast("decimal(19,4)"))
            table=table.withColumn("sample_qty",table["sample_qty"].cast("decimal(19,4)")).withColumn("rate",table["rate"].cast("double"))
            table=table.withColumn("excise",table["excise"].cast("decimal(19,4)")).withColumn("adjustmentperunit",table["adjustmentperunit"].cast("decimal(19,4)"))
            table=table.withColumn("totalweight",table["totalweight"].cast("double")).withColumn("vouch_code",table["vouch_code"].cast("integer"))
            
            table=table.withColumn("lot_code",table["lot_code"].cast("integer")).withColumn("item_det_code",table["item_det_code"].cast("integer"))
            table=table.withColumn("comm_calc_code",table["comm_calc_code"].cast("integer")).withColumn("tax_reg_code",table["tax_reg_code"].cast("integer"))
            table=table.withColumn("godown_code",table["godown_code"].cast("integer")).withColumn("comm_it_desc_code",table["comm_it_desc_code"].cast("integer"))
            table=table.withColumn("cust_code",table["cust_code"].cast("integer")).withColumn("act_code",table["act_code"].cast("integer"))
            table=table.withColumn("agent_code",table["agent_code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))\
                    .withColumn("yearmonth",table["yearmonth"].cast("integer"))
            
            table=table.withColumn("gross_amt",table["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",table["net_amt"].cast("decimal(19,4)"))
            table=table.withColumn("grnno",table["grnno"].cast("decimal(19,4)"))
            table=table.withColumn("grossamount1",table["grossamount1"].cast("decimal(19,4)")).withColumn("netamount1",table["netamount1"].cast("decimal(19,4)"))
            table=table.withColumn("total_packs",table["total_packs"].cast("decimal(19,4)")).withColumn("billdifference",table["billdifference"].cast("decimal(19,4)"))
            table=table.withColumn("grn_number",table["grn_number"].cast("decimal(19,4)")).withColumn("bill_amount",table["bill_amount"].cast("decimal(19,4)"))
            
            
            table=table.withColumn("billdate",table["billdate"].cast("timestamp")).withColumn("voucherdate",table["voucherdate"].cast("timestamp"))
            table=table.withColumn("grdate",table["grdate"].cast("timestamp"))#.withColumn("",table[""].cast("timestamp"))
            
            table=table.withColumn("rate_per_pack",table["rate_per_pack"].cast("double"))
            
            table=table.withColumn("vouchno",table["vouchno"].cast("string")).withColumn("grno",table["grno"].cast("string"))\
                    .withColumn("entityname",table["entityname"].cast("string")).withColumn("dbname",table["dbname"].cast("string"))
            
            table=table.withColumn("goods_in_transit",table["goods_in_transit"].cast("boolean"))
            
            ##################-------PLAIN VALUE DICT ERROR APR 14 END---------###############
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table = table.drop_duplicates()     ####### 14 MAR
            
            table.cache()
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn/yearmonth="+pastFY+CM)
            
            print("SUCCESSSSSSSSSSSSSS  RAN 2 MONTHS INCREMENTAL")
            cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn").count()
            Tcount=tablePM.count()+table.count()
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'13_PurchaseAnalysisS1','DB':DB,'EN':Etn,
                    'Status':'Completed','ErrorLineNo':'NA','Operation':'Incremental','Rows':str(Tcount),'BeforeETLRows':str(bcnt),'AfterETLRows':str(cnt)}]
            log_df = sqlctx.createDataFrame(log_dict,schema_log)
            log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")

#             end_time = datetime.datetime.now()
#             endtime = end_time.strftime('%H:%M:%S')
#             etime = str(end_time-start_time)
#             etime = etime.split('.')[0]
#             try:
#                 IDEorBatch = sys.argv[1]
#             except Exception as e :
#                 IDEorBatch = "IDLE"
#             log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':tbname1,'DB':DB,'EN':Etn,'Status':'Completed',\
#                     'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':table.count(),'Columns':len(table.columns),'Source':IDEorBatch}]
#             log_df = sqlctx.createDataFrame(log_dict,schema_log)
#             log_df.write.mode(apmode).save(hdfspath+"/Logs")
            
        else:
            ########### DIFFERENT FISCAL YEAR  ##########
            #################################1st part PREVIOUS MONTH OF PASTFY###################
            print("DIFFERENT FISCAL YEAR")
            bcnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn").count()
            tbname1=tbname1+str(pastFY)
            tbname=tbname1                  ##
            tbname2=tbname2+str(pastFY)
            tbname3=tbname3+str(pastFY)
            # 29 feb
            #tbname = TB_Name+tbname
            #changed Below1
            #if tbname1 =='Sl_Txn20192020':
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
            
            print(tbname)
            
            tablesPM = """(SELECT PUT.Tot_Qty as TotalPurQty,    PUT.CF_Qty as Qty,PUT.Calc_Tax_2 as Tax2,    PUT.Calc_Tax_3    as Tax3, PUT.Calc_DN as Debit_Note,
                    (0 + PUT.Calc_Gross_Amt + PUT.Calc_commission + PUT.calc_sp_commission + PUT.calc_rdf + PUT.calc_scheme_u + PUT.calc_scheme_rs + PUT.Calc_Tax_1 + PUT.Calc_Tax_2 + PUT.Calc_Tax_3 + PUT.calc_sur_on_tax3 + PUT.calc_mfees + PUT.calc_excise_u + PUT.Calc_adjustment_u + PUT.Calc_adjust_rs + PUT.Calc_freight + PUT.calc_adjust + PUT.Calc_Spdisc + PUT.Calc_DN + PUT.Calc_CN + PUT.Calc_Display + PUT.Calc_Handling + PUT.calc_Postage + PUT.calc_round)  
                    as userDefinedNetAmount, PUT.Calc_Commission as CD,    PUT.Calc_Sp_Commission as TD, PUT.Calc_Rdf AS SpCD, PUT.Calc_Labour AS Labour,
                    PUT.Free_Qty, PUT.Repl_Qty, PUT.calc_adjustment_u As Adjustment,PUT.Calc_Freight AS FREIGHT,PUT.Calc_Spdisc  as SPDiscount,PUT.Calc_Scheme_U  as schemePerunit,
                    PUT.Calc_Scheme_Rs as SchemeRs, PUT.CF_Qty AS Units_Per_Pack, PUT.Calc_MFees  as ExcisePercentage, PUT.Calc_Excise_U as ExcisePerUnit, 
                    PUT.Calc_cn  as CreditNote, PUT.Calc_Display as Display, PUT.Calc_Handling    as Handling, PUT.Calc_Postage as Postage, PUT.Sample_Qty, PUT.rate,    
                    PUT.Excise_Amt_Rate As Excise,  PUT.Calc_Adjustment_u  as AdjustmentPerUnit, PUT.Qty_Weight AS TotalWeight,PUT.vouch_code, 
                    PUT.Lot_Code, PUT.item_det_code,  PUT.Comm_Calc_Code, PUH.Tax_Reg_Code, PUT.Godown_Code, PUT.Comm_it_Desc_Code, PUH.cust_code, --Join Cond
                    PUH.act_code, PUH.agent_code,  --Join Cond
                    PUH.gross_amt, PUH.net_amt,  --Controv
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate,PUT.Calc_Gross_Amt as GrossAmount1, PUT.Calc_Net_Amt as NetAmount1,--POAna
                    PUT.Tot_Qty/PUT.CF_Qty as Total_Packs1, PUT.rate * PUT.CF_Qty as Rate_Per_Pack, PUH.net_amt-PUH.bill_amount as BillDifference,
                    
                    PUH.Vouch_Num AS VOUCHNO, PUH.Grn_Number, PUH.Bill_Amount, PUH.Goods_In_Transit,
                    PUH.Branch_Code,PUH.config_code,PUT.Lot_Sch_Code,PUT.Carton_Code,PUT.Calc_Sale_Amt,  --New Add1
                    PUH.total_packs,PUH.VouchType,PUT.Pur_Or_PR, --NewAdd2
                    PUH.Bill_No AS BillNo,PUH.GRN_PreFix,PUH.Stock_Trans, --Added Apr21
                    
                    PUH1.gr_date as GRDate, PUH1.gr_number as GRNo
                    FROM """+tbname1+""" AS PUT WITH (NOLOCK) LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK)
                    ON PUT.vouch_code = PUH.vouch_code 
                    LEFT JOIN """+tbname3+""" AS PUH1 ON PUT.vouch_code=PUH1.vouch_code WHERE PUH.Deleted_ = 0 AND MONTH(PUH.Vouch_Date)= """+PM+") AS data1"
            #tablesPM = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+PM+") AS data1"
            
            
            tablePM = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tablesPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            tablePM = tablePM.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(tablePM.columns)))
            tablePM = tablePM.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(tablePM.columns)))
            ############FISCAL YAER ADDED
            tablePM = tablePM.withColumn("fiscalyear",lit(pastFY))     ##changed from newFY
            
            ######## YAER MONTH FLAG ADD
            tablePM=tablePM.withColumn("yearmonth",concat(tablePM.fiscalyear,month(tablePM.VoucherDate)))
            
            ######################-PLAIN VALUE DICT ERROR APR 14#########################
            tablePM=tablePM.withColumn("totalpurqty",tablePM["totalpurqty"].cast("decimal(19,4)")).withColumn("qty",tablePM["qty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("tax2",tablePM["tax2"].cast("decimal(19,4)")).withColumn("tax3",tablePM["tax3"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("debit_note",tablePM["debit_note"].cast("decimal(19,4)")).withColumn("userdefinednetamount",tablePM["userdefinednetamount"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("cd",tablePM["cd"].cast("decimal(19,4)")).withColumn("td",tablePM["td"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("spcd",tablePM["spcd"].cast("decimal(19,4)")).withColumn("labour",tablePM["labour"].cast("decimal(19,4)"))
            
            tablePM=tablePM.withColumn("free_qty",tablePM["free_qty"].cast("decimal(19,4)")).withColumn("repl_qty",tablePM["repl_qty"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("adjustment",tablePM["adjustment"].cast("decimal(19,4)")).withColumn("freight",tablePM["freight"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("spdiscount",tablePM["spdiscount"].cast("decimal(19,4)")).withColumn("schemeperunit",tablePM["schemeperunit"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("schemers",tablePM["schemers"].cast("decimal(19,4)")).withColumn("units_per_pack",tablePM["units_per_pack"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("excisepercentage",tablePM["excisepercentage"].cast("decimal(19,4)")).withColumn("exciseperunit",tablePM["exciseperunit"].cast("decimal(19,4)"))
            
            tablePM=tablePM.withColumn("creditnote",tablePM["creditnote"].cast("decimal(19,4)")).withColumn("display",tablePM["display"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("handling",tablePM["handling"].cast("decimal(19,4)")).withColumn("postage",tablePM["postage"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("sample_qty",tablePM["sample_qty"].cast("decimal(19,4)")).withColumn("rate",tablePM["rate"].cast("double"))
            tablePM=tablePM.withColumn("excise",tablePM["excise"].cast("decimal(19,4)")).withColumn("adjustmentperunit",tablePM["adjustmentperunit"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("totalweight",tablePM["totalweight"].cast("double")).withColumn("vouch_code",tablePM["vouch_code"].cast("integer"))
            
            tablePM=tablePM.withColumn("lot_code",tablePM["lot_code"].cast("integer")).withColumn("item_det_code",tablePM["item_det_code"].cast("integer"))
            tablePM=tablePM.withColumn("comm_calc_code",tablePM["comm_calc_code"].cast("integer")).withColumn("tax_reg_code",tablePM["tax_reg_code"].cast("integer"))
            tablePM=tablePM.withColumn("godown_code",tablePM["godown_code"].cast("integer")).withColumn("comm_it_desc_code",tablePM["comm_it_desc_code"].cast("integer"))
            tablePM=tablePM.withColumn("cust_code",tablePM["cust_code"].cast("integer")).withColumn("act_code",tablePM["act_code"].cast("integer"))
            tablePM=tablePM.withColumn("agent_code",tablePM["agent_code"].cast("integer")).withColumn("fiscalyear",tablePM["fiscalyear"].cast("integer"))\
                    .withColumn("yearmonth",tablePM["yearmonth"].cast("integer"))
            
            tablePM=tablePM.withColumn("gross_amt",tablePM["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",tablePM["net_amt"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("grnno",tablePM["grnno"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("grossamount1",tablePM["grossamount1"].cast("decimal(19,4)")).withColumn("netamount1",tablePM["netamount1"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("total_packs",tablePM["total_packs"].cast("decimal(19,4)")).withColumn("billdifference",tablePM["billdifference"].cast("decimal(19,4)"))
            tablePM=tablePM.withColumn("grn_number",tablePM["grn_number"].cast("decimal(19,4)")).withColumn("bill_amount",tablePM["bill_amount"].cast("decimal(19,4)"))
            
            
            tablePM=tablePM.withColumn("billdate",tablePM["billdate"].cast("timestamp")).withColumn("voucherdate",tablePM["voucherdate"].cast("timestamp"))
            tablePM=tablePM.withColumn("grdate",tablePM["grdate"].cast("timestamp"))#.withColumn("",tablePM[""].cast("timestamp"))
            
            tablePM=tablePM.withColumn("rate_per_pack",tablePM["rate_per_pack"].cast("double"))
            
            tablePM=tablePM.withColumn("vouchno",tablePM["vouchno"].cast("string")).withColumn("grno",tablePM["grno"].cast("string"))\
                    .withColumn("entityname",tablePM["entityname"].cast("string")).withColumn("dbname",tablePM["dbname"].cast("string"))
            
            tablePM=tablePM.withColumn("goods_in_transit",tablePM["goods_in_transit"].cast("boolean"))
            
            ##################-------PLAIN VALUE DICT ERROR APR 14 END---------###############
            
            tablePM=tablePM.toDF(*[c.lower() for c in tablePM.columns])   #LowerCase the header of current DF
            
            tablePM = tablePM.drop_duplicates()     ####### 14 MAR
            tablePM.cache()
            tablePM.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn/yearmonth="+pastFY+PM)
            
            
            
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
            tbname1='Pur_Txn'
            tbname2='Pur_Head'
            tbname3='Pur_Head1'
            tbname1=tbname1+str(presentFY)
            tbname=tbname1                  #
            tbname2=tbname2+str(presentFY)
            tbname3=tbname3+str(presentFY)
            
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
            
            print(tbname)
            #print(tbname3)
            
            #tables = "(SELECT "+temp+" FROM ["+tbname+"] WHERE MONTH(date_) = "+CM+") AS data1"
            tables = """(SELECT PUT.Tot_Qty as TotalPurQty,    PUT.CF_Qty as Qty,PUT.Calc_Tax_2 as Tax2,    PUT.Calc_Tax_3    as Tax3, PUT.Calc_DN as Debit_Note,
                    (0 + PUT.Calc_Gross_Amt + PUT.Calc_commission + PUT.calc_sp_commission + PUT.calc_rdf + PUT.calc_scheme_u + PUT.calc_scheme_rs + PUT.Calc_Tax_1 + PUT.Calc_Tax_2 + PUT.Calc_Tax_3 + PUT.calc_sur_on_tax3 + PUT.calc_mfees + PUT.calc_excise_u + PUT.Calc_adjustment_u + PUT.Calc_adjust_rs + PUT.Calc_freight + PUT.calc_adjust + PUT.Calc_Spdisc + PUT.Calc_DN + PUT.Calc_CN + PUT.Calc_Display + PUT.Calc_Handling + PUT.calc_Postage + PUT.calc_round)  
                    as userDefinedNetAmount, PUT.Calc_Commission as CD,    PUT.Calc_Sp_Commission as TD, PUT.Calc_Rdf AS SpCD, PUT.Calc_Labour AS Labour,
                    PUT.Free_Qty, PUT.Repl_Qty, PUT.calc_adjustment_u As Adjustment,PUT.Calc_Freight AS FREIGHT,PUT.Calc_Spdisc  as SPDiscount,PUT.Calc_Scheme_U  as schemePerunit,
                    PUT.Calc_Scheme_Rs as SchemeRs, PUT.CF_Qty AS Units_Per_Pack, PUT.Calc_MFees  as ExcisePercentage, PUT.Calc_Excise_U as ExcisePerUnit, 
                    PUT.Calc_cn  as CreditNote, PUT.Calc_Display as Display, PUT.Calc_Handling    as Handling, PUT.Calc_Postage as Postage, PUT.Sample_Qty, PUT.rate,    
                    PUT.Excise_Amt_Rate As Excise,  PUT.Calc_Adjustment_u  as AdjustmentPerUnit, PUT.Qty_Weight AS TotalWeight,PUT.vouch_code, 
                    PUT.Lot_Code, PUT.item_det_code,  PUT.Comm_Calc_Code, PUH.Tax_Reg_Code, PUT.Godown_Code, PUT.Comm_it_Desc_Code, PUH.cust_code, --Join Cond
                    PUH.act_code, PUH.agent_code,  --Join Cond
                    PUH.gross_amt, PUH.net_amt,  --Controv
                    PUH.Bill_Date AS BillDate, PUH.Grn_Number As GrnNo, PUH.Vouch_Date AS VoucherDate,PUT.Calc_Gross_Amt as GrossAmount1, PUT.Calc_Net_Amt as NetAmount1,--POAna
                    PUT.Tot_Qty/PUT.CF_Qty as Total_Packs1, PUT.rate * PUT.CF_Qty as Rate_Per_Pack, PUH.net_amt-PUH.bill_amount as BillDifference,
                    
                    PUH.Vouch_Num AS VOUCHNO, PUH.Grn_Number, PUH.Bill_Amount, PUH.Goods_In_Transit,
                    PUH.Branch_Code,PUH.config_code,PUT.Lot_Sch_Code,PUT.Carton_Code,PUT.Calc_Sale_Amt,  --New Add1
                    PUH.total_packs,PUH.VouchType,PUT.Pur_Or_PR, --NewAdd2
                    PUH.Bill_No AS BillNo,PUH.GRN_PreFix,PUH.Stock_Trans, --Added Apr21
                    
                    PUH1.gr_date as GRDate, PUH1.gr_number as GRNo
                    FROM """+tbname1+""" AS PUT WITH (NOLOCK) LEFT JOIN """+tbname2+""" AS PUH WITH (NOLOCK)
                    ON PUT.vouch_code = PUH.vouch_code 
                    LEFT JOIN """+tbname3+""" AS PUH1 ON PUT.vouch_code=PUH1.vouch_code WHERE PUH.Deleted_ = 0 AND MONTH(PUH.Vouch_Date)= """+CM+") AS data1"
            
            table = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=tables,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
            
            table = table.withColumn('EntityName',lit(Etn)).withColumn('DBName',lit(DB))
            table = table.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(table.columns)))
            table = table.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(table.columns)))
            ############FISCAL YAER ADDED
            table = table.withColumn("fiscalyear",lit(presentFY))     ##changed from newFY
            
            
            ######## YAER MONTH FLAG ADD
            table=table.withColumn("yearmonth",concat(table.fiscalyear,month(table.VoucherDate)))
            
            ######################-PLAIN VALUE DICT ERROR APR 14#########################
            table=table.withColumn("totalpurqty",table["totalpurqty"].cast("decimal(19,4)")).withColumn("qty",table["qty"].cast("decimal(19,4)"))
            table=table.withColumn("tax2",table["tax2"].cast("decimal(19,4)")).withColumn("tax3",table["tax3"].cast("decimal(19,4)"))
            table=table.withColumn("debit_note",table["debit_note"].cast("decimal(19,4)")).withColumn("userdefinednetamount",table["userdefinednetamount"].cast("decimal(19,4)"))
            table=table.withColumn("cd",table["cd"].cast("decimal(19,4)")).withColumn("td",table["td"].cast("decimal(19,4)"))
            table=table.withColumn("spcd",table["spcd"].cast("decimal(19,4)")).withColumn("labour",table["labour"].cast("decimal(19,4)"))
            
            table=table.withColumn("free_qty",table["free_qty"].cast("decimal(19,4)")).withColumn("repl_qty",table["repl_qty"].cast("decimal(19,4)"))
            table=table.withColumn("adjustment",table["adjustment"].cast("decimal(19,4)")).withColumn("freight",table["freight"].cast("decimal(19,4)"))
            table=table.withColumn("spdiscount",table["spdiscount"].cast("decimal(19,4)")).withColumn("schemeperunit",table["schemeperunit"].cast("decimal(19,4)"))
            table=table.withColumn("schemers",table["schemers"].cast("decimal(19,4)")).withColumn("units_per_pack",table["units_per_pack"].cast("decimal(19,4)"))
            table=table.withColumn("excisepercentage",table["excisepercentage"].cast("decimal(19,4)")).withColumn("exciseperunit",table["exciseperunit"].cast("decimal(19,4)"))
            
            table=table.withColumn("creditnote",table["creditnote"].cast("decimal(19,4)")).withColumn("display",table["display"].cast("decimal(19,4)"))
            table=table.withColumn("handling",table["handling"].cast("decimal(19,4)")).withColumn("postage",table["postage"].cast("decimal(19,4)"))
            table=table.withColumn("sample_qty",table["sample_qty"].cast("decimal(19,4)")).withColumn("rate",table["rate"].cast("double"))
            table=table.withColumn("excise",table["excise"].cast("decimal(19,4)")).withColumn("adjustmentperunit",table["adjustmentperunit"].cast("decimal(19,4)"))
            table=table.withColumn("totalweight",table["totalweight"].cast("double")).withColumn("vouch_code",table["vouch_code"].cast("integer"))
            
            table=table.withColumn("lot_code",table["lot_code"].cast("integer")).withColumn("item_det_code",table["item_det_code"].cast("integer"))
            table=table.withColumn("comm_calc_code",table["comm_calc_code"].cast("integer")).withColumn("tax_reg_code",table["tax_reg_code"].cast("integer"))
            table=table.withColumn("godown_code",table["godown_code"].cast("integer")).withColumn("comm_it_desc_code",table["comm_it_desc_code"].cast("integer"))
            table=table.withColumn("cust_code",table["cust_code"].cast("integer")).withColumn("act_code",table["act_code"].cast("integer"))
            table=table.withColumn("agent_code",table["agent_code"].cast("integer")).withColumn("fiscalyear",table["fiscalyear"].cast("integer"))\
                    .withColumn("yearmonth",table["yearmonth"].cast("integer"))
            
            table=table.withColumn("gross_amt",table["gross_amt"].cast("decimal(19,4)")).withColumn("net_amt",table["net_amt"].cast("decimal(19,4)"))
            table=table.withColumn("grnno",table["grnno"].cast("decimal(19,4)"))
            table=table.withColumn("grossamount1",table["grossamount1"].cast("decimal(19,4)")).withColumn("netamount1",table["netamount1"].cast("decimal(19,4)"))
            table=table.withColumn("total_packs",table["total_packs"].cast("decimal(19,4)")).withColumn("billdifference",table["billdifference"].cast("decimal(19,4)"))
            table=table.withColumn("grn_number",table["grn_number"].cast("decimal(19,4)")).withColumn("bill_amount",table["bill_amount"].cast("decimal(19,4)"))
            
            
            table=table.withColumn("billdate",table["billdate"].cast("timestamp")).withColumn("voucherdate",table["voucherdate"].cast("timestamp"))
            table=table.withColumn("grdate",table["grdate"].cast("timestamp"))#.withColumn("",table[""].cast("timestamp"))
            
            table=table.withColumn("rate_per_pack",table["rate_per_pack"].cast("double"))
            
            table=table.withColumn("vouchno",table["vouchno"].cast("string")).withColumn("grno",table["grno"].cast("string"))\
                    .withColumn("entityname",table["entityname"].cast("string")).withColumn("dbname",table["dbname"].cast("string"))
            
            table=table.withColumn("goods_in_transit",table["goods_in_transit"].cast("boolean"))
            
            ##################-------PLAIN VALUE DICT ERROR APR 14 END---------###############
            
            table=table.toDF(*[c.lower() for c in table.columns])   #LowerCase the header of current DF
            
            table = table.drop_duplicates()     ####### 14 MAR
            
            table.cache()
            table.write.mode(owmode).save(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn/yearmonth="+presentFY+CM)
            
            print("SUCCESSSSSSSSSSSSSSSSSSSSS RAN TWO MONTHS INCREMENTAL")
            cnt=sqlctx.read.parquet(hdfspath+"/"+DBET+"/Stage1/Pur_HeadPur_Txn").count()
            Tcount=tablePM.count()+table.count()
            end_time = datetime.datetime.now()
            endtime = end_time.strftime('%H:%M:%S')
            etime = str(end_time-start_time)
            etime = etime.split('.')[0]
            log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'13_PurchaseAnalysisS1','DB':DB,'EN':Etn,
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'13_PurchaseAnalysisS1','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
