from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import concat_ws, when, substring,sum,split,month,dayofmonth,year,avg,count,lit
from pyspark.sql.functions import last
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udl
import pandas as pd
from pyspark.sql.types import IntegerType
import calendar
#from calendar import month

#from pandas.tests.tseries.frequencies.test_inference import day
#from _overlapped import NULL
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
    
    conf = SparkConf().setMaster('local[*]')\
                      .setAppName("StockDetailing")\
                      .set("spark.executor.memory","30g")\
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.getOrCreate()
   
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    connection_string="Driver="+ODBC13+";Server="+SURLCUR+","+PORTCUR+";Database="+WDB+";uid="+RUSER+";pwd="+RPASS

    ACT= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Accounts")
    CM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comp_Mst")
    BM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
    BM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
    IMH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
    IMD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")
    ICM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
    GM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    PM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
    LM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
    IMN= sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
    CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
    SLHD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_HD1")
    SM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Store_Mst")
    #CCI1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
    CH=sqlctx.read.parquet(hdfspath+"/Market/Stage1/Chal_Head")
    df1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_Txn1")
    df2 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Chal_Txn")
    
    
    
    
    ACT=ACT.select('Act_Name','Act_Code','Address_1','User_Code')
    BM=BM.select('Branch_Name', 'Branch_Code','Show_ST_As_Sale')
    #BM1=BM1.select('Show_ST_As_Sale','Branch_Code')
    IMH=IMH.select('Item_Hd_Code','comp_code','Color_Code','Group_Code','Item_Name_Code' )
    IMD=IMD.select('Item_Det_Code','Pack_Code','Item_Hd_Code' )
    CM=CM.select('Comp_Code')
    ICM=ICM.select('Color_Code')
    GM=GM.select('Group_Code')
    PM=PM.select('Pack_Code')
    IMN=IMN.select('Item_Name_Code')
    LM=LM.select('Basic_rate','Lot_Code','Item_Det_Code')
    CCI=CCI.select('Code','Sl_Act_Code','STax_Act_Code','Sur_Act_Code','STax_Act_Code_1','ST_Branch_Code')
    #CCI1=CCI1.select('Code','ST_Branch_Code')
    SM=SM.select('Code')
    
    SLHD=SLHD.select ('Vouch_Date','Vouch_Time', 'Store_Code','Branch_Code','Stock_Trans','Vouch_Code','New_Vouch_num',\
                      'Pay_Mode','Series_Code','number_' ,'Chq_No' ,'Chq_Date','Comm_Calc_Code','cust_Code','Deleted','Vouch_Num')
    SLHD=SLHD.filter(SLHD.Branch_Code == 5024)
    
    df1=df1.select('Sa_Subs_Lot','Tot_Qty','Calc_Gross_Amt','Calc_Spdisc','Calc_DN','Calc_cn','Calc_Display','Calc_Handling','Calc_Postage',\
                 'Calc_Net_Amt','Calc_Commission','Calc_Sp_Commission','Calc_Rdf','Calc_Scheme_U','Calc_Scheme_Rs','Calc_MFees','Calc_Labour','calc_round','Calc_Tax_1',\
                 'Calc_tax_2','Calc_Tax_3','Calc_Sur_On_Tax3','Calc_Excise_U','Calc_Adjustment_u','Calc_Adjust_RS','Calc_Freight','Calc_Adjust','Sale_Or_SR','Vouch_Code',\
                 'Lot_Code','Godown_Code','Item_Det_Code','Comm_Calc_Code')
    
    df1 = df1.join(IMD,on = ['Item_Det_Code'],how='left' )
    df1 = df1.join(CCI,df1.Comm_Calc_Code==CCI.Code,how='left' )
    df1 = df1.join(SLHD,on=['Vouch_Code'],how='left' ) 
    df1 = df1.join(BM,on = ['Branch_Code'], how = 'left')
    df1 = df1.join(LM,on =['Lot_Code'],how='left' )
    #df1 = df1.join(LM,on =['Item_det_Code'],how='left' )
    df1 = df1.join(SM,df1.Store_Code==SM.Code,how='left' )    
    df1 = df1.join(ACT,df1.cust_Code==ACT.Act_Code,how='left' )
    df1 = df1.join(PM,on = ['Pack_Code'],how='left' )
    df1 = df1.join(IMH,on = ['Item_Hd_Code'], how = 'left')
    df1 = df1.join(CM,on = ['comp_code'], how = 'left')
    df1 = df1.join(ICM,on = ['Color_Code'], how = 'left')
    df1 = df1.join(GM,on = ['Group_Code'], how = 'left')
    df1 = df1.join(IMN,on = ['Item_Name_Code'], how = 'left')#.drop('Group_Code').drop('Comm_Calc_Code')
    #df1 = df1.join(CCI1,df1.Comm_Calc_Code==CCI1.Code,how='left' )
    #df1 = df1.join(BM1,df1.ST_Branch_Code==BM1.Branch_Code,how='left' )
    
    #print(df1.columns)
    #exit() 

    
    df1 = df1.filter(df1.Sa_Subs_Lot == 0)\
             .filter(df1.Deleted == 0)\
             .filter((df1.Stock_Trans == 0) | (((df1.Stock_Trans) == 1) & (df1.Show_ST_As_Sale == 1)))
    
    
    df1=df1.withColumn('Tot_Tax3_Amt',df1['calc_tax_3']+ df1['calc_sur_on_tax3'])\
                .withColumn('TotQty',df1['Tot_Qty']/1).withColumn('UDAmt',df1['Tot_Qty'] * LM['Basic_rate'])\
                .withColumn('UdNet_Amt_2' ,'0'  + df1['Calc_commission'] + df1['calc_sp_commission'] + df1['calc_rdf'] + df1['calc_scheme_u'] + df1['calc_scheme_rs'] + df1['calc_mfees'] + df1['calc_excise_u'] + df1['Calc_adjustment_u'] + df1['Calc_adjust_rs'] + df1['Calc_freight'] + df1['calc_adjust'] + df1['Calc_Spdisc'] + df1['Calc_DN'] + df1['Calc_CN'] + df1['Calc_Display'] + df1['Calc_Handling'] + df1['calc_Postage'] + df1['calc_Round'] + df1['calc_Labour'])\
                .withColumn("Store_Name", lit('')).withColumn("Cashier_Name", lit('')).withColumn("Cashier_Code", lit(0)).withColumn("credit_amount", lit(0)).withColumn("cash_amount", lit(0)).withColumn("Gift_Vouch_Amount", lit(0)).withColumn("cheque_amount", lit(0))
 
    
    df1=df1.groupby('Vouch_Date','Vouch_Time','Vouch_Num','Act_Name','Act_Code','Address_1','User_Code','Pay_Mode','Series_Code','number_' ,'Chq_No' ,'Chq_Date',\
                    'Sl_Act_Code','STax_Act_Code','Sur_Act_Code','STax_Act_Code_1','Sale_Or_Sr','New_Vouch_num')\
               .agg(sum('calc_tax_1').alias('Tot_Tax1_Amt'),sum('STax_Act_Code_1').alias('Tax_Act_Code_1'), sum('Tot_Qty').alias('Tot_Qty'), sum('Calc_Gross_Amt').alias('Gross_Amt'),sum('UDAmt').alias('UDAmt') , sum('Calc_Net_Amt').alias('Calc_Net_Amt'),\
                    sum('UdNet_Amt_2').alias('UdNet_Amt_2'),sum('Tot_Tax3_Amt').alias('Tot_Tax3_Amt')) 
               
               
    #df1.show(1)
    
    
    
    CH=CH.select ('Vouch_Date','Vouch_Time', 'Store_Code','Branch_Code','Stock_Trans','Vouch_Code','Post_In_Accounts',\
                      'Pay_Mode','Series_Code','number_' ,'Chq_No' ,'Chq_Date','Comm_Calc_Code','cust_Code','Deleted','Vouch_Num')
    CH=CH.filter(CH.Branch_Code == 5024).filter(CH.Post_In_Accounts == 1)
    
         
    
    df2=df2.select('Org_Qty','Sa_Subs_Lot','Calc_Gross_Amt','Calc_Spdisc','Calc_DN','Calc_cn','Calc_Display','Calc_Handling','Calc_Postage',\
                 'Calc_Net_Amt','Calc_Commission','Calc_Sp_Commission','Calc_Rdf','Calc_Scheme_U','Calc_Scheme_Rs','Calc_MFees','Calc_Labour','calc_round','Calc_Tax_1',\
                 'Calc_tax_2','Calc_Tax_3','Calc_Sur_On_Tax3','Calc_Excise_U','Calc_Adjustment_u','Calc_Adjust_RS','Calc_Freight','Calc_Adjust','Sale_Or_SR','Vouch_Code',\
                 'Lot_Code','Item_Det_Code','Comm_Calc_Code')
    
    df2 = df2.join(IMD,on = ['Item_Det_Code'],how='left' )
    df2 = df2.join(CCI,df2.Comm_Calc_Code==CCI.Code,how='left' )
    df2 = df2.join(CH,on=['Vouch_Code'],how='left' ) 
    df2 = df2.join(BM,on = ['Branch_Code'], how = 'left')
    df2 = df2.join(LM,on =['Lot_Code'],how='left' )
    #df2 = df2.join(LM,on =['Item_det_Code'],how='left' )
    df2 = df2.join(SM,df2.Store_Code==SM.Code,how='left' )    
    df2 = df2.join(ACT,df2.cust_Code==ACT.Act_Code,how='left' )
    df2 = df2.join(PM,on = ['Pack_Code'],how='left' )
    df2 = df2.join(IMH,on = ['Item_Hd_Code'], how = 'left')
    df2 = df2.join(CM,on = ['comp_code'], how = 'left')
    df2 = df2.join(ICM,on = ['Color_Code'], how = 'left')
    df2 = df2.join(GM,on = ['Group_Code'], how = 'left')
    df2 = df2.join(IMN,on = ['Item_Name_Code'], how = 'left')#.drop('Group_Code').drop('Comm_Calc_Code')
    #print(df2.columns)
    #df2.write.jdbc(url=Sqlurlwrite, table="CHtest", mode="overwrite")
    #exit()

    df2 = df2.filter(df2.Sa_Subs_Lot == 0).filter(df2.Deleted == 0)\
            .filter((df2.Stock_Trans == 0) | ((df2.Stock_Trans == 1) & (df2.Show_ST_As_Sale == 1)))
    
    #exit()
    
    #.filter(df2.Branch_Code == 5024)\
             #.filter(df2.Post_In_Accounts == 1).filter(df2.Deleted == 0)\
             #.filter((df2.Stock_Trans == 0) | ((df2.Stock_Trans == 1) & (df2.Show_ST_As_Sale == 1)))
    
    
    df2=df2.withColumn('Tot_Tax3_Amt',df2['calc_tax_3']+ df2['calc_sur_on_tax3'])\
                .withColumn('Tot_Qty',df2['Org_Qty']/1).withColumn('UDAmt',df2['Org_Qty'] * LM['Basic_rate'])\
                .withColumn('UdNet_Amt_2','0' + df2['Calc_commission'] +  df2['calc_sp_commission'] +  df2['calc_rdf'] +  df2['calc_scheme_u'] +  df2['calc_scheme_rs'] +  df2['calc_mfees'] +  df2['calc_excise_u'] +  df2['Calc_adjustment_u'] +  df2['Calc_adjust_rs'] +  df2['Calc_freight'] +  df2['calc_adjust'] +  df2['Calc_Spdisc'] +  df2['Calc_DN'] +  df2['Calc_CN'] +  df2['Calc_Display'] +  df2['Calc_Handling'] +  df2['calc_Postage'] +  df2['calc_Round'] +  df2['calc_Labour'])  \
                .withColumn("Store_Name", lit('')).withColumn("Cashier_Name", lit('')).withColumn("Cashier_Code", lit(0)).withColumn("credit_amount", lit(0)).withColumn("cash_amount", lit(0)).withColumn("Gift_Vouch_Amount", lit(0)).withColumn("cheque_amount", lit(0)).withColumn("New_Vouch_num", lit(''))
    #df2.show(1)
    #exit()

    df2=df2.groupby('Vouch_Date','Vouch_Time','Vouch_Num','Act_Name','Act_Code','Address_1','User_Code','Pay_Mode','Series_Code','number_' ,'Chq_No' ,'Chq_Date',\
                    'Sl_Act_Code','STax_Act_Code','Sur_Act_Code','STax_Act_Code_1','Sale_Or_Sr','New_Vouch_num')\
            .agg(sum('calc_tax_1').alias('Tot_Tax1_Amt'),sum('STax_Act_Code_1').alias('Tax_Act_Code_1'),\
                 sum('Tot_Qty').alias('Tot_Qty'), sum('Calc_Gross_Amt').alias('Gross_Amt'),sum('UDAmt').alias('UDAmt') , \
                 sum('Calc_Net_Amt').alias('Calc_Net_Amt'),\
                 sum('UdNet_Amt_2').alias('UdNet_Amt_2'),sum('Tot_Tax3_Amt').alias('Tot_Tax3_Amt')) 
               
    
    #df2.show(1)
    
    #df2.write.jdbc(url=Sqlurlwrite, table="CHtest", mode="overwrite")
    
    df3 = df1.union(df2)
    df3.show(1)
    df3.write.jdbc(url=Sqlurlwrite, table="Tendor", mode="overwrite")
    
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'Single_Table','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")    