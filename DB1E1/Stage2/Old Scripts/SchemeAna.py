from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import concat_ws, when, substring,sum,split,month,dayofmonth,year,avg,count,lit
from pyspark.sql.functions import expr, col
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

    def RENAME(df,columns):
        if isinstance(columns, dict):
                for old_name, new_name in columns.items():
                        df = df.withColumnRenamed(old_name, new_name)
                return df
        else:
                raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
       

    
    BM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Mst")
    IMH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
    IMH1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Hd")
    IMD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")
    IMD1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det") 
    IMD2 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Det")
    CM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comp_Mst")
    GM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    GM11 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    GM111 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    GM21 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    GM31 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    GM121 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Group_Mst")
    
    ICM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
    ICM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
    ICM2 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Item_Color_Mst")
    
    PM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
    PM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")  
    PM2 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Pack_Mst")
      
    LM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Lot_Mst")
    IMN= sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
    IMN1= sqlctx.read.parquet(hdfspath+"/Market/Stage1/It_Mst_Names")
    CID = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_It_Desc")
    CID1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_It_Desc")
    CID2 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_It_Desc")
    
    SLHD = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_HD")
    SLHD1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_HD")
    SLTXN = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_Txn")
    SLTXN1= sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_Txn")
    BG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Branch_Groups")
    SS = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_Sch")
    SS1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/SL_Sch")
    SCH = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCM2= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCM3 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCM4 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCM5 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    SCGM = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Mst")
    SCGM1 = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Mst")

    CCI = sqlctx.read.parquet(hdfspath+"/Market/Stage1/Comm_Calc_Info")
    AG = sqlctx.read.parquet(hdfspath+"/Market/Stage1/AccountGroups")
    SCDET1= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Det")
    SCDET2= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Det")
    SCDET4= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Det")
    SCDET5= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Det")
    
    ACT= sqlctx.read.parquet(hdfspath+"/Market/Stage1/Accounts")
    BG=BG.select('Group_Code','Group_Name')
    BG=RENAME(BG,{"Group_Code":"Group_Code1","Group_Name":"GroupName"})
    
    BM=BM.select('Group_Code1','Branch_Name', 'Branch_Code')
    IMH=IMH.select('Item_Name_Code','Item_Hd_Code','Item_Hd_Name','comp_code','Color_Code','Group_Code','Comm_It_Desc_Code' )
    IMD=IMD.select('User_Code','Item_Det_Code','Pack_Code','Item_Hd_Code' )
    CM=CM.select('Comp_Code','Comp_Name')
    PM=PM.select('Pack_Code' , 'Pack_Name', 'Order_' ,'Pack_Short_Name')
    ICM=ICM.select('Color_Code')
    GM=GM.select('Group_Code','Group_Name')
    IMN=IMN.select('Item_Name_Code')
    CID=CID.select( 'Item_Desc','Code')
    LM=LM.select( 'Lot_Code')
    
    SS =SS.select('Code_Type','Sch_Det_Code','SL_Txn_Code') \
            .filter(SS.Code_Type== 'SCHCMP').withColumnRenamed("SL_Txn_Code","Code")
    
    SCH=SCH.select('Code','Scheme_Campaign_Name')
    
    SCGM=SCGM.select('Code','Scheme_Group_Name','Scheme_Campaign_Code')
    SCGM=RENAME(SCGM,{"Code":"SCGM_Code"})
    
    
    SLHD=SLHD.select('Net_Amt','Vouch_Date' , 'Vouch_Code','Deleted','Branch_Code')\
                .filter(SLHD.Deleted== 0)
    

    SLTXN1=SLTXN1.select('Code','Tot_Qty','Calc_Net_Amt','Sale_Or_Sr','Calc_adjust_rs','Comm_Calc_Code','Vouch_Code',\
                       'Sa_Subs_Lot','Item_Det_Code','Lot_Code','Calc_Gross_Amt', 'Calc_commission', 'calc_sp_commission', 'calc_rdf', 'calc_scheme_u', 'calc_scheme_rs', 'Calc_Tax_1', 'Calc_Tax_2', 'Calc_Tax_3', 'calc_sur_on_tax3', \
                        'calc_mfees', 'calc_excise_u', 'Calc_adjustment_u',  'Calc_freight', 'calc_adjust', 'Calc_Spdisc', 'Calc_DN', 'Calc_CN', 'Calc_Display', 'Calc_Handling',\
                        'calc_Postage', 'calc_Round', 'calc_Labour')
                        
    
    
    print('SchemeReport start: ', datetime.datetime.now())
    
    SLTXN1 = SLTXN1.join(LM,on =['Lot_Code'],how='left' )
    SLTXN1 = SLTXN1.join(SLHD,on=['Vouch_Code'],how='left' ) 
    SLTXN1 = SLTXN1.join(IMD,on = ['Item_Det_Code'],how='left')
    SLTXN1 = SLTXN1.join(BM,on = ['Branch_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(IMH,on = ['Item_Hd_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(ICM,on = ['Color_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(CM,on = ['comp_code'], how = 'left')
    SLTXN1 = SLTXN1.join(PM,on = ['Pack_Code'],how='left' )
    SLTXN1 = SLTXN1.join(GM,on = ['Group_Code'], how = 'left')
    
    SLTXN1 =  SLTXN1.join(BG,on =['Group_Code1'],how='left' )
    SLTXN1 = SLTXN1.join(IMN,on = ['Item_Name_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(CID,SLTXN1.Comm_It_Desc_Code==CID.Code,how='left' )
    SLTXN1 = SLTXN1.join(SS,on = ['Code'],how='left' )
    #SLTXN1.show(1)
    #exit()
    SLTXN1 = SLTXN1.join(SCGM,SLTXN1.Sch_Det_Code==SCGM.SCGM_Code,how='left' )
    SLTXN1 = SLTXN1.join(SCH,SLTXN1.Scheme_Campaign_Code==SCH.Code,how='left' )
    
    #SLTXN1 = SLTXN1.withColumn('SaleQty',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Tot_Qty'])).otherwise(SLTXN1['Tot_Qty'] * -1))
    #SLTXN1.show(1)
    #exit()
    
    SLTXN1=SLTXN1.withColumn('SaleQty',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Tot_Qty'])).otherwise(SLTXN1['Tot_Qty'] * -1))\
                .withColumn('Gross_Value',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Gross_Amt'])).otherwise(SLTXN1['Calc_Gross_Amt']* -1))\
                .withColumn('Net_Sale_Value',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Net_Amt'])).otherwise(SLTXN1['Calc_Net_Amt'] * -1))\
                .withColumn('Gross_Value',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Commission'])).otherwise(SLTXN1['Calc_Commission'] * -1))\
                .withColumn('CD',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Gross_Amt'])).otherwise(SLTXN1['Calc_Gross_Amt'] * -1))\
                .withColumn('TD',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Sp_Commission'])).otherwise(SLTXN1['Calc_Sp_Commission'] * -1))\
                .withColumn('SpCD',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Rdf'])).otherwise(SLTXN1['Calc_Rdf'] * -1))\
                .withColumn('Scheme_Unit',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Scheme_U'])).otherwise(SLTXN1['Calc_Scheme_U'] * -1))\
                .withColumn('Scheme_Rs',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Scheme_Rs'])).otherwise(SLTXN1['Calc_Scheme_Rs'] * -1))\
                .withColumn('Tax1',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_tax_1'])).otherwise(SLTXN1['Calc_tax_1'] * -1))\
                .withColumn('Tax2',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_tax_2'])).otherwise(SLTXN1['Calc_tax_2']* -1))\
                .withColumn('Tax3',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Tax_3'] + SLTXN1['Calc_Sur_On_Tax3'])).otherwise(SLTXN1['Calc_Tax_3'] + SLTXN1['Calc_Sur_On_Tax3']* -1))\
                .withColumn('Tax3WithoutSur',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Tax_3'])).otherwise(SLTXN1['Calc_Tax_3']* -1))\
                .withColumn('Surcharge',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Sur_On_Tax3'])).otherwise(SLTXN1['Calc_Sur_On_Tax3']* -1))\
                .withColumn('ExciseUnit',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Excise_U'])).otherwise(SLTXN1['Calc_Excise_U']) * -1)\
                .withColumn('AdjustPerUnit',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_adjustment_u'])).otherwise(SLTXN1['Calc_adjustment_u']* -1))\
                .withColumn('AdjustItem',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Adjust_RS'])).otherwise(SLTXN1['Calc_Adjust_RS']* -1))\
                .withColumn('Freight',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Freight'])).otherwise(SLTXN1['Calc_Freight']* -1))\
                .withColumn('AdjustmentRupees',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Adjust'])).otherwise(SLTXN1['Calc_Adjust']* -1))\
                .withColumn('SPDiscount',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Spdisc'])).otherwise(SLTXN1['Calc_Spdisc']* -1))\
                .withColumn('DebitNote',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_DN'])).otherwise(SLTXN1['Calc_DN']* -1))\
                .withColumn('CreditNote',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Cn'])).otherwise(SLTXN1['Calc_Cn']* -1))\
                .withColumn('Display',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Display'])).otherwise(SLTXN1['Calc_Display']* -1))\
                .withColumn('Handling',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Handling'])).otherwise(SLTXN1['Calc_Handling']* -1))\
                .withColumn('Postage',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Postage'])).otherwise(SLTXN1['Calc_Postage']* -1))\
                .withColumn('ExcisePercentage',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_MFees'])).otherwise(SLTXN1['Calc_MFees']* -1))\
                .withColumn('Labour',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Labour'])).otherwise(SLTXN1['Calc_Labour']* -1))\
                .withColumn('UserDefNetAmt1',when((SLTXN1['Sale_Or_Sr'] == 'SL'),('0' + SLTXN1['Calc_Gross_Amt'])).otherwise('0' + SLTXN1['Calc_Gross_Amt']* -1))\
                .withColumn('UserDefNetAmt2',when((SLTXN1['Sale_Or_Sr'] == 'SL'),('0' + SLTXN1['Calc_Gross_Amt'] + SLTXN1['Calc_commission'] + SLTXN1['calc_sp_commission'] + SLTXN1['calc_rdf'] + SLTXN1['calc_scheme_u'] + SLTXN1['calc_scheme_rs'] + SLTXN1['Calc_Tax_1'] + SLTXN1['Calc_Tax_2'] + SLTXN1['Calc_Tax_3'] + SLTXN1['calc_sur_on_tax3'] + SLTXN1['calc_mfees'] + SLTXN1['calc_excise_u'] + SLTXN1['Calc_adjustment_u'] + SLTXN1['Calc_adjust_rs'] + SLTXN1['Calc_freight'] + SLTXN1['calc_adjust'] + SLTXN1['Calc_Spdisc'] + SLTXN1['Calc_DN'] + SLTXN1['Calc_CN'] + SLTXN1['Calc_Display'] + SLTXN1['Calc_Handling'] + SLTXN1['calc_Postage'] + SLTXN1['calc_round'])).otherwise('0' + SLTXN1['Calc_Gross_Amt'] + SLTXN1['Calc_commission'] + SLTXN1['calc_sp_commission'] + SLTXN1['calc_rdf'] + SLTXN1['calc_scheme_u'] + SLTXN1['calc_scheme_rs'] + SLTXN1['Calc_Tax_1'] + SLTXN1['Calc_Tax_2'] + SLTXN1['Calc_Tax_3'] + SLTXN1['calc_sur_on_tax3'] + SLTXN1['calc_mfees'] + SLTXN1['calc_excise_u'] + SLTXN1['Calc_adjustment_u'] + SLTXN1['Calc_adjust_rs'] + SLTXN1['Calc_freight'] + SLTXN1['calc_adjust'] + SLTXN1['Calc_Spdisc'] + SLTXN1['Calc_DN'] + SLTXN1['Calc_CN'] + SLTXN1['Calc_Display'] + SLTXN1['Calc_Handling'] + SLTXN1['calc_Postage'] + SLTXN1['calc_round']* -1))
        
                
    SLTXN1=SLTXN1.groupby('Pack_Code' , 'Pack_Name' ,'Pack_Short_Name','Order_','Item_Hd_Code' , 'Item_Hd_Name', 'Comp_Code','Comp_Name' ,'Group_Code', GM['Group_Name'] ,\
                        'User_Code' , 'Item_Det_Code', 'Item_desc' ,BG['GroupName'], 'Group_Code1' , 'Branch_Name','Branch_Code', 'Scheme_Campaign_Name', SCH['Code'] , 'Scheme_Group_Name', SCGM['SCGM_Code'])\
               .agg(avg('Net_Amt').alias('Avg_BillAmt'),sum('SaleQty').alias('SaleQty'),sum('Gross_Value').alias('Gross_Value'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                    sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),\
                    sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                    sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SPDiscount').alias('SPDiscount'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                    sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                    sum('UserDefNetAmt1').alias('UserDefNetAmt1'),sum('UserDefNetAmt2').alias('UserDefNetAmt2'))

    SLTXN1=SLTXN1.orderBy('Scheme_Campaign_Name',  'Scheme_Group_Name', 'Branch_Name' ,  BG['GroupName'] ,  'User_Code' , 'Order_', 'Pack_Name' , \
                            'Item_Hd_Name' , 'Comp_Name' , GM['Group_Name'])
    #SLTXN1.show(1)
    
    SLTXN1.write.jdbc(url=Sqlurlwrite, table="SchemeReport", mode="overwrite")
    
    print('SchemeReport end: ', datetime.datetime.now())
    
    
    print('PriceDropSaleReport start: ', datetime.datetime.now())
    
    IMD1=IMD1.select('User_Code','Pack_Code','Item_Hd_Code','CF_1','CF_2', 'CF_3','Item_Det_Code','ud_order')
    CCI= CCI.select('Code','Commission_P','Sp_Commission_P' ,'Rdf_P','Excise_U','Tax_1','Tax_2','Tax_3','Scheme_Rs','Scheme_U',\
                      'Adjust_Rs')
    CCI=RENAME(CCI,{"Code":"Comm_Calc_Code","Scheme_Rs":"SchemeRs","Scheme_U":"SchemeU","Adjust_Rs":"AdjustRs"})                
    
    ACT=ACT.select('Act_Code','Grp_Code')
    IMH1=IMH1.select('Item_Name_Code','Item_Hd_Code','Item_Hd_Name','comp_code','Color_Code',\
                     'Group_Code','Comm_It_Desc_Code','Group_Code_12','Group_Code2','Group_Code3','Group_Code11')                    
    ICM1=ICM1.select('Color_Code','Color_Name')
    PM1=PM1.select('Pack_Code','Pack_Name')
    IMN1=IMN1.select('Item_Name_Code','Item_Name')
    CID1=CID1.select('Addl_Item_Name','Code')
    AG = AG.select('Grp_Code')
    GM11 = GM11.select('Group_Code','Group_Name').withColumnRenamed("Group_Name","Group_Name11")\
                .withColumn("Group_Code01", GM11.Group_Code.cast(StringType()))
    GM21 = GM21.select('Group_Code','Group_Name').withColumnRenamed("Group_Name","Group_Name21")\
                .withColumn("Group_Code21", GM21.Group_Code.cast(StringType()))
    GM111 = GM111.select('Group_Code','Group_Name').withColumnRenamed("Group_Name","Group_Name111")\
                    .withColumn("Group_Code111", GM111.Group_Code.cast(StringType()))
    GM31 = GM31.select('Group_Code','Group_Name').withColumnRenamed("Group_Name","Group_Name31")\
                .withColumn("Group_Code31", GM31.Group_Code.cast(StringType()))
    GM121 = GM121.select('Group_Code','Group_Name').withColumnRenamed("Group_Name","Group_Name121")\
                .withColumn("Group_Code121", GM121.Group_Code.cast(StringType()))
    
    SLHD1=SLHD1.select('Vouch_Code','Stock_Trans','Deleted','Branch_Code','Cust_Code','Vouch_Date')\
               .filter(SLHD1.Deleted== 0).filter(SLHD1.Stock_Trans== 0)
    
    
    SLTXN=SLTXN.select('Tot_Qty','Calc_Net_Amt','Sale_Or_Sr','Calc_adjust_rs','Comm_Calc_Code','Vouch_Code','Repl_Qty','Free_Qty'\
                       ,'Sa_Subs_Lot','Item_Det_Code','Lot_Code','Calc_Gross_Amt', 'Calc_commission', 'calc_sp_commission', 'calc_rdf', 'calc_scheme_u', 'calc_scheme_rs', 'Calc_Tax_1', 'Calc_Tax_2', 'Calc_Tax_3', 'calc_sur_on_tax3', \
                        'calc_mfees', 'calc_excise_u', 'Calc_adjustment_u',  'Calc_freight', 'calc_adjust', 'Calc_Spdisc', 'Calc_DN', 'Calc_CN', 'Calc_Display', 'Calc_Handling',\
                        'calc_Postage', 'calc_Round', 'calc_Labour','Sample_Qty')
    
    
    #SLTXN = SLTXN.join(CCI,SLTXN.Comm_Calc_Code==CCI.Code,how='left' )
    
    SLTXN = SLTXN.join(SLHD1,on=['Vouch_Code'],how='left' ) 
    SLTXN = SLTXN.join(IMD1,on = ['Item_Det_Code'],how='left' )
    SLTXN = SLTXN.join(CCI,on = ['Comm_Calc_Code'], how = 'left')
    SLTXN = SLTXN.join(ACT,SLTXN.Cust_Code==ACT.Act_Code,how='left' )
    SLTXN = SLTXN.join(IMH1,on = ['Item_Hd_Code'], how = 'left')
    SLTXN = SLTXN.join(CM,on = ['comp_code'], how = 'left')
    SLTXN = SLTXN.join(BM,on = ['Branch_Code'], how = 'left')
    SLTXN = SLTXN.join(AG,on = ['Grp_Code'], how = 'left')
    SLTXN = SLTXN.join(GM,on = ['Group_Code'], how = 'left')
    SLTXN = SLTXN.join(IMN1,on = ['Item_Name_Code'], how = 'left')
    SLTXN = SLTXN.join(CID1,SLTXN.Comm_It_Desc_Code==CID1.Code,how='left')
    SLTXN = SLTXN.join(GM11,SLTXN.Group_Code==GM11.Group_Code01,how='left')
    SLTXN = SLTXN.join(ICM1,on = ['Color_Code'], how = 'left')
    SLTXN = SLTXN.join(PM1,on = ['Pack_Code'],how='left' )
    SLTXN = SLTXN.join(GM21,SLTXN.Group_Code2==GM21.Group_Code21,how='left')
    SLTXN = SLTXN.join(GM31,SLTXN.Group_Code3==GM31.Group_Code31,how='left')
    SLTXN = SLTXN.join(GM111,SLTXN.Group_Code11==GM111.Group_Code111,how='left')
    SLTXN = SLTXN.join(GM121,SLTXN.Group_Code_12==GM121.Group_Code121,how='left')
    
    
    #SLTXN = SLTXN.filter(SLTXN.Vouch_Date.between(pd.to_datetime('2019-09-18'),pd.to_datetime('2019-09-18')))
    
    SLTXN=SLTXN.withColumn('SaleQty',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Tot_Qty'])).otherwise(SLTXN['Tot_Qty']* -1)/1 )\
                .withColumn('Free_Qty',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Free_Qty'])).otherwise(SLTXN['Free_Qty']* -1)/1 )\
                .withColumn('Repl_Qty',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Repl_Qty'])).otherwise(SLTXN['Repl_Qty']* -1)/1 )\
                .withColumn('Sample_Qty',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Sample_Qty'])).otherwise(SLTXN['Sample_Qty'] * -1)/1)
    SLTXN=SLTXN.withColumn('TotQty',when((SLTXN['Sale_Or_Sr'] == 'SL'),((SLTXN['Tot_Qty'] + SLTXN['Free_Qty'] + SLTXN['Repl_Qty'] + SLTXN['Sample_Qty']))).otherwise(((SLTXN['Tot_Qty'] + SLTXN['Free_Qty'] + SLTXN['Repl_Qty'] + SLTXN['Sample_Qty'])* -1)/1 ))\
                .withColumn('CD',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Commission'])).otherwise(SLTXN['Calc_Commission']* -1))\
                .withColumn('TD',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Sp_Commission'])).otherwise(SLTXN['Calc_Sp_Commission']* -1))\
                .withColumn('SPCD',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Rdf'])).otherwise(SLTXN['Calc_Rdf']* -1))\
                .withColumn('Calc_Scheme_U',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Scheme_U'])).otherwise(SLTXN['Calc_Scheme_U']* -1))\
                .withColumn('Scheme_Rs',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Scheme_Rs'])).otherwise(SLTXN['Calc_Scheme_Rs']* -1))\
                .withColumn('TAX1',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_tax_1'])).otherwise(SLTXN['Calc_tax_1']* -1))\
                .withColumn('TAX2',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_tax_2'])).otherwise(SLTXN['Calc_tax_2']* -1))\
                .withColumn('TAX3',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Tax_3'] + SLTXN['Calc_Sur_On_Tax3'])).otherwise(SLTXN['Calc_Tax_3'] + SLTXN['Calc_Sur_On_Tax3']* -1))\
                .withColumn('Bill_Amt',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Net_Amt'])).otherwise(SLTXN['Calc_Net_Amt']* -1))\
                .withColumn('Adjust_Rs',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_Adjust_RS'])).otherwise(SLTXN['Calc_Adjust_RS']* -1))\
                .withColumn('Excise',when((SLTXN['Sale_Or_Sr'] == 'SL'),(SLTXN['Calc_MFees'])).otherwise(SLTXN['Calc_MFees']* -1))\
                .withColumn('UDNetAmt1',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0')).otherwise('0') * -1).withColumn('Month',month(SLHD1.Vouch_Date))
    SLTXN=SLTXN.withColumn('UDNetAmt2',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour']* -1))\
                .withColumn('UDNetAmt3',when((SLTXN['Sale_Or_Sr'] == 'SL'),('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour'])).otherwise('0' + SLTXN['Calc_Gross_Amt'] + SLTXN['Calc_commission'] + SLTXN['calc_sp_commission'] + SLTXN['calc_rdf'] + SLTXN['calc_scheme_u'] + SLTXN['calc_scheme_rs'] + SLTXN['Calc_Tax_1'] + SLTXN['Calc_Tax_2'] + SLTXN['Calc_Tax_3'] + SLTXN['calc_sur_on_tax3'] + SLTXN['calc_mfees'] + SLTXN['calc_excise_u'] + SLTXN['Calc_adjustment_u'] + SLTXN['Calc_adjust_rs'] + SLTXN['Calc_freight'] + SLTXN['calc_adjust'] + SLTXN['Calc_Spdisc'] + SLTXN['Calc_DN'] + SLTXN['Calc_CN'] + SLTXN['Calc_Display'] + SLTXN['Calc_Handling'] + SLTXN['calc_Postage'] + SLTXN['calc_Round'] + SLTXN['calc_Labour']* -1))
    SLTXN=SLTXN.withColumn('GroupChangeCode',(SLTXN['Group_Code01']+lit('|')+SLTXN['Group_Code21']+lit('|')+SLTXN['Group_Code31']+lit('|')+SLTXN['Group_Code111']+lit('|')+SLTXN['Group_Code121']))\
                
    
    SLTXN=SLTXN.groupby('Item_Det_Code',  IMH1['Item_Hd_Name'], PM1['Pack_Name'], ICM1['Color_Name'], CM['Comp_Name'],  IMD1['User_code'], IMD1['ud_order'], 'Item_Hd_Code',\
                          IMH1['Color_code'], IMH1['comp_code'], 'Item_Name_Code',IMN1['Item_Name'],'Pack_Code', IMD1['CF_1'], IMD1['CF_2'], IMD1['CF_3'],CID1['Addl_Item_Name'],\
                          CCI['Commission_P'],  CCI['SP_Commission_P'],  CCI['Rdf_P'],  CCI['Excise_U'],  CCI['Tax_1'],  CCI['Tax_2'],  CCI['Tax_3'], CCI['SchemeRs'], CCI['SchemeU'], CCI['AdjustRs'],\
                          'Month', BM['Branch_Name'], 'Branch_Code', GM11['Group_Name11'],  GM21['Group_Name21'],  GM31['Group_Name31'] ,  GM111['Group_Name111'],  GM121['Group_Name121'],\
                          GM11['Group_Code01'], GM21['Group_Code21'], GM31['Group_Code31'], GM111['Group_Code111'], GM121['Group_Code121'],'GroupChangeCode')\
               .agg(sum('SaleQty').alias('SaleQty'),sum('Free_Qty').alias('Free_Qty'),sum('Repl_Qty').alias('Repl_Qty'),sum('Sample_Qty').alias('Sample_Qty'),sum('TotQty').alias('TotQty'),\
                    sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SPCD').alias('SPCD'),sum('Calc_Scheme_U').alias('Calc_Scheme_U'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('TAX1').alias('TAX1'),\
                    sum('TAX2').alias('TAX2'),sum('TAX3').alias('TAX3'),sum('Bill_Amt').alias('Bill_Amt'),sum('Adjust_Rs').alias('Adjust_Rs'),sum('Excise').alias('Excise'),\
                    sum('UDNetAmt1').alias('UDNetAmt1'),sum('UDNetAmt2').alias('UDNetAmt2'),sum('UDNetAmt3').alias('UDNetAmt3'))
    #SLTXN.show(1)
    #print(SLTXN.columns)
    #print(SLTXN.count())
    SLTXN.write.jdbc(url=Sqlurlwrite, table="PriceDropSaleReport", mode="overwrite")
    
    print('PriceDropSaleReport end: ', datetime.datetime.now())
    exit()
    
    print('SchemeDateWiseSaleReport start: ', datetime.datetime.now())
    
    #SS1 =SS1.select('Code_Type','Sch_Det_Code','SL_Txn_Code')
    ICM2=ICM2.select('Color_Code','Color_Name','Color_Short_Name')
    
    SLTXN1 = SLTXN1.join(LM,on =['Lot_Code'],how='left' )
    SLTXN1 = SLTXN1.join(IMD,on = ['Item_Det_Code'],how='left' )
    SLTXN1 = SLTXN1.join(SLHD,on=['Vouch_Code'],how='left' ) 
    SLTXN1 = SLTXN1.join(BM,on = ['Branch_Code'], how = 'left')
    
    SLTXN1 = SLTXN1.join(IMH,on = ['Item_Hd_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(ICM2,on = ['Color_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(CM,on = ['comp_code'], how = 'left')
    SLTXN1 = SLTXN1.join(PM,on = ['Pack_Code'],how='left' )
    SLTXN1 = SLTXN1.join(GM,on = ['Group_Code'], how = 'left')#.drop('Group_Code')
    SLTXN1 =  SLTXN1.join(BG,on =['Group_Code1'],how='left' )
    SLTXN1 = SLTXN1.join(IMN,on = ['Item_Name_Code'], how = 'left')
    SLTXN1 = SLTXN1.join(CID,SLTXN1.Comm_It_Desc_Code==CID.Code,how='left' )
    SLTXN1 = SLTXN1.join(SS,on = ['Code'],how='left' )
    #SLTXN1.show(1)
    #exit()
    SLTXN1 = SLTXN1.join(SCGM,SLTXN1.Sch_Det_Code==SCGM.SCGM_Code,how='left' )
    SLTXN1 = SLTXN1.join(SCH,SLTXN1.Scheme_Campaign_Code==SCH.Code,how='left' )
    
       
    
    SLTXN1=SLTXN1.withColumn('SaleQty',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Tot_Qty'])).otherwise(SLTXN1['Tot_Qty']* -1))\
                .withColumn('Gross_Value',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Gross_Amt'])).otherwise(SLTXN1['Calc_Gross_Amt']* -1))\
                .withColumn('Net_Sale_Value',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Net_Amt'])).otherwise(SLTXN1['Calc_Net_Amt']* -1))\
                .withColumn('Gross_Value',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Commission'])).otherwise(SLTXN1['Calc_Commission']* -1))\
                .withColumn('CD',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Gross_Amt'])).otherwise(SLTXN1['Calc_Gross_Amt']* -1))\
                .withColumn('TD',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Sp_Commission'])).otherwise(SLTXN1['Calc_Sp_Commission']* -1))\
                .withColumn('SpCD',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Rdf'])).otherwise(SLTXN1['Calc_Rdf']* -1))\
                .withColumn('Scheme_Unit',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Scheme_U'])).otherwise(SLTXN1['Calc_Scheme_U']* -1))\
                .withColumn('Scheme_Rs',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Scheme_Rs'])).otherwise(SLTXN1['Calc_Scheme_Rs']* -1))\
                .withColumn('Tax1',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_tax_1'])).otherwise(SLTXN1['Calc_tax_1']* -1))\
                .withColumn('Tax2',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_tax_2'])).otherwise(SLTXN1['Calc_tax_2']* -1))\
                .withColumn('Tax3',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Tax_3'] + SLTXN1['Calc_Sur_On_Tax3'])).otherwise(SLTXN1['Calc_Tax_3'] + SLTXN1['Calc_Sur_On_Tax3']* -1))\
                .withColumn('Tax3WithoutSur',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Tax_3'])).otherwise(SLTXN1['Calc_Tax_3']* -1))\
                .withColumn('Surcharge',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Sur_On_Tax3'])).otherwise(SLTXN1['Calc_Sur_On_Tax3']* -1))\
                .withColumn('ExciseUnit',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Excise_U'])).otherwise(SLTXN1['Calc_Excise_U']* -1))\
                .withColumn('AdjustPerUnit',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_adjustment_u'])).otherwise(SLTXN1['Calc_adjustment_u']* -1))\
                .withColumn('AdjustItem',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Adjust_RS'])).otherwise(SLTXN1['Calc_Adjust_RS']* -1))\
                .withColumn('Freight',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Freight'])).otherwise(SLTXN1['Calc_Freight']* -1))\
                .withColumn('AdjustmentRupees',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Adjust'])).otherwise(SLTXN1['Calc_Adjust']* -1))\
                .withColumn('SPDiscount',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Spdisc'])).otherwise(SLTXN1['Calc_Spdisc']* -1))\
                .withColumn('DebitNote',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_DN'])).otherwise(SLTXN1['Calc_DN']* -1))\
                .withColumn('CreditNote',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Cn'])).otherwise(SLTXN1['Calc_Cn']* -1))\
                .withColumn('Display',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Display'])).otherwise(SLTXN1['Calc_Display']* -1))\
                .withColumn('Handling',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Handling'])).otherwise(SLTXN1['Calc_Handling']* -1))\
                .withColumn('Postage',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Postage'])).otherwise(SLTXN1['Calc_Postage']* -1))\
                .withColumn('ExcisePercentage',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_MFees'])).otherwise(SLTXN1['Calc_MFees']* -1))\
                .withColumn('Labour',when((SLTXN1['Sale_Or_Sr'] == 'SL'),(SLTXN1['Calc_Labour'])).otherwise(SLTXN1['Calc_Labour']* -1))\
                .withColumn('UserDefNetAmt1',when((SLTXN1['Sale_Or_Sr'] == 'SL'),('0' + SLTXN1['Calc_Gross_Amt'])).otherwise('0' + SLTXN1['Calc_Gross_Amt']* -1))\
                .withColumn('UserDefNetAmt2',when((SLTXN1['Sale_Or_Sr'] == 'SL'),('0' + SLTXN1['Calc_Gross_Amt'] + SLTXN1['Calc_commission'] + SLTXN1['calc_sp_commission'] + SLTXN1['calc_rdf'] + SLTXN1['calc_scheme_u'] + SLTXN1['calc_scheme_rs'] + SLTXN1['Calc_Tax_1'] + SLTXN1['Calc_Tax_2'] + SLTXN1['Calc_Tax_3'] + SLTXN1['calc_sur_on_tax3'] + SLTXN1['calc_mfees'] + SLTXN1['calc_excise_u'] + SLTXN1['Calc_adjustment_u'] + SLTXN1['Calc_adjust_rs'] + SLTXN1['Calc_freight'] + SLTXN1['calc_adjust'] + SLTXN1['Calc_Spdisc'] + SLTXN1['Calc_DN'] + SLTXN1['Calc_CN'] + SLTXN1['Calc_Display'] + SLTXN1['Calc_Handling'] + SLTXN1['calc_Postage'] + SLTXN1['calc_round'])).otherwise('0' + SLTXN1['Calc_Gross_Amt'] + SLTXN1['Calc_commission'] + SLTXN1['calc_sp_commission'] + SLTXN1['calc_rdf'] + SLTXN1['calc_scheme_u'] + SLTXN1['calc_scheme_rs'] + SLTXN1['Calc_Tax_1'] + SLTXN1['Calc_Tax_2'] + SLTXN1['Calc_Tax_3'] + SLTXN1['calc_sur_on_tax3'] + SLTXN1['calc_mfees'] + SLTXN1['calc_excise_u'] + SLTXN1['Calc_adjustment_u'] + SLTXN1['Calc_adjust_rs'] + SLTXN1['Calc_freight'] + SLTXN1['calc_adjust'] + SLTXN1['Calc_Spdisc'] + SLTXN1['Calc_DN'] + SLTXN1['Calc_CN'] + SLTXN1['Calc_Display'] + SLTXN1['Calc_Handling'] + SLTXN1['calc_Postage'] + SLTXN1['calc_round']* -1))
                
    
    SLTXN1 =SLTXN1.withColumn('MonthOrder', when(month(SLHD.Vouch_Date) < 4, month(SLHD.Vouch_Date) + 12).otherwise(month(SLHD.Vouch_Date)))\
               .withColumn('DayOrder', dayofmonth(SLHD['Vouch_Date']))
    
    
    
    
    SLTXN1=SLTXN1.groupby('Pack_Code' , 'Pack_Name' ,'Pack_Short_Name','Order_','Item_Hd_Code' , 'Item_Hd_Name', 'Comp_Code','Comp_Name' , 'Group_Code', GM['Group_Name'] ,\
                        'User_Code' , 'MonthOrder','DayOrder','Item_Det_Code', 'Item_desc','Color_Code' , ICM2['Color_Name'] , ICM2['Color_Short_Name'] ,BG['GroupName'],\
                        'Group_Code1' , 'Branch_Name','Branch_Code', 'Scheme_Campaign_Name', SCH['Code'] , 'Scheme_Group_Name', SCGM['SCGM_Code'])\
               .agg(avg('Net_Amt').alias('Avg_BillAmt'),sum('SaleQty').alias('SaleQty'),sum('Gross_Value').alias('Gross_Value'),sum('Net_Sale_Value').alias('Net_Sale_Value'),\
                    sum('CD').alias('CD'),sum('TD').alias('TD'),sum('SpCD').alias('SpCD'),sum('Scheme_Unit').alias('Scheme_Unit'),sum('Scheme_Rs').alias('Scheme_Rs'),sum('Tax1').alias('Tax1'),\
                    sum('Tax2').alias('Tax2'),sum('Tax3').alias('Tax3'),sum('Tax3WithoutSur').alias('Tax3WithoutSur'),sum('Surcharge').alias('Surcharge'),sum('ExciseUnit').alias('ExciseUnit'),sum('AdjustPerUnit').alias('AdjustPerUnit'),\
                    sum('AdjustItem').alias('AdjustItem'),sum('Freight').alias('Freight'),sum('AdjustmentRupees').alias('AdjustmentRupees'),sum('SPDiscount').alias('SPDiscount'),sum('DebitNote').alias('DebitNote'),sum('CreditNote').alias('CreditNote'),\
                    sum('Display').alias('Display'),sum('Handling').alias('Handling'),sum('Postage').alias('Postage'),sum('ExcisePercentage').alias('ExcisePercentage'),sum('Labour').alias('Labour'),\
                    sum('UserDefNetAmt1').alias('UserDefNetAmt1'),sum('UserDefNetAmt2').alias('UserDefNetAmt2'))
    
    SLTXN1.show(1)
    SLTXN1.write.jdbc(url=Sqlurlwrite, table="SchemeDateWiseSaleReport", mode="overwrite")

    print('SchemeDateWiseSaleReport end: ', datetime.datetime.now())
    
    exit()
    
    '''
    print('SchemeCampaignWiseItemDifinedReport start: ', datetime.datetime.now())
    
    
    CID2=CID2.select( 'Item_Desc','Code').withColumnRenamed("Code","Comm_It_Desc_Code")
    SCM1=SCM1.select('Code','Scheme_Campaign_Name','Date_From','Date_To')
    SCGM1=SCGM1.select('Code','Scheme_Group_Name','Scheme_Campaign_Code').withColumnRenamed("Code","Scheme_Campaign_Group_code")
    
                
    SCDET1=SCDET1.select('Scheme_Campaign_Group_code','Code_Type','Item_Code')\
                .filter(SCDET1.Code_Type== 'IMH')
    
    IMD2=IMD2.select('User_Code','Item_Det_Code','Mrp','Item_Hd_Code','Pack_Code' )
  
    
    SCM1=SCM1.join(SCGM1,SCM1.Code==SCGM1.Scheme_Campaign_Code,how='left' )
    SCM1=SCM1.join(SCDET1,on=['Scheme_Campaign_Group_code'],how='left')
    SCM1=SCM1.join(IMH,SCM1.Item_Code==IMH.Item_Hd_Code,how='left' )
    SCM1=SCM1.join(CID2,on=['Comm_It_Desc_Code'],how='left' )
    SCM1=SCM1.join(IMD2,on = ['Item_Hd_Code'], how = 'left')
    SCM1 = SCM1.join(CM,on = ['comp_code'], how = 'left')
    SCM1 = SCM1.join(ICM2,on = ['Color_Code'], how = 'left')
    SCM1 = SCM1.join(PM,on = ['Pack_Code'],how='left' )
    SCM1 = SCM1.join(GM,on = ['Group_Code'], how = 'left')
    SCM1 = SCM1.join(IMN,on = ['Item_Name_Code'], how = 'left')
    
    
    #SCM1.show(1)
    #exit()
    
    SCM2=SCM2.select('Code','Scheme_Campaign_Name','Date_From','Date_To')
    SCDET2=SCDET2.select('Scheme_Campaign_Group_code','Code_Type','Item_Code')\
                .filter(SCDET2.Code_Type== 'ITGRP')
    
    
    SCM2=SCM2.join(SCGM1,SCM2.Code==SCGM1.Scheme_Campaign_Code,how='left' )
    SCM2=SCM2.join(SCDET2,on=['Scheme_Campaign_Group_code'],how='left')
    SCM2=SCM2.join(IMH,SCM2.Item_Code==IMH.Item_Hd_Code,how='left' )
    SCM2=SCM2.join(CID2,on=['Comm_It_Desc_Code'],how='left' )
    SCM2=SCM2.join(IMD2,on = ['Item_Hd_Code'], how = 'left')
    SCM2 = SCM2.join(CM,on = ['comp_code'], how = 'left')
    SCM2 = SCM2.join(ICM2,on = ['Color_Code'], how = 'left')
    SCM2 = SCM2.join(PM,on = ['Pack_Code'],how='left' )
    SCM2 = SCM2.join(GM,on = ['Group_Code'], how = 'left')
    SCM2 = SCM2.join(IMN,on = ['Item_Name_Code'], how = 'left')  
    
    
    SCM2 = SCM1.union(SCM2)
    #SCM2.show(1)
    
    SCM3=SCM3.select('Code','Scheme_Campaign_Name','Date_From','Date_To')
    
    SCM3=SCM3.join(SCGM1,SCM3.Code==SCGM1.Scheme_Campaign_Code,how='left' )
    SCM3=SCM3.join(SCDET2,on=['Scheme_Campaign_Group_code'],how='left')
    SCM3=SCM3.join(IMH,SCM3.Item_Code==IMH.Item_Hd_Code,how='left' )
    SCM3=SCM3.join(CID2,on=['Comm_It_Desc_Code'],how='left' )
    SCM3=SCM3.join(IMD2,on = ['Item_Hd_Code'], how = 'left')
    SCM3 = SCM3.join(CM,on = ['comp_code'], how = 'left')
    SCM3 = SCM3.join(ICM2,on = ['Color_Code'], how = 'left')
    SCM3 = SCM3.join(PM,on = ['Pack_Code'],how='left' )
    SCM3 = SCM3.join(GM,on = ['Group_Code'], how = 'left')
    SCM3 = SCM3.join(IMN,on = ['Item_Name_Code'], how = 'left') 

    

    SCM3= SCM2.union(SCM3)
    #SCM3.show(1)
    
    
    SCM4=SCM4.select('Code','Scheme_Campaign_Name','Date_From','Date_To')
    SCDET4=SCDET4.select('Scheme_Campaign_Group_code','Code_Type','Item_Code')\
                .filter(SCDET4.Code_Type== 'ITCOMP')
    
    
    SCM4=SCM4.join(SCGM1,SCM4.Code==SCGM1.Scheme_Campaign_Code,how='left' )
    SCM4=SCM4.join(SCDET4,on=['Scheme_Campaign_Group_code'],how='left')
    SCM4=SCM4.join(IMH,SCM4.Item_Code==IMH.Item_Hd_Code,how='left' )
    SCM4=SCM4.join(CID2,on=['Comm_It_Desc_Code'],how='left' )
    SCM4=SCM4.join(IMD2,on = ['Item_Hd_Code'], how = 'left')
    SCM4 = SCM4.join(CM,on = ['comp_code'], how = 'left')
    SCM4 = SCM4.join(ICM2,on = ['Color_Code'], how = 'left')
    SCM4 = SCM4.join(PM,on = ['Pack_Code'],how='left' )
    SCM4 = SCM4.join(GM,on = ['Group_Code'], how = 'left')
    SCM4 = SCM4.join(IMN,on = ['Item_Name_Code'], how = 'left')

    


    SCM4= SCM3.union(SCM4)
    #SCM4.show(1)
    
    SCM5=SCM5.select('Code','Scheme_Campaign_Name','Date_From','Date_To')
    SCDET5=SCDET5.select('Scheme_Campaign_Group_code','Code_Type','Item_Code')\
                .filter(SCDET5.Code_Type== 'IMD')
    
    
    SCM5=SCM5.join(SCGM1,SCM5.Code==SCGM1.Scheme_Campaign_Code,how='left' )
    SCM5=SCM5.join(SCDET5,on=['Scheme_Campaign_Group_code'],how='left')
    SCM5=SCM5.join(IMH,SCM5.Item_Code==IMH.Item_Hd_Code,how='left' )
    SCM5=SCM5.join(CID2,on=['Comm_It_Desc_Code'],how='left' )
    SCM5=SCM5.join(IMD2,on = ['Item_Hd_Code'], how = 'left')
    SCM5 = SCM5.join(CM,on = ['comp_code'], how = 'left')
    SCM5 = SCM5.join(ICM2,on = ['Color_Code'], how = 'left')
    SCM5 = SCM5.join(PM,on = ['Pack_Code'],how='left' )
    SCM5 = SCM5.join(GM,on = ['Group_Code'], how = 'left')
    SCM5 = SCM5.join(IMN,on = ['Item_Name_Code'], how = 'left')  

    
    SCM5= SCM4.union(SCM5)
    #SCM5.show(1)
    SCM5.write.jdbc(url=Sqlurlwrite, table="SchemeCampaignWiseItemDifinedReport", mode="overwrite")
    
    
    print('SchemeCampaignWiseItemDifinedReport end: ', datetime.datetime.now())
    
    '''
    

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