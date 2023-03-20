from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import concat_ws, when, substring,sum,month,year
from pyspark.sql.functions import last
#from fiscalyear import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udl#,fiscalyear
import pandas as pd
from pyspark.sql.types import IntegerType
#from calendar import month
import pandas as pd
from _datetime import date
import dateutil.relativedelta

print('MasterDataIngestion start: ', datetime.datetime.now())

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

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()#.strftime('%H:%M:%S')
stime = start_time.strftime('%H:%M:%S')



try:
    config = os.path.dirname(os.path.realpath(__file__))
    Market = config[config.rfind("DB"):config.rfind("/")]
    Etn = Market[Market.rfind("E"):]
    DB = Market[:Market.rfind("E")]
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
    
    conf = SparkConf().setMaster("local[*]").setAppName("StockDataIngestion").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:StockDataIngestion").getOrCreate()
   
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    
    
    
    
    Lot_Mst= "(SELECT * From Lot_Mst) as LM"
    #Lot_Mst="(SELECT sp_rate2,Puv_Code,CF_Lot ,Expiry ,Basic_rate, Mfg_Date, Map_Code ,Lot_Code,exp_dmg, Org_Lot_Code,Bill_No ,Pur_Date , Lot_Number,sale_rate , pur_rate, Mrp, Item_Det_Code from Lot_Mst) as LM"
    Lot_Mst= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Lot_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    Lot_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Lot_Mst")
    
    
    
    
    
    
    
    RetailFootfall ="(SELECT * from Retail_FootFalls) as RF"
    RetailFootfall = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=RetailFootfall,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    RetailFootfall.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/RetailFootfall")
    
    
    
    Discount_Coupon_Mst = "(SELECT Sale_Fin_Year,Sale_vouch_code,CD_Rs,Discount_Coupon_Prefix,Code,Discount_Coupon_Number,Expiry_Date,CD_P,Discount_Coupon_Barcode,Discount_Coupon_Group_Code,Discount_Coupon_Num from Discount_Coupon_Mst) as CDM"
    Discount_Coupon_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Discount_Coupon_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Discount_Coupon_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/Discount_Coupon_Mst")
   
    Store_Mst ="(SELECT Code from Store_Mst) as SM"
    Store_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Store_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Store_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/Store_Mst")
    

    sup_det="(SELECT * FROM sup_det ) as SD"
    sup_det= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=sup_det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    sup_det.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/sup_det")
    
    
    
    CCMst="(SELECT CC_No_Code ,CC_Party_Name FROM CC_No_Mst) as Ccms"
    CCMst= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=CCMst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    CCMst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/CCMst")
    
    
    Tax_Regions = "(SELECT Tax_Reg_Name,Tax_Reg_Code From Tax_Regions) as TR"  
    Tax_Regions= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Tax_Regions,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()  
    Tax_Regions.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/Tax_Regions")
    
    
    Setup_Masters = "(SELECT Mst_Name, Code,General_Text_1  from Setup_Masters) as SM"
    Setup_Masters = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Setup_Masters,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Setup_Masters.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/Setup_Masters")
    

    Scheme_Campaign_Group_Mst = "(SELECT Code,Scheme_Group_Name,Scheme_Campaign_Code  from Scheme_Campaign_Group_Mst) as SCGM"
    Scheme_Campaign_Group_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Scheme_Campaign_Group_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Scheme_Campaign_Group_Mst.coalesce(1).write.mode(owmode).save(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Mst")
    
    
    '''
    RetailFootfall ="(SELECT * from Retail_FootFalls) as RF"
    RetailFootfall = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=RetailFootfall,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    RetailFootfall.write.mode(owmode).save(hdfspath+"/Market/Stage1/RetailFootfall")
    
    
    
    Discount_Coupon_Mst = "(SELECT Sale_Fin_Year,Sale_vouch_code,CD_Rs,Discount_Coupon_Prefix,Code,Discount_Coupon_Number,Expiry_Date,CD_P,Discount_Coupon_Barcode,Discount_Coupon_Group_Code,Discount_Coupon_Num from Discount_Coupon_Mst) as CDM"
    Discount_Coupon_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Discount_Coupon_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Discount_Coupon_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Discount_Coupon_Mst")
   
    Store_Mst ="(SELECT Code from Store_Mst) as SM"
    Store_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Store_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Store_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Store_Mst")
    

    sup_det="(SELECT * FROM sup_det ) as SD"
    sup_det= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=sup_det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    sup_det.write.mode(owmode).save(hdfspath+"/Market/Stage1/sup_det")
    
    
    
    CCMst="(SELECT CC_No_Code ,CC_Party_Name FROM CC_No_Mst) as Ccms"
    CCMst= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=CCMst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    CCMst.write.mode(owmode).save(hdfspath+"/Market/Stage1/CCMst")
    
    Accounts = "(SELECT Act_Name, Print_Act_Name,Address_1, Act_Code,City_Code,Grp_Code,City_Start_Pos,User_Code from Accounts) as ACT"
    Accounts = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Accounts,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Accounts.write.mode(owmode).save(hdfspath+"/Market/Stage1/Accounts")
    
    
    
    
    
    Godown_Mst = "(SELECT godown_Name,godown_Code from Godown_Mst) as GDM"
    Godown_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Godown_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Godown_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Godown_Mst")
    
        
    City_ = "(SELECT City_Name,City_Code  from City_) as Ct"
    City_ = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=City_,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    City_.write.mode(owmode).save(hdfspath+"/Market/Stage1/City_")
    
    It_Mst_O_Det ="(SELECT Code from It_Mst_O_Det) as IMOD"
    It_Mst_O_Det = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_O_Det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    It_Mst_O_Det.write.mode(owmode).save(hdfspath+"/Market/Stage1/It_Mst_O_Det")
    
    Branch_Groups ="(SELECT Group_Code, Group_Name from Branch_Groups) as BG"
    Branch_Groups = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Branch_Groups,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Branch_Groups.write.mode(owmode).save(hdfspath+"/Market/Stage1/Branch_Groups")
    
    Pack_Mst ="(SELECT Pack_Code, Pack_Name ,Link_Code, Pack_Short_Name,Order_ from Pack_Mst) as PM"
    Pack_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Pack_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Pack_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Pack_Mst")
    
    Comp_Mst= "(SELECT Comp_Code,Comp_Name from Comp_Mst) as CM"
    Comp_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Comp_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Comp_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Comp_Mst")

    Comm_It_Desc= "(SELECT Item_Desc,Code,Addl_Item_Name from Comm_It_Desc) as CID"
    Comm_It_Desc = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Comm_It_Desc,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Comm_It_Desc.write.mode(owmode).save(hdfspath+"/Market/Stage1/Comm_It_Desc")
    
    Group_Mst = "(SELECT Group_Code,Group_Name from Group_Mst) as GM"
    Group_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Group_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Group_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Group_Mst")
    
    
    AccountGroups = "(SELECT Grp_Code from AccountGroups) as AG"
    AccountGroups = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=AccountGroups,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    AccountGroups.write.mode(owmode).save(hdfspath+"/Market/Stage1/AccountGroups")
 
    
    Tax_Regions = "(SELECT Tax_Reg_Name,Tax_Reg_Code From Tax_Regions) as TR"  
    Tax_Regions= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Tax_Regions,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()  
    Tax_Regions.write.mode(owmode).save(hdfspath+"/Market/Stage1/Tax_Regions")
    
    Agents_Brokers = "(SELECT Agent_Name, Code  from Agents_Brokers) as AB"
    Agents_Brokers = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Agents_Brokers,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Agents_Brokers.write.mode(owmode).save(hdfspath+"/Market/Stage1/Agents_Brokers")
        
    
    
    
    
    
    
    Branch_Mst = "(SELECT Branch_Short_Name,Branch_Name, Branch_Code,Group_Code6,Group_Code1,Rate_Group_Code,Show_ST_As_Sale from Branch_Mst) as BM"
    Branch_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Branch_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Branch_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Branch_Mst")
    
    
    
    
    
    Setup_Masters = "(SELECT Mst_Name, Code,General_Text_1  from Setup_Masters) as SM"
    Setup_Masters = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Setup_Masters,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Setup_Masters.write.mode(owmode).save(hdfspath+"/Market/Stage1/Setup_Masters")
    

    Scheme_Campaign_Group_Mst = "(SELECT Code,Scheme_Group_Name,Scheme_Campaign_Code  from Scheme_Campaign_Group_Mst) as SCGM"
    Scheme_Campaign_Group_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Scheme_Campaign_Group_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Scheme_Campaign_Group_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Mst")
    
    
    Chal_Head = "(SELECT Post_In_Accounts,Vouch_Date,Vouch_Time,Deleted,cust_Code, Store_Code,Branch_Code,Comm_Calc_Code,Stock_Trans,vouch_code,\
                        Pay_Mode,Series_Code,number_,Chq_No ,Chq_Date,Vouch_Num From Chal_Head) as Chal_Head"  
    Chal_Head= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Chal_Head,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()  
    Chal_Head.write.mode(owmode).save(hdfspath+"/Market/Stage1/Chal_Head")
     
 
    It_Mst_Det = "(SELECT Mrp,ud_order,Cf_1_Desc, Cf_2_Desc, Cf_3_Desc, Cf_1_Desc As ItemDescCf1, Packing , Cf_1 , Cf_2, CF_3,User_Code, Item_Det_Code ,Pack_Code,Item_Hd_Code,Item_O_Det_Code from It_Mst_Det) as IMD"
    It_Mst_Det = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_Det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    It_Mst_Det.write.mode(owmode).save(hdfspath+"/Market/Stage1/It_Mst_Det")
    
    Item_Color_Mst = "(SELECT Color_Name,Color_Code,Color_Short_Name from Item_Color_Mst) as ICM"
    Item_Color_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Item_Color_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Item_Color_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Item_Color_Mst")
   
    It_Mst_Names = "(SELECT Item_Name,Item_Name_Code from It_Mst_Names) as IMN"
    It_Mst_Names = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_Names,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    It_Mst_Names.write.mode(owmode).save(hdfspath+"/Market/Stage1/It_Mst_Names")
    
    It_Mst_Hd = "(SELECT Item_Hd_Code,comp_code,Color_Code,Item_Name_Code, Item_Hd_Name, Group_Code,Comm_It_Desc_Code,Group_Code_12,Group_Code2,Group_Code3,Group_Code11 from It_Mst_Hd) as IMH1"
    It_Mst_Hd = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_Hd,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    It_Mst_Hd.write.mode(owmode).save(hdfspath+"/Market/Stage1/It_Mst_Hd")
    
    Comm_Calc_Info= "(SELECT Excise_U,Code,Commission_P,Sp_Commission_P ,Rdf_P,Exchange_Rate,Agent_Code,Sl_Act_Code,STax_Act_Code,Sur_Act_Code,STax_Act_Code_1,\
                        ST_Branch_Code,Tax_1,Tax_2,Tax_3,Scheme_Rs,Scheme_U,Adjust_Rs from Comm_Calc_Info) as CCI"
    Comm_Calc_Info = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Comm_Calc_Info,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Comm_Calc_Info.write.mode(owmode).save(hdfspath+"/Market/Stage1/Comm_Calc_Info")
    
    Gift_Vouch_Mst = "(SELECT Sale_Fin_Year,Gift_Vouch_Prefix,Gift_Vouch_Number,Code,Gift_Vouch_Barcode,Create_Date,Expiry_Date,Sale_vouch_code,Amount,Gift_Voucher_Group_Code  from Gift_Vouch_Mst) as GVM"
    Gift_Vouch_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Gift_Vouch_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Gift_Vouch_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Gift_Vouch_Mst")
    
    Discount_Coupon_Mst = "(SELECT Sale_Fin_Year,Sale_vouch_code,CD_Rs,Discount_Coupon_Prefix,Code,Discount_Coupon_Number,Expiry_Date,CD_P,Discount_Coupon_Barcode,Discount_Coupon_Group_Code from Discount_Coupon_Mst) as CDM"
    Discount_Coupon_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Discount_Coupon_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Discount_Coupon_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Discount_Coupon_Mst")
    
    
    Setup_Masters = "(SELECT Mst_Name, Code,General_Text_1  from Setup_Masters) as CDG"
    Setup_Masters = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Setup_Masters,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Setup_Masters.write.mode(owmode).save(hdfspath+"/Market/Stage1/Setup_Masters")
    

    Scheme_Campaign_Group_Mst = "(SELECT Code,Scheme_Group_Name,Scheme_Campaign_Code  from Scheme_Campaign_Group_Mst) as SCGM"
    Scheme_Campaign_Group_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Scheme_Campaign_Group_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Scheme_Campaign_Group_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Mst")
    
   
   
    Scheme_Campaign_Mst = "(SELECT Code,Scheme_Campaign_Name,Date_From,Date_To  from Scheme_Campaign_Mst) as SCM"
    Scheme_Campaign_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Scheme_Campaign_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Scheme_Campaign_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Scheme_Campaign_Mst")
    
    Scheme_Campaign_Group_Det = "(SELECT Scheme_Campaign_Group_code,Code_Type,Item_Code  from Scheme_Campaign_Group_Det) as SCM"
    Scheme_Campaign_Group_Det = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Scheme_Campaign_Group_Det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Scheme_Campaign_Group_Det.write.mode(owmode).save(hdfspath+"/Market/Stage1/Scheme_Campaign_Group_Det")
    '''
    ####
    '''
    Chal_Txn= "(SELECT Org_Qty,Sa_Subs_Lot,Rate,Tot_Qty,Free_Qty,Repl_Qty,Sample_Qty,Calc_Gross_Amt,Calc_Spdisc,Calc_DN,Calc_cn,Calc_Display,Calc_Handling,Calc_Postage,\
                 Calc_Net_Amt,Calc_Commission,Calc_Sp_Commission,Calc_Rdf,Calc_Scheme_U,Calc_Scheme_Rs,Calc_MFees,Calc_Labour,calc_round,Calc_Tax_1,\
                 Calc_tax_2,Calc_Tax_3,Calc_Sur_On_Tax3,Calc_Excise_U,Calc_Adjustment_u,Calc_Adjust_RS,Calc_Freight,Calc_Adjust,Sale_Or_SR,vouch_code,\
                 Lot_Code,Godown_Code,Item_Det_Code,Comm_Calc_Code from Chal_Txn) as Chal_Txn"  
    Chal_Txn= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Chal_Txn,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()  
    Chal_Txn.write.mode(owmode).save(hdfspath+"/Market/Stage1/Chal_Txn")
    
    Gift_Vouch_Mst = "(SELECT Gift_Vouch_Num,Sale_Fin_Year,Gift_Vouch_Prefix,Gift_Vouch_Number,Code,Gift_Vouch_Barcode,Create_Date,Expiry_Date,Sale_vouch_code,Amount,Gift_Voucher_Group_Code  from Gift_Vouch_Mst) as GVM"
    Gift_Vouch_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Gift_Vouch_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    Gift_Vouch_Mst.write.mode(owmode).save(hdfspath+"/Market/Stage1/Gift_Vouch_Mst")
    
    
    Sl_h22="(SELECT vouch_code,MPM_Code,net_amt FROM sl_head20212022) as SLH22"
    Sl_h22= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Sl_h22,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    Sl_h22.write.mode(owmode).save(hdfspath+"/Market/Stage1/Sl_h22")
    
    SLCC="(SELECT MPM_Code,Vouch_Code,Code,CC_Amount,CC_No_Code FROM Sl_CC_Det20212022  ) as SLcc"
    SLCC= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=SLCC,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    SLCC.write.mode(owmode).save(hdfspath+"/Market/Stage1/SLCC")
    
    SL_Sch = "(SELECT Code_Type,Sch_Det_Code,SL_Txn_Code,CD,TD,CALC_CD,CALC_TD  from SL_Sch20192020) as SS"
    SL_Sch = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=SL_Sch,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    SL_Sch.write.mode(owmode).save(hdfspath+"/Market/Stage1/SL_Sch")
    
    Sl_MPM="(SELECT Vouch_Code,Code,Cash_Amount ,CC_Amount,Credit_Amount,Gift_Vouch_Amount FROM Sl_MPM20212022 ) as SLMPM"
    Sl_MPM= sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Sl_MPM,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    Sl_MPM.write.mode(owmode).save(hdfspath+"/Market/Stage1/Sl_MPM")
    '''
    
    
    
    #print(SLTXN.columns)
    
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
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'MasterDataIngestion','DB':DB,'EN':Etn,
        'Status':'Failed','ErrorLineNo':str(exc_traceback.tb_lineno),'Operation':'NA','Rows':'0','BeforeETLRows':'0','AfterETLRows':'0'}]
    log_df = sqlctx.createDataFrame(log_dict,schema_log)
    log_df.write.jdbc(url=Sqlurlwrite, table="Logs", mode="append")
    
print('MasterDataIngestion end: ', datetime.datetime.now())
print("\U0001F600")