#print("Shivam")
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import concat_ws, when, substring
from pyspark.sql.functions import last
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import csv,io,os,re,traceback,sys
import datetime,time,pyspark,udl
import pandas as pd
from pyspark.sql.types import IntegerType
from calendar import month
#from pandas.tests.tseries.frequencies.test_inference import day
#from _overlapped import NULL


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
    
    conf = SparkConf().setMaster("local[*]").setAppName("Stage1:Data_Ingestion").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set('spark.local.dir', path+"/dump")\
                .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Stage1:Data_Ingestion").getOrCreate()
   
    Sqlurl=SURL+";databaseName="+RDB+";user="+RUSER+";password="+RPASS
    
    Sqlurlwrite=SURL+";databaseName="+WDB+";user="+RUSER+";password="+RPASS
    
    Accounts = "(SELECT Act_Name, City_Start_Pos, City_Code, User_Code AS ACT_User_Code, Print_Act_Name, Act_Code, Grp_Code, User_Code from Accounts) as Act"
    Accounts = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Accounts,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
  
    
    Tax_Regions = "(SELECT Tax_Reg_Name, Tax_Reg_Code from Tax_Regions) as TR"
    Tax_Regions = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Tax_Regions,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
   
    City_ = "(SELECT City_Name, City_Code from City_) as Ct"
    City_ = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=City_,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    City_.show()
    exit()
    
    Sl_Head20192020 = "(SELECT Bill_Cust_Code, Branch_Code, Tax_Reg_Code, Vouch_Num, Vouch_Date, Deleted, Vouch_Code, Series_Code, Number_, Net_Amt, New_Vouch_Num, Pay_Mode, Comm_Calc_Code, Act_Code_For_Txn_X, Agent_Code, Stock_Trans from Sl_Head20192020) as HD"
    Sl_Head20192020 = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Sl_Head20192020,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()  
     
    
    Sl_Head20192020 = Sl_Head20192020.withColumn('Deleted', Sl_Head20192020['Deleted'].cast(IntegerType()))

    
          
   # Sl_Txn20192020 = "(SELECT AVG(Rate) AS RatePerUnit, Vouch_Code, Lot_Code, SUM(Tot_Qty/CF_Qty) AS Int_Total_Packs, AVG(CF_Qty) AS Units_Per_Pack, SUM(Tot_Qty / 1) AS TotQty, SUM(Tot_Qty / 1) AS SaleQty, SUM(Repl_Qty) AS ReplQty, SUM(Sample_Qty / 1) AS SampleQty, SUM(Calc_Gross_Amt) AS Gross_Value, SUM(Calc_Net_Amt) AS Net_Sale_Value, SUM(Calc_Commission) AS CD, SUM(Calc_Sp_Commission) AS TD, SUM(Calc_Rdf) AS SpCD, SUM(Calc_Scheme_U) AS Scheme_Unit, SUM(Calc_Scheme_Rs) AS Scheme_Rs, SUM(Calc_tax_1) AS Tax1, SUM(Calc_tax_2) AS Tax2, SUM(Calc_Tax_3 + Calc_Sur_On_Tax3) AS Tax3, SUM(Calc_Tax_3) AS Tax3WithoutSur, SUM(Calc_Sur_On_Tax3) AS Surcharge, SUM(Calc_Excise_U) AS ExciseUnit, SUM(Calc_Adjustment_u) AS AdjustPerUnit, SUM(Calc_Adjust_RS) AS AdjustItem, SUM(Calc_Freight) AS Freight, SUM(Calc_Adjust) AS AdjustmentRupees, SUM(Calc_Spdisc) AS SPDiscount, SUM(Calc_DN) AS DebitNote, SUM(Calc_cn) AS CreditNote, SUM(Calc_Display) AS Display, SUM(Calc_Handling) AS Handling, SUM(Calc_Postage) AS Postage, SUM(Calc_MFees) AS ExcisePercentage, SUM(Calc_Labour) AS Labour, SUM(Calc_Gross_Amt + Calc_commission + calc_sp_commission + calc_rdf + calc_scheme_u + calc_scheme_rs + Calc_Tax_1 + Calc_Tax_2 + Calc_Tax_3 + calc_sur_on_tax3 + calc_mfees + calc_excise_u + Calc_adjustment_u + Calc_adjust_rs + Calc_freight + calc_adjust + Calc_Spdisc + Calc_DN + Calc_CN + Calc_Display + Calc_Handling + calc_Postage + calc_Round + calc_Labour) As UserDefNetAmt, SUM(TXN.Calc_Gross_Amt)  As UserDefNetAmt_2, SUM(Calc_Round) As Round_Amt, Sale_Or_SR, Comm_Calc_Code, Item_Det_Code, Sa_Subs_Lot, Deleted from Sl_Txn20192020) as Txn"
   # Sl_Txn20192020 = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Sl_Txn20192020,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()  
     
    
    
     
    #Lot_Mst = "(SELECT Pur_Rate, AVG(Sale_rate), Lot_Code As UDR_Rate, Item_Det_Code from Lot_Mst) as LM"
   # Lot_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Lot_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Branch_Mst = "(SELECT Branch_Name, Branch_Code, Rate_Group_Code from Branch_Mst) as BM"
    Branch_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Branch_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Comm_Calc_Info = "(SELECT Commission_P As CD_P, Sp_Commission_P As TD_P, Rdf_P As SPCD_P, Code  from Comm_Calc_Info) as CCI"
    Comm_Calc_Info = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Comm_Calc_Info,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    It_Mst_O_Det = "(SELECT Code from It_Mst_O_Det) as IMOD"
    It_Mst_O_Det = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_O_Det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    #It_Mst_Det = "(SELECT Item_O_Det_Code, Item_Hd_Code, Pack_Code, Item_Det_Code from Lot_Mst) as IMD"
    #It_Mst_Det = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_Det,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Godown_Mst = "(SELECT Godown_Code from Godown_Mst) as GDM" 
    Godown_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Godown_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Branch_Mst = "(SELECT Branch_Code, Group_Code1, Branch_Name, Rate_Group_Code from Branch_Mst) as BM"
    Branch_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Branch_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Branch_Groups = "(SELECT Group_Code from Branch_Groups) as BG"
    Branch_Groups = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Branch_Groups,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    It_Mst_Hd = "(SELECT Item_Hd_Code, Comp_Code, Color_Code, Group_Code, Item_Name_Code from It_Mst_Hd) as IMH"
    It_Mst_Hd = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_Hd,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Comp_Mst = "(SELECT Comp_Code from Comp_Mst) as CM"
    Comp_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Comp_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
  
    
    Item_Color_Mst = "(SELECT Color_Code from Item_Color_Mst) as ICM"
    Item_Color_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Item_Color_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Group_Mst = "(SELECT Group_Code from Group_Mst) as GM"
    Group_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Group_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Pack_Mst = "(SELECT Pack_Code from Pack_Mst) as PM"
    Pack_Mst = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Pack_Mst,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    It_Mst_Names = "(SELECT Item_Name_Code from It_Mst_Names) as IMN"
    It_Mst_Names = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=It_Mst_Names,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    AccountGroups = "(SELECT Grp_Code from AccountGroups) as AG"
    AccountGroups = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=AccountGroups,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    Agents_Brokers = "(SELECT Code from Agents_Brokers) as AB"
    Agents_Brokers = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=Agents_Brokers,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
    
    
    
    #City_ = "(SELECT City_Name from City_) as Ct"
    #City_ = sqlctx.read.format("jdbc").options(url=Sqlurl,dbtable=City_,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
  
    
    
    Sl_Head20192020 = Sl_Head20192020.withColumn('MonthOrder', when(month(Sl_Head20192020['Vouch_Date'] < 4), month(Sl_Head20192020['Vouch_Date']) + 12).otherwise(month(Sl_Head20192020['Vouch_Date'])))
    
    Sl_Head20192020 = Sl_Head20192020.withColumn('DayOrder', day(Sl_Head20192020['Vouch_Date']))
    
    Sl_Txn20192020 = Sl_Txn20192020.filter(Sl_Txn20192020.Sa_Subs_Lot == 0)
    
    Sl_Head20192020 = Sl_Head20192020.filter(Sl_Head20192020.Deleted == 0)
    
    Sl_Txn20192020 = Sl_Txn20192020.filter(Sl_Txn20192020.Deleted == 0)
    
    Sl_Txn20192020 = Sl_Txn20192020.filter(Sl_Txn20192020.Sale_Or_SR == 'SL')
    
    Sl_Head20192020 = Sl_Head20192020.filter(Sl_Head20192020.Stock_Trans == 1)
    
    Sl_Head20192020 = Sl_Head20192020.filter(Sl_Head20192020.Vouch_Date.between(pd.to_datetime('2019-09-16'),pd.to_datetime('2020-03-31')))
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Sl_Head20192020, on = ['Vouch_Code'], how = 'left')
    
    Sl_Txn20192020 = It_Mst_Det.join(It_Mst_O_Det, It_Mst_Det.Item_O_Det_Code == It_Mst_O_Det.Code, how = left)
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Accounts, Sl_Txn20192020.Cust_Code == Accounts.Act_Code, how = left)
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Lot_Mst, on = ['Lot_Code'], how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Godown_Mst, on = ['Godown_Code'], how = 'left') 
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Branch_Mst, on = ['Branch_Code'], how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Branch_Groups, Sl_Txn20192020.Group_Code1 == Branch_Groups.Group_Code, how = 'left')
    
    Sl_Txn20192020 = Accounts.join(City_, on = ['City_Code'], how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Tax_Regions, on = ['Tax_Reg_Code'], how = 'left')
    
    Sl_Txn20192020 = It_Mst_Hd.join(It_Mst_Det, on = ['Item_Hd_Code'], how ='left')
    
    Sl_Txn20192020 = It_Mst_Hd.join(Comp_Mst, on = ['Comp_Code'], how = 'left' )
    
    Sl_Txn20192020 = It_Mst_Hd.join(Item_Color_Mst, on = ['Color_Code'], how = 'left')
    
    Sl_Txn20192020 = It_Mst_Hd.join(Group_Mst, on = ['Group_Code'], how = 'left')
    
    Sl_Txn20192020 = It_Mst_Det.join(Pack_Mst, on = ['Pack_Code'], how = 'left')
    
    Sl_Txn20192020 = It_Mst_Hd.join(It_Mst_Names, on = ['Item_Name_Code'], how = 'left')
    
    Sl_Txn20192020 = It_Mst_Det.join(Lot_Mst, on =['Item_Det_Code'], how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Comm_Calc_Info, Sl_Txn20192020.Comm_Calc_Code == Comm_Calc_Info.Code, how ='left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(It_Mst_Det, on = ['Item_Det_Code'], how = 'left' )
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Comm_Calc_Info, Sl_Txn20192020.Comm_Calc_Code == Comm_Calc_Info.Code, how = 'left')
    
    Sl_Txn20192020 = AccountGroups.join(Accounts, on = ['Grp_Code'], how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Accounts, Sl_Txn20192020.Act_Code_For_Txn_X == Accounts.Act_Code, how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Agents_Brokers, Sl_Txn20192020.Agent_Code == Agents_Brokers.Code, how = 'left')
    
    Sl_Txn20192020 = Sl_Txn20192020.join(Comm_Calc_Info, Sl_Txn20192020.Comm_Calc_Code == Comm_Calc_Info.Code, how = 'left')
    
    
    Sl_Txn20192020 = Sl_Txn20192020.groupBy('Act_Name', 'City_Start_Pos', 'User_Code', 'Print_Act_Name', 'Act_Code', 'Tax_Reg_Name', 'Bill_Cust_Code', 'Vouch_Num', 'Vouch_Date', 'Vouch_Code', 'Series_Code', 'Number_', 'Net_Amt', 'New_Vouch_Num', 'Pay_Mode', 'Deleted', 'Pur_Rate', 'Branch_Name', 'Branch_Code', 'Rate_Group_Code', 'Commission_P', 'Sp_Commission_P', 'Rdf_P')
    
    
    Sl_Txn20192020 = Sl_Txn20192020.orderBy('Branch_Name', 'Vouch_Date', 'Series_Code', 'Number_', 'Act_Name', 'Sale_Or_SR')
    
    
    Sl_Txn20192020.show()
    
    
except Exception as ex:
    print(ex)
    print("test")