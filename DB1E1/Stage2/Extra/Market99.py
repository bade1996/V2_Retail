# old  version . New version Branch,Single_Table, trial

'''
Created on 23 Dec 2016

@author: Abhishek
'''


'''
Have to rename Dataframes of GM111 etc
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year,lit,concat
import re,keyring,os,datetime
import time,sys
from pyspark.sql.types import *

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')

try:
    conf = SparkConf().setMaster("local[8]").setAppName("Market99")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)

    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName("Market99").getOrCreate()
    sqlctx = SQLContext(sc)
    
    Sqlurl="jdbc:sqlserver://DEMO-KOCKPIT\Demo;databaseName=logic;user=sa;password=sa@123"
    Postgresurl = "jdbc:postgresql://localhost:5432/kockpit"
    
    
    Postgresprop= {
        "user":"postgres",
        "password":"sa@123",
        "driver": "org.postgresql.Driver" 
    }
   
    
    '''
    
    Table1="(Select Godown_Code FROM Godown_Mst) as GoDown"
    GDM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #GDM.show()
    
    Table1="(Select Code FROM It_Mst_O_Det ) as GoDown"
    IMOD = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #IMOD.show()
    
    Table1="(Select Group_Code1,Branch_Name,Branch_Code,Order_ FROM Branch_Mst ) as GoDown"
    BM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #BM.show()
    
    Table1="(Select Group_Code FROM Branch_Groups  ) as GoDown"
    BG = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #BG.show()
    
    Table1="(Select Item_Hd_Code,Item_Hd_Name,Comp_Code,Group_Code,Item_Name_Code,Group_Code2,Group_Code3\
    Group_Code4,Group_Code8,Group_Code11,Group_Code12,Group_Code16,Group_Code20  FROM It_Mst_Hd   ) as GoDown"
    ITM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #ITM.show()
    
    Table1="(Select User_Code as Item_Code,User_Code,Sale_Rate,Pur_Rate,Basic_Rate,Mrp , Cf_1 \
    , Sale_Rate As Rate,Item_Hd_Code,Pack_Code,Item_Det_Code , Basic_Rate  As Rate1 FROM It_Mst_Det   ) as GoDown"
    IMD = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #IMD.show()
    
    Table1="(Select Comp_Code, Comp_Name FROM Comp_Mst  ) as GoDown"
    CM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #CM.show()
    
    Table1="(Select Color_Code FROM Item_Color_Mst  ) as GoDown"
    ICM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #ICM.show()

    
    Table1="(Select Pack_Code,Link_Code FROM Pack_Mst  ) as GoDown"
    PM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #PM.show()
    
    Table1="(Select Item_Name_Code,Item_Name FROM It_Mst_Names  ) as GoDown"
    IMN = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #IMN.show()
    
    Table1="(Select Lot_Code,Puv_Code,Exp_dmg FROM Lot_Mst  ) as GoDown"
    LM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #LM.show()
    
    Table1="(Select Lot_Code,Puv_Code,Exp_dmg FROM Lot_Mst  ) as GoDown"
    LM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #LM.show()
        
    Table1="(Select Group_Code,Group_Name FROM Group_Mst  ) as GoDown"
    GM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #GM.show()
    
    GM11=GM
    GM = GM.drop("Group_Name")
    GM11 = GM11.withColumnRenamed("Group_Name","GroupName11")
    
    GM21= GM31= GM41= GM81= GM111=GM121= GM161=GM201= GM11
    
    GM21 = GM21.withColumnRenamed("Group_Name11","GroupName21")
    GM21 = GM21.withColumnRenamed("Group_Name11","GroupName21")'''
    
    Table1=" (SELECT        '' As Godown_Name, 0 As Godown_Code,'' As Godown_Grp_Name, 0 As Godown_Grp_Code, '' AS Branch_Group_Name, 0 AS Branch_Group_Code , BM.Branch_Name, \
    BM.Branch_Code, BM.Branch_Code As BranchCode,BM.Order_ As B_Order , '' As Prod_Unit, '' As Lot_Number1, '' as Lot_Code, '' as Last_Purchase_Date , 0 As Pack_Code , \
            '' As Pack_Name, 0 As Order_ , IMH.Item_Hd_Code , IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name ,\
                        0 As ItemGroup_Code, '' As Group_Name ,    0 As Color_Code, '' As Color_Name, '' As Color_Short_Name  , '' As User_Code, 0 As Item_Det_Code ,\
            IMD.User_Code As Item_Code , (CAST(IMH.Item_Hd_Code AS VARCHAR) + '|' +  CAST(IMN.Item_Name_Code AS VARCHAR) + '|' +  CAST(CM.Comp_Code AS VARCHAR) + '|' +  CAST(IMD.User_Code AS VARCHAR) ) As ItemChangeCode , \
            PM.Link_Code ,IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 , 0 As ConsRate , IMD.Sale_Rate   As Rate , IMD.Basic_Rate  As Rate1 ,\
Cast(CID.Item_Desc_M As VarChar(300)) As Item_Desc ,  GM11.Group_Name As GroupName11, '1|1' As GroupLevel11,  GM21.Group_Name As GroupName21, '2|1' As GroupLevel21,\
GM31.Group_Name As GroupName31, '3|1' As GroupLevel31,  GM41.Group_Name As GroupName41, '4|1' As GroupLevel41,  GM81.Group_Name As GroupName81, '8|1' As GroupLevel81,\
GM111.Group_Name As GroupName111, '11|1' As GroupLevel111,  GM121.Group_Name As GroupName121, '12|1' As GroupLevel121,  GM161.Group_Name As GroupName161,\
'16|1' As GroupLevel161,  GM201.Group_Name As GroupName201, '20|1' As GroupLevel201, GM11.Group_Code As GroupCode11, GM21.Group_Code As GroupCode21,\
GM31.Group_Code As GroupCode31, GM41.Group_Code As GroupCode41, GM81.Group_Code As GroupCode81, GM111.Group_Code As GroupCode111, GM121.Group_Code As GroupCode121, \
GM161.Group_Code As GroupCode161, GM201.Group_Code As GroupCode201, \
CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)  + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  CAST(GM201.Group_Code As VarChar)  As GroupChangeCode , \
0 AS SlQtyCF, 0 AS SrQtyCF, 0 As SlSTQtyCF, 0 As SrSTQtyCF,0 as SlFreeQty,0 as SlSTFreeQty,0 as SRFreeQty ,0 as Godown_STI ,0 as Godown_STO , \
0 As SlQtyCF_2 ,0 As SrQtyCF_2, 0 As SlSTQtyCF_2 , 0 As SrSTQtyCF_2 , 0 AS SlVal, 0 AS SrVal,0 AS SlSTVal, 0 AS SrSTVal , 0 AS RecVal, 0 as IssueVal , \
0 AS PuQtyCF,0 as PuFreeQtyCF, 0 AS PrQtyCF,0 AS PuQtyCF_2,0 AS PrQtyCF_2 , 0 AS PuSTQtyCF,0 as PuSTFreeQtyCF, 0 AS PrSTQtyCF,0 AS PuSTQtyCF_2,0 AS PrSTQtyCF_2 , \
0 as PuVal, 0 as PrVal , 0 AS PuSTVal , 0 AS PrSTVal , 0 AS RecQty, 0 as IssueQty , SUM(TXN.Net_Qty /1) As NetQtyCF , SUM(TXN.Net_Qty /1) As NetQtyCF_2 , \
SUM(TXN.Net_Qty * IMD.basic_rate) AS NetQtyVal,0 as NetQtyVal2 , '' As TempDate , 0 As PartyGrpCode, '' As PeriodFrom, 0 As BillCount, 0 As PartyCount, \
'OP' As ValueType , 0 As TotalItems  \
FROM        \
            Godown_Mst AS GDM with (NOLOCK) ,It_Mst_O_Det AS IMOD with (NOLOCK), Branch_Mst AS BM with (NOLOCK), \
                        Branch_Groups BG with (NOLOCK),  It_Mst_Hd As IMH, It_Mst_Det As IMD, Comp_Mst As CM , Item_Color_Mst As ICM, Group_Mst As GM, Pack_Mst As PM, \
                        It_Mst_Names As IMN , Lot_Mst as LM with (nolock) , Group_Mst As Gm11, Group_Mst As Gm21, Group_Mst As Gm31, Group_Mst As Gm41, Group_Mst As Gm81, \
                        Group_Mst As Gm111, Group_Mst As Gm121, Group_Mst As Gm161, Group_Mst As Gm201, Comm_It_Desc As CID with (NOLOCK) , stk_dtxn20182019 AS TXN with (NOLOCK) \
                        WHERE \
            TXN.Lot_Code = LM.Lot_Code And TXN.Godown_Code = GDM.Godown_Code  AND TXN.Branch_Code = BM.Branch_Code And IMD.Item_O_Det_Code = IMOD.Code  AND\
            BG.Group_Code = BM.Group_Code1 AND  IMH.Item_Hd_Code = IMD.Item_Hd_Code AND IMH.Comp_Code = CM.Comp_Code  AND IMH.Color_Code = ICM.Color_Code AND\
                        IMH.Group_Code = GM.Group_Code  AND IMD.Pack_Code = PM.Pack_Code AND IMH.Item_Name_Code = IMN.Item_Name_Code  AND IMD.Item_Det_Code = LM.Item_Det_Code  AND\
                                   IMH.Group_Code= GM11.Group_Code  AND IMH.Group_Code2= GM21.Group_Code  AND IMH.Group_Code3= GM31.Group_Code  AND IMH.Group_Code4= GM41.Group_Code  AND \
            IMH.Group_Code8= GM81.Group_Code  AND IMH.Group_Code11= GM111.Group_Code  AND IMH.Group_Code_12 = GM121.Group_Code  AND IMH.Group_Code16= GM161.Group_Code  AND \
            IMH.Group_Code20= GM201.Group_Code  AND (Not LM.Puv_Code Like 'CS%') AND \
IMD.Item_Det_Code IN(11626,11874,11928,11929,11930,11931,11932,11933,11934,11935,11936,11937,11938,11939,11962,12069,12245,12246,12339,12357,12566,12567,13002,13003,13004,42015,59640,59641,59642,59067,59068,59069,59070,59071,59072,59073,59074,59075,59076,59077,59078,59079,59080,65652,65653,65654,65655,65656,65657,65658,65659,65660,65661,44850,64488,45259,45263,53404,53409,52634,38424,38426,56509,56510,56511,56375,56378,56374,56377,56376,56379,52185,52188,60881,59240,52186,52189,59241,52187,52190,59237,59234,59238,59235,59239,59236,56505,56508,56381,56384,56503,56506,56380,56383,56504,56507,56382,56385,61460,59250,59252,59249,59251,59243,59246,59245,59248,59244,59247,61464,46609,38427,61467,44789,44790,47569,42913,51337,61465,38431,46613,46610,38428,61468,51341,44791,44793,47570,42914,51338,61466,61469,44792,44794,47571,51339,61471,61470,61472,52181,52184,52180,59242,56369,56372,56370,56373,56368,56371,56512,46615,44787,42919,56513,46616,44788,47573,42920,56514,44795,42921,50125,51296,51297,51298,51299,45575,38631,69154,59294,59300,59295,59301,59296,59305,59297,69156,60332,60333,60334,69157,47262,47286,47287,47288,47265,47289,47267,47291,47292,47269,47281,47293,47294,47295,47272,47296,47297,39082,59666,64484,64779,55399,64780,64787,64786,64781,64788,64782,69140,57513,42705,42707,42709,51478,51479,51481,48197,48239,59603,59604,51555,51557,51558,53109,52904,52906,52907,52908,54824,54825,54829,56812,57194,57195,57196,57197,57200,56766,56767,56768,56769,56770,56771,56772,56646,56647,57289,57290,57291,57292,57293,57294,57295,57296,56780,56781,56782,56783,56784,59738,59758,59739,59759,59740,59760,59741,59761,59742,59762,59743,59763,59767,59768,63306,64069,64070,64071,64072,62438,63297,63298,63299,63300,63301,62261,62262,62263,62264,62265,62266,62267,62268,62269,62270,63809,63810,63811,63812,63813,62806,62807,62808,62809,62810,62811,62840,62841,62842,62689,62688,62690,69158,66272,66273,66274,66275,67576,67599,67600,67609,67828,67829,67830,67831,67832,67833,67834,67835,67836,67837,67838,67839,67840,67841,67842,67843,67844,67845,67846,67674,67701,67810,68991,68992,68874,68743,43163,50227,50228,41561,52821,57462,57464,57465,54904,57466,52825,57467,52826,54905,46685,46686,47546,60777,60776,60771,60772,64493,60779,60778,60785,60786,60787,60784,60782,60781,60780,60783,60774,60775,60773,64497,64496,64498,66968,66969,66967,59693,66059,66058,60877,62635,62636,62634,62633,66057,66056,60876,62638,62637,62639,62640,41547,41546,39247,58399,58398,57447,57468,59367,64489,64490,43168,45372,58372,52077,52078,52079,52080,52081,52082,52083,51300,51301,51302,51303,51304,58245,58246,57453,57454,57452,57448,57449,57511,57512,58175,58174,53516,53517,53518,53519,65684,57451,58177,57450,65685,59805,32007,31828,34890,34899,35952,32945,45366,45367,59368,61748,69270,69272,69271,69273,69275,69274,69282,69281,69284,69283,69280,69279,61874,61875,61876,61877,66758,69255,69254,66760,66759,69257,69256,66761,69267,69268,69266,69269,66702,66706,61868,61869,61870,61871,61872,61873,61878,61879,61880,61881,61882,66704,66703,66707,66705,64775,64776,64784,64783,64777,64785,64778,32008,58370,58369,46668,45192,40642,40643,40641,55029,55030,46622,55008,50133,59448,59447,59446,56307,59445,52656,59444,59443,59442,59441,53911,53909,53899,55028,55007,56339,56340,58311,58289,59440,59439,59449,59160,59180,59181,59186,59182,59183,59184,59185,59159,60792,60720,60719,60791,61440,61439,61438,39192,51324,51325,67128,67127,67129,67132,67131,45381,45382,58251,59365,59366,58250,37307,48502,38054,36555,43167,64160,64161,64275,64276,64277,64274,55640,58048,45368,26289,58697,58479,42514,58698,26291,58699,62973,63181,63182,63183,63184,45369,69464,58371,58256,58257,60878,58480,58373,29959,58243,58260,57622,58244,58261,58242,58259,58241,58258,64162,45365,39191,47544,58254,59371,58255,59372,58252,59369,58253,59370,34196,34198,32951,32952,33039,33045,33048,33049,39195,51953,41558,35951,58247,58249,58262,58248,65669,38310,60607,31870,43268,32683,33611,31576,35648,35705,67017,67320,67015,67009,67016,67319,67013,67010,67011,67012,67020,67014,67008,67018,67019,67021,35953,48896,42134,47053,48895,42128,48897,42130,57910,45273,57919,60673,42148,42129,57918,56993,60672,42146,42145,42147,56994,47055,57916,56992,45371,64491,64492,34194,61687,61688,61686,60879,62632,62631,62630,62629,62624,62623,62622,62621,62628,62627,62626,62625)  AND GDM.Godown_Code IN (0) AND TXN.Branch_Code IN (5033,5013,5038,5031,5028,5025,5016,5015,5024,5043,5021,5017,5032,5008,5007,5009,5045,5027,5037,5000,5022,5044,5039,5047,5034,5018,5006,5011,5012,5023,5041,5042,5036,5046,5019,5030,5026,5029,5010,5049,5035,5040,5020,5048,5002,5003,5005,5004,5001)\
AND CID.Code = IMH.Comm_It_Desc_Code  AND LM.Exp_dmg='N' AND TXN.Date_ BETWEEN CONVERT(smalldatetime,('31/03/2018'),103)  AND\
            CONVERT(smalldatetime,('31/07/2018'),103)   \
            GROUP BY \
              BM.Branch_Name, BM.Branch_Code,BM.Order_, PM.Link_Code,IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 ,IMD.Sale_Rate ,IMD.Basic_Rate ,\
              Cast(CID.Item_Desc_M As VarChar(300)) ,  GM11.Group_Name ,  GM21.Group_Name ,  GM31.Group_Name ,  GM41.Group_Name ,  GM81.Group_Name ,  \
              GM111.Group_Name ,  GM121.Group_Name ,  GM161.Group_Name ,  GM201.Group_Name , GM11.Group_Code , GM21.Group_Code , GM31.Group_Code , GM41.Group_Code , \
              GM81.Group_Code , GM111.Group_Code , GM121.Group_Code , GM161.Group_Code , GM201.Group_Code ,  \
              CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)  + '|' +  CAST(GM41.Group_Code As VarChar)  \
              + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  CAST(GM121.Group_Code As VarChar)  + '|' +  \
              CAST(GM161.Group_Code As VarChar)  + '|' +  CAST(GM201.Group_Code As VarChar) ,            IMH.Item_Hd_Code , IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , IMD.User_Code  \
UNION ALL  \
SELECT        '' As Godown_Name, 0 As Godown_Code,'' As Godown_Grp_Name, 0 As Godown_Grp_Code, '' AS Branch_Group_Name, 0 AS Branch_Group_Code , BM.Branch_Name, \
            BM.Branch_Code, BM.Branch_Code As BranchCode,BM.Order_ As B_Order , '' As Prod_Unit, '' As Lot_Number1, '' as Lot_Code, '' as Last_Purchase_Date , \
            0 As Pack_Code , '' As Pack_Name, 0 As Order_ , IMH.Item_Hd_Code , IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , \
            0 As ItemGroup_Code, '' As Group_Name , 0 As Color_Code, '' As Color_Name, '' As Color_Short_Name  , '' As User_Code, 0 As Item_Det_Code , \
            IMD.User_Code As Item_Code , (CAST(IMH.Item_Hd_Code AS VARCHAR) + '|' +  CAST(IMN.Item_Name_Code AS VARCHAR) + '|' +  CAST(CM.Comp_Code AS VARCHAR) + '|' + \
            CAST(IMD.User_Code AS VARCHAR) ) As ItemChangeCode , PM.Link_Code ,IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 , 0 As ConsRate , \
            IMD.Sale_Rate   As Rate , IMD.Basic_Rate  As Rate1 ,Cast(CID.Item_Desc_M As VarChar(300)) As Item_Desc ,  GM11.Group_Name As GroupName11, '1|1' As GroupLevel11, \
            GM21.Group_Name As GroupName21, '2|1' As GroupLevel21,  GM31.Group_Name As GroupName31, '3|1' As GroupLevel31,  GM41.Group_Name As GroupName41, \
            '4|1' As GroupLevel41,  GM81.Group_Name As GroupName81, '8|1' As GroupLevel81,  GM111.Group_Name As GroupName111, '11|1' As GroupLevel111, \
             GM121.Group_Name As GroupName121, '12|1' As GroupLevel121,  GM161.Group_Name As GroupName161, '16|1' As GroupLevel161,  GM201.Group_Name As GroupName201, \
            '20|1' As GroupLevel201, GM11.Group_Code As GroupCode11, GM21.Group_Code As GroupCode21, GM31.Group_Code As GroupCode31, GM41.Group_Code As GroupCode41, \
            GM81.Group_Code As GroupCode81, GM111.Group_Code As GroupCode111, GM121.Group_Code As GroupCode121, GM161.Group_Code As GroupCode161, \
            GM201.Group_Code As GroupCode201,  CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)  \
            + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  \
            CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  CAST(GM201.Group_Code As VarChar)  As GroupChangeCode , \
            SUM((TXN.Sl_Qty- TXN.Sl_Free- TXN.Sl_Repl- TXN.Sl_Sample)/1) As SlQtyCF, SUM((TXN.Sr_Qty- TXN.Sr_Free- TXN.Sr_Repl- TXN.Sr_Sample)/1) AS SrQtyCF, \
            0 As SlSTQtyCF, 0 As SrSTQtyCF , SUM(CASE WHEN TXN.Txn_Type = 'NO' OR BM.Show_ST_As_Sale=1 THEN  ( TXN.Sl_Free + TXN.Sl_Repl +TXN.Sl_Sample )/1 ELSE  0 END ) As SlFreeQty ,\
            SUM(CASE WHEN TXN.Txn_Type = 'ST' AND BM.Show_ST_As_Sale=0 THEN  ( TXN.Sl_Free + TXN.Sl_Repl +TXN.Sl_Sample )/1 ELSE 0 END ) As SlSTFreeQty ,\
            SUM(CASE WHEN TXN.Txn_Type = 'NO' OR BM.Show_ST_As_Sale=1 THEN  ( TXN.SR_Free + TXN.SR_Repl +TXN.SR_Sample )/1 ELSE  0 END ) As SRFreeQty ,\
            SUM(  TXN.Stk_In ) as Godown_STI ,SUM(  TXN.Stk_Out ) as Godown_STO , SUM((TXN.Sl_Qty- TXN.Sl_Free- TXN.Sl_Repl- TXN.Sl_Sample)/1) As SlQtyCF_2, \
            SUM((TXN.Sr_Qty- TXN.Sr_Free- TXN.Sr_Repl- TXN.Sr_Sample)/1) AS SrQtyCF_2, 0 As SlSTQtyCF_2, 0 As SrSTQtyCF_2 , SUM(TXN.sl_net_val) AS SlVal,\
            SUM(TXN.sr_net_val) AS SrVal, 0 As SlSTVal, 0 As SrSTVal , SUM((TXN.rec_qty +  TXN.Prod_Qty) * IMD.basic_rate) AS RecVal , \
            SUM((TXN.issue_qty  + TXN.Prod_Issue) * IMD.basic_rate) As IssueVal , SUM(CASE WHEN TXN.Txn_Type='NO'  THEN (TXN.Pu_Qty -TXN.Pu_Free-TXN.Pu_Repl-TXN.Pu_Sample/1) ELSE 0 END)  As PuQtyCF , \
            SUM(CASE WHEN TXN.Txn_Type='NO' THEN (TXN.Pu_Free+TXN.Pu_Repl+TXN.Pu_Sample/1 ) ELSE 0 END)  As PuFreeQtyCF , \
            SUM(CASE WHEN TXN.Txn_Type='NO' THEN (TXN.Pr_Qty-TXN.Pr_Free-TXN.Pr_Repl-TXN.Pr_Sample/1 ) ELSE 0 END)  As PrQtyCF , \
            SUM(CASE WHEN TXN.Txn_Type='NO'  THEN (TXN.Pu_Qty -TXN.Pu_Free-TXN.Pu_Repl-TXN.Pu_Sample/1) ELSE 0 END)  As PuQtyCF_2 , \
            SUM(CASE WHEN TXN.Txn_Type='NO'  THEN (TXN.Pr_Qty-TXN.Pr_Free-TXN.Pr_Repl-TXN.Pr_Sample/1) ELSE 0 END)  As PrQtyCF_2 , \
            SUM(CASE WHEN TXN.Txn_Type='ST'  THEN (TXN.Pu_Qty -TXN.Pu_Free-TXN.Pu_Repl-TXN.Pu_Sample/1) ELSE 0 END)  As PuSTQtyCF , \
            SUM(CASE WHEN TXN.Txn_Type='ST' THEN (TXN.Pu_Free+TXN.Pu_Repl+TXN.Pu_Sample/1 ) ELSE 0 END)  As PuSTFreeQtyCF ,\
            SUM(CASE WHEN TXN.Txn_Type='ST' THEN (TXN.Pr_Qty-TXN.Pr_Free-TXN.Pr_Repl-TXN.Pr_Sample/1 ) ELSE 0 END)  As PrSTQtyCF, \
            SUM(CASE WHEN TXN.Txn_Type='ST'  THEN (TXN.Pu_Qty -TXN.Pu_Free-TXN.Pu_Repl-TXN.Pu_Sample/1) ELSE 0 END)  As PuSTQtyCF_2 , \
            SUM(CASE WHEN TXN.Txn_Type='ST'  THEN (TXN.Pr_Qty-TXN.Pr_Free-TXN.Pr_Repl-TXN.Pr_Sample/1) ELSE 0 END)  As PrSTQtyCF_2 , \
            SUM(CASE WHEN   TXN.Txn_Type='NO' THEN TXN.pu_Gross_Val ELSE 0 END) AS PuVal , SUM(CASE WHEN   TXN.Txn_Type='NO' THEN TXN.pr_Gross_Val ELSE 0 END) AS PrVal , \
            SUM(CASE WHEN   TXN.Txn_Type='ST' THEN TXN.pu_Gross_Val ELSE 0 END) AS PuSTVal , SUM(CASE WHEN   TXN.Txn_Type='ST' THEN TXN.pr_Gross_Val ELSE 0 END) AS PrSTVal , \
            SUM((TXN.rec_qty + TXN.Prod_Qty) /1) AS RecQty, SUM((TXN.issue_qty  + TXN.Prod_Issue) /1) as IssueQty ,0 As NetQtyCF  ,0 As NetQtyCF_2 ,\
            0 As NetQtyVal, 0 AS NetQtyVal2 , '' As TempDate ,0 As PartyGrpCode, '' As PeriodFrom, 0 As BillCount, 0 As PartyCount, 'SLPU' as ValueType , \
            0 As TotalItems  \
            FROM \
            Godown_Mst AS GDM with (NOLOCK) ,It_Mst_O_Det AS IMOD with (NOLOCK), Branch_Mst AS BM with (NOLOCK), Branch_Groups BG with (NOLOCK), \
            It_Mst_Hd As IMH, It_Mst_Det As IMD, Comp_Mst As CM , Item_Color_Mst As ICM, Group_Mst As GM, Pack_Mst As PM, It_Mst_Names As IMN , Lot_Mst as LM with (nolock) , \
            Group_Mst As Gm11, Group_Mst As Gm21, Group_Mst As Gm31, Group_Mst As Gm41, Group_Mst As Gm81, Group_Mst As Gm111, Group_Mst As Gm121, Group_Mst As Gm161, \
            Group_Mst As Gm201, Comm_It_Desc As CID with (NOLOCK) , stk_dtxn20182019 AS TXN with (NOLOCK)  \
             \
WHERE        \
            TXN.Lot_Code = LM.Lot_Code And TXN.Godown_Code = GDM.Godown_Code  AND TXN.Branch_Code = BM.Branch_Code And IMD.Item_O_Det_Code = IMOD.Code  AND \
            BG.Group_Code = BM.Group_Code1 AND  IMH.Item_Hd_Code = IMD.Item_Hd_Code AND IMH.Comp_Code = CM.Comp_Code  AND IMH.Color_Code = ICM.Color_Code AND\
            IMH.Group_Code = GM.Group_Code  AND IMD.Pack_Code = PM.Pack_Code AND IMH.Item_Name_Code = IMN.Item_Name_Code  AND IMD.Item_Det_Code = LM.Item_Det_Code  AND \
            IMH.Group_Code= GM11.Group_Code  AND IMH.Group_Code2= GM21.Group_Code  AND IMH.Group_Code3= GM31.Group_Code  AND IMH.Group_Code4= GM41.Group_Code  AND \
            IMH.Group_Code8= GM81.Group_Code  AND IMH.Group_Code11= GM111.Group_Code  AND IMH.Group_Code_12 = GM121.Group_Code  AND IMH.Group_Code16= GM161.Group_Code  AND \
            IMH.Group_Code20= GM201.Group_Code  AND (Not LM.Puv_Code Like 'CS%') AND \
             \
            IMD.Item_Det_Code IN(11626,11874,11928,11929,11930,11931,11932,11933,11934,11935,11936,11937,11938,11939,11962,12069,12245,12246,12339,12357,12566,12567,13002,13003,13004,42015,59640,59641,59642,59067,59068,59069,59070,59071,59072,59073,59074,59075,59076,59077,59078,59079,59080,65652,65653,65654,65655,65656,65657,65658,65659,65660,65661,44850,64488,45259,45263,53404,53409,52634,38424,38426,56509,56510,56511,56375,56378,56374,56377,56376,56379,52185,52188,60881,59240,52186,52189,59241,52187,52190,59237,59234,59238,59235,59239,59236,56505,56508,56381,56384,56503,56506,56380,56383,56504,56507,56382,56385,61460,59250,59252,59249,59251,59243,59246,59245,59248,59244,59247,61464,46609,38427,61467,44789,44790,47569,42913,51337,61465,38431,46613,46610,38428,61468,51341,44791,44793,47570,42914,51338,61466,61469,44792,44794,47571,51339,61471,61470,61472,52181,52184,52180,59242,56369,56372,56370,56373,56368,56371,56512,46615,44787,42919,56513,46616,44788,47573,42920,56514,44795,42921,50125,51296,51297,51298,51299,45575,38631,69154,59294,59300,59295,59301,59296,59305,59297,69156,60332,60333,60334,69157,47262,47286,47287,47288,47265,47289,47267,47291,47292,47269,47281,47293,47294,47295,47272,47296,47297,39082,59666,64484,64779,55399,64780,64787,64786,64781,64788,64782,69140,57513,42705,42707,42709,51478,51479,51481,48197,48239,59603,59604,51555,51557,51558,53109,52904,52906,52907,52908,54824,54825,54829,56812,57194,57195,57196,57197,57200,56766,56767,56768,56769,56770,56771,56772,56646,56647,57289,57290,57291,57292,57293,57294,57295,57296,56780,56781,56782,56783,56784,59738,59758,59739,59759,59740,59760,59741,59761,59742,59762,59743,59763,59767,59768,63306,64069,64070,64071,64072,62438,63297,63298,63299,63300,63301,62261,62262,62263,62264,62265,62266,62267,62268,62269,62270,63809,63810,63811,63812,63813,62806,62807,62808,62809,62810,62811,62840,62841,62842,62689,62688,62690,69158,66272,66273,66274,66275,67576,67599,67600,67609,67828,67829,67830,67831,67832,67833,67834,67835,67836,67837,67838,67839,67840,67841,67842,67843,67844,67845,67846,67674,67701,67810,68991,68992,68874,68743,43163,50227,50228,41561,52821,57462,57464,57465,54904,57466,52825,57467,52826,54905,46685,46686,47546,60777,60776,60771,60772,64493,60779,60778,60785,60786,60787,60784,60782,60781,60780,60783,60774,60775,60773,64497,64496,64498,66968,66969,66967,59693,66059,66058,60877,62635,62636,62634,62633,66057,66056,60876,62638,62637,62639,62640,41547,41546,39247,58399,58398,57447,57468,59367,64489,64490,43168,45372,58372,52077,52078,52079,52080,52081,52082,52083,51300,51301,51302,51303,51304,58245,58246,57453,57454,57452,57448,57449,57511,57512,58175,58174,53516,53517,53518,53519,65684,57451,58177,57450,65685,59805,32007,31828,34890,34899,35952,32945,45366,45367,59368,61748,69270,69272,69271,69273,69275,69274,69282,69281,69284,69283,69280,69279,61874,61875,61876,61877,66758,69255,69254,66760,66759,69257,69256,66761,69267,69268,69266,69269,66702,66706,61868,61869,61870,61871,61872,61873,61878,61879,61880,61881,61882,66704,66703,66707,66705,64775,64776,64784,64783,64777,64785,64778,32008,58370,58369,46668,45192,40642,40643,40641,55029,55030,46622,55008,50133,59448,59447,59446,56307,59445,52656,59444,59443,59442,59441,53911,53909,53899,55028,55007,56339,56340,58311,58289,59440,59439,59449,59160,59180,59181,59186,59182,59183,59184,59185,59159,60792,60720,60719,60791,61440,61439,61438,39192,51324,51325,67128,67127,67129,67132,67131,45381,45382,58251,59365,59366,58250,37307,48502,38054,36555,43167,64160,64161,64275,64276,64277,64274,55640,58048,45368,26289,58697,58479,42514,58698,26291,58699,62973,63181,63182,63183,63184,45369,69464,58371,58256,58257,60878,58480,58373,29959,58243,58260,57622,58244,58261,58242,58259,58241,58258,64162,45365,39191,47544,58254,59371,58255,59372,58252,59369,58253,59370,34196,34198,32951,32952,33039,33045,33048,33049,39195,51953,41558,35951,58247,58249,58262,58248,65669,38310,60607,31870,43268,32683,33611,31576,35648,35705,67017,67320,67015,67009,67016,67319,67013,67010,67011,67012,67020,67014,67008,67018,67019,67021,35953,48896,42134,47053,48895,42128,48897,42130,57910,45273,57919,60673,42148,42129,57918,56993,60672,42146,42145,42147,56994,47055,57916,56992,45371,64491,64492,34194,61687,61688,61686,60879,62632,62631,62630,62629,62624,62623,62622,62621,62628,62627,62626,62625)  AND GDM.Godown_Code IN (0) AND TXN.Branch_Code IN (5033,5013,5038,5031,5028,5025,5016,5015,5024,5043,5021,5017,5032,5008,5007,5009,5045,5027,5037,5000,5022,5044,5039,5047,5034,5018,5006,5011,5012,5023,5041,5042,5036,5046,5019,5030,5026,5029,5010,5049,5035,5040,5020,5048,5002,5003,5005,5004,5001) \
             AND CID.Code = IMH.Comm_It_Desc_Code  AND LM.Exp_dmg='N' AND TXN.Date_ Between CONVERT(smalldatetime,('01/08/2018'),103)  AND \
            CONVERT(smalldatetime,('20/08/2018'),103) AND (TXN.Txn_Type='NO' Or BM.Show_ST_As_Sale=1)  GROUP BY  BM.Branch_Name, BM.Branch_Code,BM.Order_, \
            PM.Link_Code,IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 ,IMD.Sale_Rate ,IMD.Basic_Rate ,Cast(CID.Item_Desc_M As VarChar(300)) ,  \
            GM11.Group_Name ,  GM21.Group_Name ,  GM31.Group_Name ,  GM41.Group_Name ,  GM81.Group_Name ,  GM111.Group_Name ,  GM121.Group_Name ,  GM161.Group_Name , \
            GM201.Group_Name , GM11.Group_Code , GM21.Group_Code , GM31.Group_Code , GM41.Group_Code , GM81.Group_Code , GM111.Group_Code , GM121.Group_Code , \
            GM161.Group_Code , GM201.Group_Code ,  CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)\
            + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  \
            CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  CAST(GM201.Group_Code As VarChar) ,IMH.Item_Hd_Code , \
            IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , IMD.User_Code \
UNION ALL  \
SELECT\
            '' As Godown_Name, 0 As Godown_Code,'' As Godown_Grp_Name, 0 As Godown_Grp_Code, '' AS Branch_Group_Name, 0 AS Branch_Group_Code , BM.Branch_Name, \
            BM.Branch_Code, BM.Branch_Code As BranchCode,BM.Order_ As B_Order , '' As Prod_Unit, '' As Lot_Number1, '' as Lot_Code, '' as Last_Purchase_Date , \
            0 As Pack_Code , '' As Pack_Name, 0 As Order_ , IMH.Item_Hd_Code , IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , \
            0 As ItemGroup_Code, '' As Group_Name , 0 As Color_Code, '' As Color_Name, '' As Color_Short_Name  , '' As User_Code, 0 As Item_Det_Code , \
            IMD.User_Code As Item_Code , (CAST(IMH.Item_Hd_Code AS VARCHAR) + '|' +  CAST(IMN.Item_Name_Code AS VARCHAR) + '|' +  CAST(CM.Comp_Code AS VARCHAR) + '|' +  \
            CAST(IMD.User_Code AS VARCHAR) ) As ItemChangeCode , PM.Link_Code ,IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 , 0 As ConsRate , \
            IMD.Sale_Rate   As Rate , IMD.Basic_Rate  As Rate1 ,Cast(CID.Item_Desc_M As VarChar(300)) As Item_Desc ,  GM11.Group_Name As GroupName11, '1|1' As GroupLevel11,\
            GM21.Group_Name As GroupName21, '2|1' As GroupLevel21,  GM31.Group_Name As GroupName31, '3|1' As GroupLevel31,  GM41.Group_Name As GroupName41, \
            '4|1' As GroupLevel41,  GM81.Group_Name As GroupName81, '8|1' As GroupLevel81,  GM111.Group_Name As GroupName111, '11|1' As GroupLevel111,  \
            GM121.Group_Name As GroupName121, '12|1' As GroupLevel121,  GM161.Group_Name As GroupName161, '16|1' As GroupLevel161,  GM201.Group_Name As GroupName201, \
            '20|1' As GroupLevel201, GM11.Group_Code As GroupCode11, GM21.Group_Code As GroupCode21, GM31.Group_Code As GroupCode31, GM41.Group_Code As GroupCode41, \
            GM81.Group_Code As GroupCode81, GM111.Group_Code As GroupCode111, GM121.Group_Code As GroupCode121, GM161.Group_Code As GroupCode161, \
            GM201.Group_Code As GroupCode201,  CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)  \
            + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  \
            CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  CAST(GM201.Group_Code As VarChar)  As GroupChangeCode , 0 AS SlQtyCF, \
            0 AS SrQtyCF, 0 As SlSTQtyCF, 0 As SrSTQtyCF,0 as SlFreeQty,0 as SlSTFreeQty,0 as SRFreeQty ,0 as Godown_STI ,0 as Godown_STO , 0 As SlQtyCF_2 ,0 As SrQtyCF_2,\
            0 As SlSTQtyCF_2 , 0 As SrSTQtyCF_2 , 0 AS SlVal, 0 AS SrVal,0 AS SlSTVal, 0 AS SrSTVal , 0 AS RecVal, 0 as IssueVal , 0 AS PuQtyCF,0 as PuFreeQtyCF, \
            0 AS PrQtyCF,0 AS PuQtyCF_2,0 AS PrQtyCF_2 , 0 AS PuSTQtyCF,0 as PuSTFreeQtyCF, 0 AS PrSTQtyCF,0 AS PuSTQtyCF_2,0 AS PrSTQtyCF_2 , 0 as PuVal, 0 as PrVal ,\
                       0 AS PuSTVal , 0 AS PrSTVal , 0 AS RecQty, 0 as IssueQty , SUM(TXN.Net_Qty/1) As NetQtyCF , SUM(TXN.Net_Qty/1) As NetQtyCF_2 , \
            SUM(TXN.Net_Qty * IMD.Pur_Rate) AS NetQtyVal , SUM(TXN.Net_Qty * IMD.sale_rate) AS NetQtyVal2 , '' As TempDate,0 As PartyGrpCode, '' As PeriodFrom, 0 As BillCount, \
            0 As PartyCount, 'CL' As ValueType  , 0 As TotalItems  \
FROM \
            Godown_Mst AS GDM with (NOLOCK) ,It_Mst_O_Det AS IMOD with (NOLOCK), Branch_Mst AS BM with (NOLOCK), Branch_Groups BG with (NOLOCK),  \
            It_Mst_Hd As IMH, It_Mst_Det As IMD, Comp_Mst As CM , Item_Color_Mst As ICM, Group_Mst As GM, Pack_Mst As PM, It_Mst_Names As IMN , \
            Lot_Mst as LM with (nolock) , Group_Mst As Gm11, Group_Mst As Gm21, Group_Mst As Gm31, Group_Mst As Gm41, Group_Mst As Gm81, Group_Mst As Gm111, \
            Group_Mst As Gm121, Group_Mst As Gm161, Group_Mst As Gm201, Comm_It_Desc As CID with (NOLOCK) , stk_dtxn20182019 AS TXN with (NOLOCK)  \
WHERE        TXN.Lot_Code = LM.Lot_Code And TXN.Godown_Code = GDM.Godown_Code  AND TXN.Branch_Code = BM.Branch_Code And IMD.Item_O_Det_Code = IMOD.Code  AND\
            BG.Group_Code = BM.Group_Code1 AND  IMH.Item_Hd_Code = IMD.Item_Hd_Code AND IMH.Comp_Code = CM.Comp_Code  AND IMH.Color_Code = ICM.Color_Code AND \
            IMH.Group_Code = GM.Group_Code  AND IMD.Pack_Code = PM.Pack_Code AND IMH.Item_Name_Code = IMN.Item_Name_Code  AND IMD.Item_Det_Code = LM.Item_Det_Code  \
            AND IMH.Group_Code= GM11.Group_Code  AND IMH.Group_Code2= GM21.Group_Code  AND IMH.Group_Code3= GM31.Group_Code  AND IMH.Group_Code4= GM41.Group_Code  \
            AND IMH.Group_Code8= GM81.Group_Code  AND IMH.Group_Code11= GM111.Group_Code  AND IMH.Group_Code_12 = GM121.Group_Code  AND IMH.Group_Code16= GM161.Group_Code \
            AND IMH.Group_Code20= GM201.Group_Code  AND (Not LM.Puv_Code Like 'CS%') AND \
             IMD.Item_Det_Code IN(11626,11874,11928,11929,11930,11931,11932,11933,11934,11935,11936,11937,11938,11939,11962,12069,12245,12246,12339,12357,12566,12567,13002,13003,13004,42015,59640,59641,59642,59067,59068,59069,59070,59071,59072,59073,59074,59075,59076,59077,59078,59079,59080,65652,65653,65654,65655,65656,65657,65658,65659,65660,65661,44850,64488,45259,45263,53404,53409,52634,38424,38426,56509,56510,56511,56375,56378,56374,56377,56376,56379,52185,52188,60881,59240,52186,52189,59241,52187,52190,59237,59234,59238,59235,59239,59236,56505,56508,56381,56384,56503,56506,56380,56383,56504,56507,56382,56385,61460,59250,59252,59249,59251,59243,59246,59245,59248,59244,59247,61464,46609,38427,61467,44789,44790,47569,42913,51337,61465,38431,46613,46610,38428,61468,51341,44791,44793,47570,42914,51338,61466,61469,44792,44794,47571,51339,61471,61470,61472,52181,52184,52180,59242,56369,56372,56370,56373,56368,56371,56512,46615,44787,42919,56513,46616,44788,47573,42920,56514,44795,42921,50125,51296,51297,51298,51299,45575,38631,69154,59294,59300,59295,59301,59296,59305,59297,69156,60332,60333,60334,69157,47262,47286,47287,47288,47265,47289,47267,47291,47292,47269,47281,47293,47294,47295,47272,47296,47297,39082,59666,64484,64779,55399,64780,64787,64786,64781,64788,64782,69140,57513,42705,42707,42709,51478,51479,51481,48197,48239,59603,59604,51555,51557,51558,53109,52904,52906,52907,52908,54824,54825,54829,56812,57194,57195,57196,57197,57200,56766,56767,56768,56769,56770,56771,56772,56646,56647,57289,57290,57291,57292,57293,57294,57295,57296,56780,56781,56782,56783,56784,59738,59758,59739,59759,59740,59760,59741,59761,59742,59762,59743,59763,59767,59768,63306,64069,64070,64071,64072,62438,63297,63298,63299,63300,63301,62261,62262,62263,62264,62265,62266,62267,62268,62269,62270,63809,63810,63811,63812,63813,62806,62807,62808,62809,62810,62811,62840,62841,62842,62689,62688,62690,69158,66272,66273,66274,66275,67576,67599,67600,67609,67828,67829,67830,67831,67832,67833,67834,67835,67836,67837,67838,67839,67840,67841,67842,67843,67844,67845,67846,67674,67701,67810,68991,68992,68874,68743,43163,50227,50228,41561,52821,57462,57464,57465,54904,57466,52825,57467,52826,54905,46685,46686,47546,60777,60776,60771,60772,64493,60779,60778,60785,60786,60787,60784,60782,60781,60780,60783,60774,60775,60773,64497,64496,64498,66968,66969,66967,59693,66059,66058,60877,62635,62636,62634,62633,66057,66056,60876,62638,62637,62639,62640,41547,41546,39247,58399,58398,57447,57468,59367,64489,64490,43168,45372,58372,52077,52078,52079,52080,52081,52082,52083,51300,51301,51302,51303,51304,58245,58246,57453,57454,57452,57448,57449,57511,57512,58175,58174,53516,53517,53518,53519,65684,57451,58177,57450,65685,59805,32007,31828,34890,34899,35952,32945,45366,45367,59368,61748,69270,69272,69271,69273,69275,69274,69282,69281,69284,69283,69280,69279,61874,61875,61876,61877,66758,69255,69254,66760,66759,69257,69256,66761,69267,69268,69266,69269,66702,66706,61868,61869,61870,61871,61872,61873,61878,61879,61880,61881,61882,66704,66703,66707,66705,64775,64776,64784,64783,64777,64785,64778,32008,58370,58369,46668,45192,40642,40643,40641,55029,55030,46622,55008,50133,59448,59447,59446,56307,59445,52656,59444,59443,59442,59441,53911,53909,53899,55028,55007,56339,56340,58311,58289,59440,59439,59449,59160,59180,59181,59186,59182,59183,59184,59185,59159,60792,60720,60719,60791,61440,61439,61438,39192,51324,51325,67128,67127,67129,67132,67131,45381,45382,58251,59365,59366,58250,37307,48502,38054,36555,43167,64160,64161,64275,64276,64277,64274,55640,58048,45368,26289,58697,58479,42514,58698,26291,58699,62973,63181,63182,63183,63184,45369,69464,58371,58256,58257,60878,58480,58373,29959,58243,58260,57622,58244,58261,58242,58259,58241,58258,64162,45365,39191,47544,58254,59371,58255,59372,58252,59369,58253,59370,34196,34198,32951,32952,33039,33045,33048,33049,39195,51953,41558,35951,58247,58249,58262,58248,65669,38310,60607,31870,43268,32683,33611,31576,35648,35705,67017,67320,67015,67009,67016,67319,67013,67010,67011,67012,67020,67014,67008,67018,67019,67021,35953,48896,42134,47053,48895,42128,48897,42130,57910,45273,57919,60673,42148,42129,57918,56993,60672,42146,42145,42147,56994,47055,57916,56992,45371,64491,64492,34194,61687,61688,61686,60879,62632,62631,62630,62629,62624,62623,62622,62621,62628,62627,62626,62625)  AND GDM.Godown_Code IN (0) AND TXN.Branch_Code IN (5033,5013,5038,5031,5028,5025,5016,5015,5024,5043,5021,5017,5032,5008,5007,5009,5045,5027,5037,5000,5022,5044,5039,5047,5034,5018,5006,5011,5012,5023,5041,5042,5036,5046,5019,5030,5026,5029,5010,5049,5035,5040,5020,5048,5002,5003,5005,5004,5001)\
            AND CID.Code = IMH.Comm_It_Desc_Code  AND LM.Exp_dmg='N' AND TXN.date_ between CONVERT(smalldatetime,('31/03/2018'),103)  AND \
            CONVERT(smalldatetime,('20/08/2018'),103)  GROUP BY  BM.Branch_Name, BM.Branch_Code,BM.Order_, PM.Link_Code,IMD.Sale_Rate, \
            IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 ,IMD.Sale_Rate ,IMD.Basic_Rate ,Cast(CID.Item_Desc_M As VarChar(300)) ,  \
            GM11.Group_Name ,  GM21.Group_Name ,  GM31.Group_Name ,  GM41.Group_Name ,  GM81.Group_Name ,  GM111.Group_Name ,  GM121.Group_Name ,  GM161.Group_Name ,  \
            GM201.Group_Name , GM11.Group_Code , GM21.Group_Code , GM31.Group_Code , GM41.Group_Code , GM81.Group_Code , GM111.Group_Code , GM121.Group_Code , \
            GM161.Group_Code , GM201.Group_Code ,  CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)  \
            + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  \
            CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  CAST(GM201.Group_Code As VarChar) ,IMH.Item_Hd_Code , \
            IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , IMD.User_Code UNION ALL  SELECT '' As Godown_Name, 0 As Godown_Code,\
            '' As Godown_Grp_Name, 0 As Godown_Grp_Code, '' AS Branch_Group_Name, 0 AS Branch_Group_Code , BM.Branch_Name, BM.Branch_Code, BM.Branch_Code As BranchCode,\
            BM.Order_ As B_Order , '' As Prod_Unit, '' As Lot_Number1, '' as Lot_Code, '' as Last_Purchase_Date , 0 As Pack_Code , '' As Pack_Name, 0 As Order_ , \
            IMH.Item_Hd_Code , IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , 0 As ItemGroup_Code, '' As Group_Name , 0 As Color_Code, \
            '' As Color_Name, '' As Color_Short_Name  , '' As User_Code, 0 As Item_Det_Code , IMD.User_Code As Item_Code , (CAST(IMH.Item_Hd_Code AS VARCHAR) + '|' +  \
            CAST(IMN.Item_Name_Code AS VARCHAR) + '|' +  CAST(CM.Comp_Code AS VARCHAR) + '|' +  CAST(IMD.User_Code AS VARCHAR) ) As ItemChangeCode , PM.Link_Code ,\
            IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 , 0 As ConsRate , IMD.Sale_Rate   As Rate , IMD.Basic_Rate  As Rate1 ,\
            Cast(CID.Item_Desc_M As VarChar(300)) As Item_Desc ,  GM11.Group_Name As GroupName11, '1|1' As GroupLevel11,  GM21.Group_Name As GroupName21, '2|1' As GroupLevel21,\
             GM31.Group_Name As GroupName31, '3|1' As GroupLevel31,  GM41.Group_Name As GroupName41, '4|1' As GroupLevel41,  GM81.Group_Name As GroupName81, \
             '8|1' As GroupLevel81,  GM111.Group_Name As GroupName111, '11|1' As GroupLevel111,  GM121.Group_Name As GroupName121, '12|1' As GroupLevel121,  \
             GM161.Group_Name As GroupName161, '16|1' As GroupLevel161,  GM201.Group_Name As GroupName201, '20|1' As GroupLevel201, GM11.Group_Code As GroupCode11,\
             GM21.Group_Code As GroupCode21, GM31.Group_Code As GroupCode31, GM41.Group_Code As GroupCode41, GM81.Group_Code As GroupCode81, GM111.Group_Code As GroupCode111,\
             GM121.Group_Code As GroupCode121, GM161.Group_Code As GroupCode161, GM201.Group_Code As GroupCode201,  CAST(GM11.Group_Code As VarChar)  + '|' +  \
             CAST(GM21.Group_Code As VarChar)  + '|' +  CAST(GM31.Group_Code As VarChar)  + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  \
             + '|' +  CAST(GM111.Group_Code As VarChar)  + '|' +  CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  \
             CAST(GM201.Group_Code As VarChar)  As GroupChangeCode , SUM((CASE WHEN TXN.Sale_Or_Sr='SL' THEN  Txn.Tot_Qty  ELSE 0 END)/1) As SlQtyCF , \
             SUM((CASE WHEN TXN.Sale_Or_Sr='SR' THEN  Txn.Tot_Qty  ELSE 0 END)/1) AS SrQtyCF, 0 As SlSTQtyCF, 0 As SrSTQtyCF , \
             SUM((CASE WHEN TXN.Sale_Or_Sr='SL' THEN  Txn.Free_Qty + Txn.Repl_Qty+ Txn.Sample_Qty  ELSE 0 END)/1) As SlFreeQty  , 0 As SlSTFreeQty, \
             SUM((CASE WHEN TXN.Sale_Or_Sr='SR' THEN  Txn.Free_Qty + Txn.Repl_Qty+ Txn.Sample_Qty  ELSE 0 END)/1) As SRFreeQty  ,0 as Godown_STI,0 as Godown_STO  , \
             SUM((CASE WHEN TXN.Sale_Or_Sr='SL' THEN  Txn.Tot_Qty  ELSE 0 END)/1) As SlQtyCF_2 , \
             SUM((CASE WHEN TXN.Sale_Or_Sr='SR' THEN  Txn.Tot_Qty  ELSE 0 END)/1) AS SrQtyCF_2, 0 As SlSTQtyCF_2, 0 As SrSTQtyCF_2 , \
             SUM(CASE WHEN   TXN.Sale_Or_Sr='SL' THEN TXN.Calc_Net_Amt ELSE 0 END) AS SLVal , \
             SUM(CASE WHEN   TXN.Sale_Or_Sr='SR' THEN TXN.Calc_Net_Amt ELSE 0 END) AS SrVal ,0 AS SlSTVal, 0 AS SrSTVal , 0 AS RecVal, 0 as IssueVal , 0 AS PuQtyCF,\
             0 as PuFreeQtyCF, 0 AS PrQtyCF,0 AS PuQtyCF_2,0 AS PrQtyCF_2 , 0 AS PuSTQtyCF,0 as PuSTFreeQtyCF, 0 AS PrSTQtyCF,0 AS PuSTQtyCF_2,0 AS PrSTQtyCF_2 , \
             0 AS PuVal , 0 AS PrVal , 0 AS PuSTVal , 0 AS PrSTVal , 0 AS RecQty, 0 as IssueQty  ,0 As NetQtyCF  ,0 As NetQtyCF_2 ,0 As NetQtyVal, 0 AS NetQtyVal2 , \
             '' As TempDate ,0 As PartyGrpCode, '' As PeriodFrom, 0 As BillCount, 0 As PartyCount,'SALE' As ValueType , 0 As TotalItems  FROM Godown_Mst AS GDM with (NOLOCK) ,\
             It_Mst_O_Det AS IMOD with (NOLOCK), Branch_Mst AS BM with (NOLOCK), Branch_Groups BG with (NOLOCK),  It_Mst_Hd As IMH, It_Mst_Det As IMD, Comp_Mst As CM , \
             Item_Color_Mst As ICM, Group_Mst As GM, Pack_Mst As PM, It_Mst_Names As IMN , Lot_Mst as LM with (nolock) , Group_Mst As Gm11, Group_Mst As Gm21, \
             Group_Mst As Gm31, Group_Mst As Gm41, Group_Mst As Gm81, Group_Mst As Gm111, Group_Mst As Gm121, Group_Mst As Gm161, Group_Mst As Gm201, \
             Comm_It_Desc As CID with (NOLOCK) , sl_head20182019 As HD WITH (NOLOCK), sl_txn20182019 As Txn  WITH (NOLOCK)  \
              WHERE\
            TXN.Lot_Code = LM.Lot_Code And TXN.Godown_Code = GDM.Godown_Code  AND HD.Branch_Code = BM.Branch_Code And IMD.Item_O_Det_Code = IMOD.Code  AND \
            BG.Group_Code = BM.Group_Code1 AND  IMH.Item_Hd_Code = IMD.Item_Hd_Code AND IMH.Comp_Code = CM.Comp_Code  AND IMH.Color_Code = ICM.Color_Code AND\
            IMH.Group_Code = GM.Group_Code  AND IMD.Pack_Code = PM.Pack_Code AND IMH.Item_Name_Code = IMN.Item_Name_Code  AND IMD.Item_Det_Code = LM.Item_Det_Code  AND \
            IMH.Group_Code= GM11.Group_Code  AND IMH.Group_Code2= GM21.Group_Code  AND IMH.Group_Code3= GM31.Group_Code  AND IMH.Group_Code4= GM41.Group_Code  AND \
            IMH.Group_Code8= GM81.Group_Code  AND IMH.Group_Code11= GM111.Group_Code  AND IMH.Group_Code_12 = GM121.Group_Code  AND IMH.Group_Code16= GM161.Group_Code  AND \
            IMH.Group_Code20= GM201.Group_Code  AND (Not LM.Puv_Code Like 'CS%') AND \
            IMD.Item_Det_Code IN(11626,11874,11928,11929,11930,11931,11932,11933,11934,11935,11936,11937,11938,11939,11962,12069,12245,12246,12339,12357,12566,12567,13002,13003,13004,42015,59640,59641,59642,59067,59068,59069,59070,59071,59072,59073,59074,59075,59076,59077,59078,59079,59080,65652,65653,65654,65655,65656,65657,65658,65659,65660,65661,44850,64488,45259,45263,53404,53409,52634,38424,38426,56509,56510,56511,56375,56378,56374,56377,56376,56379,52185,52188,60881,59240,52186,52189,59241,52187,52190,59237,59234,59238,59235,59239,59236,56505,56508,56381,56384,56503,56506,56380,56383,56504,56507,56382,56385,61460,59250,59252,59249,59251,59243,59246,59245,59248,59244,59247,61464,46609,38427,61467,44789,44790,47569,42913,51337,61465,38431,46613,46610,38428,61468,51341,44791,44793,47570,42914,51338,61466,61469,44792,44794,47571,51339,61471,61470,61472,52181,52184,52180,59242,56369,56372,56370,56373,56368,56371,56512,46615,44787,42919,56513,46616,44788,47573,42920,56514,44795,42921,50125,51296,51297,51298,51299,45575,38631,69154,59294,59300,59295,59301,59296,59305,59297,69156,60332,60333,60334,69157,47262,47286,47287,47288,47265,47289,47267,47291,47292,47269,47281,47293,47294,47295,47272,47296,47297,39082,59666,64484,64779,55399,64780,64787,64786,64781,64788,64782,69140,57513,42705,42707,42709,51478,51479,51481,48197,48239,59603,59604,51555,51557,51558,53109,52904,52906,52907,52908,54824,54825,54829,56812,57194,57195,57196,57197,57200,56766,56767,56768,56769,56770,56771,56772,56646,56647,57289,57290,57291,57292,57293,57294,57295,57296,56780,56781,56782,56783,56784,59738,59758,59739,59759,59740,59760,59741,59761,59742,59762,59743,59763,59767,59768,63306,64069,64070,64071,64072,62438,63297,63298,63299,63300,63301,62261,62262,62263,62264,62265,62266,62267,62268,62269,62270,63809,63810,63811,63812,63813,62806,62807,62808,62809,62810,62811,62840,62841,62842,62689,62688,62690,69158,66272,66273,66274,66275,67576,67599,67600,67609,67828,67829,67830,67831,67832,67833,67834,67835,67836,67837,67838,67839,67840,67841,67842,67843,67844,67845,67846,67674,67701,67810,68991,68992,68874,68743,43163,50227,50228,41561,52821,57462,57464,57465,54904,57466,52825,57467,52826,54905,46685,46686,47546,60777,60776,60771,60772,64493,60779,60778,60785,60786,60787,60784,60782,60781,60780,60783,60774,60775,60773,64497,64496,64498,66968,66969,66967,59693,66059,66058,60877,62635,62636,62634,62633,66057,66056,60876,62638,62637,62639,62640,41547,41546,39247,58399,58398,57447,57468,59367,64489,64490,43168,45372,58372,52077,52078,52079,52080,52081,52082,52083,51300,51301,51302,51303,51304,58245,58246,57453,57454,57452,57448,57449,57511,57512,58175,58174,53516,53517,53518,53519,65684,57451,58177,57450,65685,59805,32007,31828,34890,34899,35952,32945,45366,45367,59368,61748,69270,69272,69271,69273,69275,69274,69282,69281,69284,69283,69280,69279,61874,61875,61876,61877,66758,69255,69254,66760,66759,69257,69256,66761,69267,69268,69266,69269,66702,66706,61868,61869,61870,61871,61872,61873,61878,61879,61880,61881,61882,66704,66703,66707,66705,64775,64776,64784,64783,64777,64785,64778,32008,58370,58369,46668,45192,40642,40643,40641,55029,55030,46622,55008,50133,59448,59447,59446,56307,59445,52656,59444,59443,59442,59441,53911,53909,53899,55028,55007,56339,56340,58311,58289,59440,59439,59449,59160,59180,59181,59186,59182,59183,59184,59185,59159,60792,60720,60719,60791,61440,61439,61438,39192,51324,51325,67128,67127,67129,67132,67131,45381,45382,58251,59365,59366,58250,37307,48502,38054,36555,43167,64160,64161,64275,64276,64277,64274,55640,58048,45368,26289,58697,58479,42514,58698,26291,58699,62973,63181,63182,63183,63184,45369,69464,58371,58256,58257,60878,58480,58373,29959,58243,58260,57622,58244,58261,58242,58259,58241,58258,64162,45365,39191,47544,58254,59371,58255,59372,58252,59369,58253,59370,34196,34198,32951,32952,33039,33045,33048,33049,39195,51953,41558,35951,58247,58249,58262,58248,65669,38310,60607,31870,43268,32683,33611,31576,35648,35705,67017,67320,67015,67009,67016,67319,67013,67010,67011,67012,67020,67014,67008,67018,67019,67021,35953,48896,42134,47053,48895,42128,48897,42130,57910,45273,57919,60673,42148,42129,57918,56993,60672,42146,42145,42147,56994,47055,57916,56992,45371,64491,64492,34194,61687,61688,61686,60879,62632,62631,62630,62629,62624,62623,62622,62621,62628,62627,62626,62625)  AND GDM.Godown_Code IN (0) AND HD.Branch_Code IN (5033,5013,5038,5031,5028,5025,5016,5015,5024,5043,5021,5017,5032,5008,5007,5009,5045,5027,5037,5000,5022,5044,5039,5047,5034,5018,5006,5011,5012,5023,5041,5042,5036,5046,5019,5030,5026,5029,5010,5049,5035,5040,5020,5048,5002,5003,5005,5004,5001) \
            AND HD.Stock_Trans=0  AND CID.Code = IMH.Comm_It_Desc_Code  AND Txn.Vouch_Code=HD.Vouch_Code   AND Txn.Item_Det_Code = IMD.Item_Det_Code AND \
            HD.Deleted=0  AND HD.Vouch_Date Between CONVERT(smalldatetime,('01/08/2018'),103)  AND CONVERT(smalldatetime,('20/08/2018'),103) \
GROUP BY\
            BM.Branch_Name, BM.Branch_Code,BM.Order_, PM.Link_Code,IMD.Sale_Rate, IMD.Pur_Rate,IMD.Basic_Rate,IMD.Mrp , IMD.Cf_1 ,IMD.Sale_Rate ,IMD.Basic_Rate ,\
            Cast(CID.Item_Desc_M As VarChar(300)) ,  GM11.Group_Name ,  GM21.Group_Name ,  GM31.Group_Name ,  GM41.Group_Name ,  GM81.Group_Name ,  GM111.Group_Name ,  \
            GM121.Group_Name ,  GM161.Group_Name ,  GM201.Group_Name , GM11.Group_Code , GM21.Group_Code , GM31.Group_Code , GM41.Group_Code , GM81.Group_Code , \
            GM111.Group_Code , GM121.Group_Code , GM161.Group_Code , GM201.Group_Code ,  CAST(GM11.Group_Code As VarChar)  + '|' +  CAST(GM21.Group_Code As VarChar)  \
            + '|' +  CAST(GM31.Group_Code As VarChar)  + '|' +  CAST(GM41.Group_Code As VarChar)  + '|' +  CAST(GM81.Group_Code As VarChar)  + '|' +  \
            CAST(GM111.Group_Code As VarChar)  + '|' +  CAST(GM121.Group_Code As VarChar)  + '|' +  CAST(GM161.Group_Code As VarChar)  + '|' +  \
            CAST(GM201.Group_Code As VarChar) ,IMH.Item_Hd_Code , IMH.Item_Hd_Name , IMN.Item_Name_Code , IMN.Item_Name , CM.Comp_Code, CM.Comp_Name , IMD.User_Code \
             ) as tb"
    print(Table1)
    GDM = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    print(GDM.count())
   
    #GDM.show(1,False)
    GDM.cache()
    GDM.write.jdbc(url=Postgresurl, table="market99"+".data", mode="overwrite", properties=Postgresprop)
    print("SUCCESS")
    
        
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
   
    
    
#     
# Sqlurl="Url:jdbc:sqlserver://127.0.0.1;databasename=<logic;integratedsecurity=true"
# 
#Table1="(Select * FROM Godown_Mst) as calendar1"
#print(Table1)
#df = sqlctx.read.format("jdbc").options(url=Sqlurl, dbtable=Table1,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
