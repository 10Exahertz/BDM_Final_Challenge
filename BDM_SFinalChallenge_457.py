#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 17 15:22:24 2020

@author: stevenalsheimer
"""
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from collections import defaultdict
import sys
import pandas as pd
sc = SparkContext()
spark = SparkSession(sc)
C_file = 'Parking_Violations/Centerline.csv'

dfcent = spark.read.load(C_file, format='csv',
                      header = True,
                      inferSchema = True)
def dd():
    return defaultdict(list)
def ddd():
    return defaultdict(dd)
CSCL_T = defaultdict(ddd)
CSCL_T2 = defaultdict(ddd)
#CSCL = dfcent.select("*").toPandas()
#CSCL = CSCL.dropna(subset=['L_LOW_HN','R_LOW_HN','R_HIGH_HN','L_HIGH_HN',], axis=0)
#CSCL['ST_LABEL'] = CSCL['ST_LABEL'].map(lambda x: x.lower())
#CSCL['FULL_STREE'] = CSCL['FULL_STREE'].map(lambda x: x.lower() if pd.notnull(x) else x)
#CSCL['L_LOW_HN'] = CSCL['L_LOW_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
#CSCL[['L_LOW_HN','L_LOW_HN1']] = pd.DataFrame(CSCL['L_LOW_HN'].tolist(),index =CSCL.index)
#
#CSCL['L_HIGH_HN'] = CSCL['L_HIGH_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
#CSCL[['L_HIGH_HN','L_HIGH_HN1']] = pd.DataFrame(CSCL['L_HIGH_HN'].tolist(),index =CSCL.index)
#
#CSCL['R_LOW_HN'] = CSCL['R_LOW_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
#CSCL[['R_LOW_HN','R_LOW_HN1']] = pd.DataFrame(CSCL['R_LOW_HN'].tolist(),index =CSCL.index)
#
#CSCL['R_HIGH_HN'] = CSCL['R_HIGH_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
#CSCL[['R_HIGH_HN','R_HIGH_HN1']] = pd.DataFrame(CSCL['R_HIGH_HN'].tolist(),index =CSCL.index)
#    
#CSCL = CSCL[['BOROCODE','ST_LABEL','FULL_STREE','L_LOW_HN','L_HIGH_HN','R_LOW_HN','R_HIGH_HN','L_LOW_HN1','L_HIGH_HN1','R_LOW_HN1','R_HIGH_HN1','PHYSICALID']]
#print("Mem",CSCL.memory_usage(index=True, deep=True).sum())
#CSCL = CSCL.values.tolist()
def main():
    #C_file = '/data/share/bdm/nyc_cscl.csv'
    C_file = 'Parking_Violations/Centerline.csv'

    dfcent = spark.read.load(C_file, format='csv',
                          header = True,
                          inferSchema = True)
    CSCL = dfcent.select("*").toPandas()
    CSCL = CSCL.dropna(subset=['L_LOW_HN','R_LOW_HN','R_HIGH_HN','L_HIGH_HN',], axis=0)
    CSCL['ST_LABEL'] = CSCL['ST_LABEL'].map(lambda x: x.lower())
    CSCL['FULL_STREE'] = CSCL['FULL_STREE'].map(lambda x: x.lower() if pd.notnull(x) else x)
    CSCL['L_LOW_HN'] = CSCL['L_LOW_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
    CSCL[['L_LOW_HN','L_LOW_HN1']] = pd.DataFrame(CSCL['L_LOW_HN'].tolist(),index =CSCL.index)

    CSCL['L_HIGH_HN'] = CSCL['L_HIGH_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
    CSCL[['L_HIGH_HN','L_HIGH_HN1']] = pd.DataFrame(CSCL['L_HIGH_HN'].tolist(),index =CSCL.index)

    CSCL['R_LOW_HN'] = CSCL['R_LOW_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
    CSCL[['R_LOW_HN','R_LOW_HN1']] = pd.DataFrame(CSCL['R_LOW_HN'].tolist(),index =CSCL.index)

    CSCL['R_HIGH_HN'] = CSCL['R_HIGH_HN'].map(lambda x: tuple([int(i) for i in x.split('-')]))
    CSCL[['R_HIGH_HN','R_HIGH_HN1']] = pd.DataFrame(CSCL['R_HIGH_HN'].tolist(),index =CSCL.index)
    
    CSCL = CSCL[['BOROCODE','ST_LABEL','FULL_STREE','L_LOW_HN','L_HIGH_HN','R_LOW_HN','R_HIGH_HN','L_LOW_HN1','L_HIGH_HN1','R_LOW_HN1','R_HIGH_HN1','PHYSICALID']]
    print("Mem",CSCL.memory_usage(index=True, deep=True).sum())
    CSCL = CSCL.values.tolist()


#    for row in CSCL:
#        CSCL_T[row[0]][row[1]][row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]].append(row[11])
#    
#                   
#    for row in CSCL:
#        CSCL_T2[row[0]][row[2]][row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]].append(row[11])
#    CSCL_TB = sc.broadcast(CSCL_T)
#    CSCL_T2B = sc.broadcast(CSCL_T2)
    CSCL_B = sc.broadcast(CSCL)
    def GetPhys(boro):
        ind = CSCL.index[CSCL['BOROCODE']==boro].tolist()
        return CSCL.loc[ind,'PHYSICALID']
    def BoroT(boro):
        if boro == 'K':
            return 3
        if boro == 'BK':
            return 3
        if boro == 'KING':
            return 3
        if boro == 'KINGS':
            return 3
        if boro == 'Q':
            return 4
        if boro == 'QN':
            return 4
        if boro == 'QNS':
            return 4
        if boro == 'QU':
            return 4
        if boro == 'QUEENS':
            return 4
        if boro == 'BX':
            return 2
        if boro == 'BRONX':
            return 2
        if boro == 'R':
            return 5
        if boro == 'RICHMOND':
            return 5
        if boro == 'MAN':
            return 1
        if boro == 'MH':
            return 1
        if boro == 'MN':
            return 1
        if boro == 'NEWY':
            return 1
        if boro == 'NEW Y':
            return 1
        if boro == 'NY':
            return 1

    def processTrips(pid, records):
        import csv
        CSCL = CSCL_B.value


        for row in CSCL:
            CSCL_T[row[0]][row[1]][row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]].append(row[11])
    
                   
        for row in CSCL:
            CSCL_T2[row[0]][row[2]][row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]].append(row[11])
    
    # Skip the header
        if pid==0:
            next(records)
        reader = csv.reader(records)
        counts = {}
#        CSCL_T = CSCL_TB.value
#        CSCL_T2 = CSCL_T2B.value
    
        for row in reader:
            if row[21] == "Violation County":
                continue
            BoroV = BoroT(row[21])
            StreetV = row[24].lower()
            try:
                year = int(row[4].split('/')[2])
            except:
                continue
            House_Num = row[23]
            House_Num = House_Num.split('-')
            if House_Num == None or BoroV == None or StreetV == None:
                pass
            try:
                House_Num = tuple([int(i) if int(i) else 0 for i in House_Num])
            except:
                House_Num = (0,)
            ID=None
            if StreetV in CSCL_T[BoroV]:
                if len(House_Num)==1:
                    if House_Num[0]%2==0:
                        for key2, val2 in CSCL_T[BoroV][StreetV].items():
                            if key2[2]<=House_Num[0] and key2[3]>=House_Num[0]:
                                ID = val2[0]
                    else:
                        for key2, val2 in CSCL_T[BoroV][StreetV].items():
                            if key2[0]<=House_Num[0] and key2[1]>=House_Num[0]:
                                ID = val2[0]
                if len(House_Num)==2:
                    if House_Num[1]%2==0:
                        for key2, val2 in CSCL_T[BoroV][StreetV].items():
                            if key2[2]<=House_Num[0] and key2[3]>=House_Num[0] and key2[6]<=House_Num[1] and key2[7]>=House_Num[1]:
                                ID = val2[0]
                    else:
                        for key2, val2 in CSCL_T[BoroV][StreetV].items():
                            if key2[0]<=House_Num[0] and key2[1]>=House_Num[0] and key2[4]<=House_Num[1] and key2[5]>=House_Num[1]:
                                ID = val2[0]
            if StreetV in CSCL_T2[BoroV]:
                if len(House_Num)==1:
                    if House_Num[0]%2==0:
                        for key2, val2 in CSCL_T2[BoroV][StreetV].items():
                            if key2[2]<=House_Num[0] and key2[3]>=House_Num[0]:
                                ID = val2[0]
                    else:
                        for key2, val2 in CSCL_T2[BoroV][StreetV].items():
                            if key2[0]<=House_Num[0] and key2[1]>=House_Num[0]:
                                ID = val2[0]
                if len(House_Num)==2:
                    if House_Num[1]%2==0:
                        for key2, val2 in CSCL_T2[BoroV][StreetV].items():
                            if key2[2]<=House_Num[0] and key2[3]>=House_Num[0] and key2[6]<=House_Num[1] and key2[7]>=House_Num[1]:
                                ID = val2[0]
                    else:
                        for key2, val2 in CSCL_T2[BoroV][StreetV].items():
                            if key2[0]<=House_Num[0] and key2[1]>=House_Num[0] and key2[4]<=House_Num[1] and key2[5]>=House_Num[1]:
                                ID = val2[0]
            if ID != None:
                counts[(ID,year)] = counts.get((ID,year), 0) + 1
        return counts.items()
            
    #rdd = sc.textFile('/data/share/bdm/nyc_parking_violation/*.csv')
    rdd = sc.textFile('Parking_Violations/Parking_Violations_Issued_-_Fiscal_Year_2019_Small.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[0][0],(x[1],x[0][1]))) \
#               .collect()
# counts[:20]
    from pyspark.sql.functions import col, when, lit, array
    DF_C = counts.toDF(["PHYSID","YearCount"])
    DF_C = DF_C.withColumn("YearCount", array([col("YearCount").getField("_1"),col("YearCount").getField("_2")]))
    DF_C = DF_C.withColumn('2019',when(DF_C.YearCount[1]==2019,DF_C.YearCount[0]).otherwise(0))
    DF_C = DF_C.withColumn('2018',when(DF_C.YearCount[1]==2018,DF_C.YearCount[0]).otherwise(0))
    DF_C = DF_C.withColumn('2017',when(DF_C.YearCount[1]==2017,DF_C.YearCount[0]).otherwise(0))
    DF_C = DF_C.withColumn('2016',when(DF_C.YearCount[1]==2016,DF_C.YearCount[0]).otherwise(0))
    DF_C = DF_C.withColumn('2015',when(DF_C.YearCount[1]==2015,DF_C.YearCount[0]).otherwise(0))
    DF_C = DF_C.select(DF_C["PHYSID"],DF_C["2015"],DF_C["2016"],DF_C["2017"],DF_C["2018"],DF_C["2019"])
    from pyspark.sql.functions import size, col,split
    dfcent = dfcent.select('PHYSICALID')
    dfcent = dfcent.withColumn('2015',lit(0))
    dfcent = dfcent.withColumn('2016',lit(0))
    dfcent = dfcent.withColumn('2017',lit(0))
    dfcent = dfcent.withColumn('2018',lit(0))
    dfcent = dfcent.withColumn('2019',lit(0))
    DF_C = DF_C.union(dfcent)
    DF_C = DF_C.groupby('PHYSID').sum('2015','2016','2017','2018','2019')
    DF_C = DF_C.withColumn('OLS',(-2*(DF_C[1]-((DF_C[1]+DF_C[2]+DF_C[3]+DF_C[4]+DF_C[5])/5))-1*(DF_C[2]-((DF_C[1]+DF_C[2]+DF_C[3]+DF_C[4]+DF_C[5])/5))-0*(DF_C[3]-((DF_C[1]+DF_C[2]+DF_C[3]+DF_C[4]+DF_C[5])/5))+1*(DF_C[4]-((DF_C[1]+DF_C[2]+DF_C[3]+DF_C[4]+DF_C[5])/5))+2*(DF_C[5]-((DF_C[1]+DF_C[2]+DF_C[3]+DF_C[4]+DF_C[5])/5)))/(10))
    DF_C = DF_C.orderBy('PHYSID')

    DF_C.show()
    #DF_C.write.csv('Output2019_test.csv')

if __name__ == "__main__":
#    sc = SparkContext()
#    spark = SparkSession(sc)
    main()
