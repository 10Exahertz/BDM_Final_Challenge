#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  9 12:32:35 2020

@author: stevenalsheimer
"""

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lower, col, when, split, regexp_replace
from pyspark.sql.functions import lit, size


def main(sc):
    V_file = 'hdfs:///data/share/bdm/nyc_parking_violations/2019.csv'
    dfviol = spark.read.load(V_file, format='csv',
                             header = True,
                             inferSchema = True)
    
    dfviol = dfviol.select(dfviol['House Number'].alias('House_Num'),
                           dfviol['Street Name'].alias('Street_Name'),
                           dfviol['Violation County'].alias('Violation_County'))
    dfviol = dfviol.na.fill(0)
    dfviol = dfviol.withColumn('Street_Name', lower(col('Street_Name')))
    dfviol = dfviol.withColumn('V_county', when(col("Violation_County")=="K", 3)
                               .when(col("Violation_County")=="BK", 3)
                               .when(col("Violation_County")=="KINGS", 3)
                               .when(col("Violation_County")=="KING", 3)
                               .when(col("Violation_County")=="Q", 4)
                               .when(col("Violation_County")=="QN", 4)
                               .when(col("Violation_County")=="QNS", 4)
                               .when(col("Violation_County")=="QU", 4)
                               .when(col("Violation_County")=="QUEEN", 4)
                               .when(col("Violation_County")=="BX", 2)
                               .when(col("Violation_County")=="BRONX", 2)
                               .when(col("Violation_County")=="PBX", 2)
                               .when(col("Violation_County")=="M", 1)
                               .when(col("Violation_County")=="MAN", 1)
                               .when(col("Violation_County")=="MH", 1)
                               .when(col("Violation_County")=="MN", 1)
                               .when(col("Violation_County")=="NEWY", 1)
                               .when(col("Violation_County")=="NEW Y", 1)
                               .when(col("Violation_County")=="NY", 1)
                               .when(col("Violation_County")=="S", 5)
                               .when(col("Violation_County")=="R", 5)
                               .when(col("Violation_County")=="RICHMOND", 5)
                               .otherwise(0))
    dfviol = dfviol.withColumn('House_Num',regexp_replace('House_Num','-0*','-'))
    split_col = split(col('House_Num'),'-').cast('array<int>')
    dfviol = dfviol.withColumn('House_Num',split_col)
    
    C_file = 'hdfs:///data/share/bdm/nyc_cscl.csv'

    dfcent = spark.read.load(C_file, format='csv',
                             header = True,
                             inferSchema = True)

    dfcent = dfcent.select('PHYSICALID',
                           'ST_LABEL',
                           'FULL_STREE',
                           'L_LOW_HN',
                           'L_HIGH_HN',
                           'R_LOW_HN',
                           'R_HIGH_HN',
                           'BOROCODE')
                      
    dfcent = dfcent.withColumn('ST_LABEL', lower(col('ST_LABEL')))
    dfcent = dfcent.withColumn('FULL_STREE', lower(col('FULL_STREE')))
    dfcent = dfcent.na.fill('0')
    dfcent = dfcent.withColumn('L_LOW_HN',regexp_replace('L_LOW_HN','-0*','-'))
    dfcent = dfcent.withColumn('L_HIGH_HN',regexp_replace('L_HIGH_HN','-0*','-'))
    dfcent = dfcent.withColumn('R_LOW_HN',regexp_replace('R_LOW_HN','-0*','-'))
    dfcent = dfcent.withColumn('R_HIGH_HN',regexp_replace('R_HIGH_HN','-0*','-'))
    split_col = split(col('L_LOW_HN'),'-').cast('array<int>')
    dfcent = dfcent.withColumn('L_LOW_HN',split_col)
    split_col = split(col('L_HIGH_HN'),'-').cast('array<int>')
    dfcent = dfcent.withColumn('L_HIGH_HN',split_col)
    split_col = split(col('R_HIGH_HN'),'-').cast('array<int>')
    dfcent = dfcent.withColumn('R_HIGH_HN',split_col)
    split_col = split(col('R_LOW_HN'),'-').cast('array<int>')
    dfcent = dfcent.withColumn('R_LOW_HN',split_col)

    joinCondition = (when(size(dfviol.House_Num)==2,(when(dfviol.House_Num[1]%2== 0, (dfcent.R_LOW_HN[0] <= dfviol.House_Num[0])&(dfcent.R_HIGH_HN[0] >= dfviol.House_Num[0])&(dfcent.R_LOW_HN[1] <= dfviol.House_Num[1])&(dfcent.R_HIGH_HN[1] >= dfviol.House_Num[1]))
                                               .otherwise((dfcent.L_LOW_HN[0] <= dfviol.House_Num[0])&(dfcent.L_HIGH_HN[0] >= dfviol.House_Num[0])&(dfcent.L_LOW_HN[1] <= dfviol.House_Num[1])&(dfcent.L_HIGH_HN[1] >= dfviol.House_Num[1]))))
    .otherwise(when(dfviol.House_Num[0]%2== 0, (dfcent.R_LOW_HN[0] <= dfviol.House_Num[0])&(dfcent.R_HIGH_HN[0] >= dfviol.House_Num[0]))
    .otherwise((dfcent.L_LOW_HN[0] <= dfviol.House_Num[0]) & (dfcent.L_HIGH_HN[0] >= dfviol.House_Num[0]))))

    '''how he wants it done, below. Talk to him about this set-up'''
    df_full = dfviol.join(dfcent,[dfviol.V_county == dfcent.BOROCODE, ((dfviol.Street_Name == dfcent.FULL_STREE)|(dfviol.Street_Name == dfcent.ST_LABEL)),joinCondition
                                  ], how='inner')
    df_full = df_full.select('PHYSICALID',
                             'Street_Name',
                             'Violation_County',
                             'House_Num')
    df_full = df_full.withColumn('2019',lit(1))
    df_full= df_full.groupby('PHYSICALID').sum('2019')
    df_full.repartition(4).write.csv('Output2019_test.csv')








if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc)
    main(sc)