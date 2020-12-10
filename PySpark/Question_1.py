# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 2 (SPARK Through PySpark(Python))

from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession, Row


# Function to define the Common Friends Mappings List
def Common_Friends_Mapping(friends_info):
    SIZE = len(friends_info)
    OUTPUT = []
    IDX = 0
    while IDX < SIZE-1:
        CURR_USER = friends_info[IDX]
        FRIENDS_List = friends_info[IDX+1]
        if len(FRIENDS_List) > 0:
            FRIENDS_List = FRIENDS_List.split(",")
            for friend in FRIENDS_List:
                if int(CURR_USER) <= int(friend):
                    MAP_KEY = CURR_USER + ":" + friend
                else:
                    MAP_KEY = friend + ":" + CURR_USER
                OUTPUT.append((MAP_KEY, FRIENDS_List))
        IDX += 2

    return OUTPUT

SPARK_APP = SparkSession.builder.appName("Common_Friends_Count").getOrCreate()
# "/FileStore/tables/soc_LiveJournal1Adj.txt"

Friends_LINES = SPARK_APP.read.text("/FileStore/tables/soc_LiveJournal1Adj.txt").rdd.map(lambda F: F[0])
Friends_LINES = Friends_LINES.flatMap(lambda L: L.split("\t")).collect()
FRIENDS_LI = Common_Friends_Mapping(Friends_LINES)
FR_RDD = SPARK_APP.sparkContext.parallelize(FRIENDS_LI)
ALL_FR_RDD = FR_RDD.map(lambda F: (F[0], F[1]))

OUTPUT_Mutual = ALL_FR_RDD.reduceByKey(lambda F1, F2: len(set(F1).intersection(F2)))
OUTPUT_Mutual.coalesce(1).saveAsTextFile("/FileStore/tables/output_mutual1.txt")