# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 2 (SPARK Through PySpark(Python))

from pyspark.sql.functions import split
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, desc



CONFIG = SparkConf().setMaster("local").setAppName("Business_Review")
# sc = SparkContext(conf=CONFIG)
SPARK_APP = SparkSession(sc)

# Loading the CSV Files
REVIEWS_DF = sc.textFile("/FileStore/tables/review.csv").map(lambda L1: L1.split("::")).toDF()
BUSINESSES_DF = sc.textFile("/FileStore/tables/business.csv").map(lambda L2: L2.split("::")).toDF()

BUSINESSES_DF = BUSINESSES_DF.select(BUSINESSES_DF._1.alias('Business_ID'), BUSINESSES_DF._2.alias('Full_Address'), BUSINESSES_DF._3.alias('Categories'))
REVIEWS_DF = REVIEWS_DF.select(REVIEWS_DF._1.alias('Review_ID'), REVIEWS_DF._2.alias('User_ID'), REVIEWS_DF._3.alias('Business_ID'), REVIEWS_DF._4.alias('Ratings'))

BUSIN_Stanford_DF = BUSINESSES_DF.filter(BUSINESSES_DF.Full_Address.contains('Stanford'))

OUTPUT_DF = REVIEWS_DF.join(BUSIN_Stanford_DF, REVIEWS_DF.Business_ID == BUSIN_Stanford_DF.Business_ID)
OUTPUT_DataFrame = OUTPUT_DF.select('User_ID','Ratings')
#OUTPUT_DataFrame.show()
output_FilePath = '/FileStore/tables/out_3.csv'

OUTPUT_DataFrame.repartition(1).write.csv(output_FilePath,header=True)

display(OUTPUT_DataFrame)