# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 2 (SPARK Through PySpark(Python))


from pyspark.sql.functions import split
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, desc



REVIEWS_DF = sc.textFile("/FileStore/tables/review.csv").map(lambda L1: L1.split("::")).toDF()
BUSINESSES_DF = sc.textFile("/FileStore/tables/business.csv").map(lambda L2: L2.split("::")).toDF()

REVIEWS_DF = REVIEWS_DF.select(REVIEWS_DF._1.alias('Review_ID'), REVIEWS_DF._2.alias('User_ID'), REVIEWS_DF._3.alias('Business_ID'), REVIEWS_DF._4.cast("float").alias('Ratings'))

REVIEWS_DF = REVIEWS_DF.select('Business_ID', 'Ratings').groupBy('Business_ID').avg('Ratings').orderBy(desc('avg(Ratings)')).limit(10)

BUSINESSES_DF = BUSINESSES_DF.select(BUSINESSES_DF._1.alias('Business_ID'), BUSINESSES_DF._2.alias('Full_Address'), BUSINESSES_DF._3.alias('categories'))

output = BUSINESSES_DF.join(REVIEWS_DF, REVIEWS_DF.Business_ID == BUSINESSES_DF.Business_ID)

OUTPUT_DataFrame = output.select(REVIEWS_DF.Business_ID, 'Full_Address', 'categories', 'avg(Ratings)').distinct()

OUTPUT_DataFrame.show()


display(OUTPUT_DataFrame)


















