# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 2 (SPARK Through PySpark(Python))



from pyspark.sql.functions import split
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf,array_contains, desc



BUSINESSES_DF = sc.textFile("/FileStore/tables/business.csv").map(lambda L2: L2.split("::")).toDF()

BUSINESSES_DF = BUSINESSES_DF.select(BUSINESSES_DF._1.alias('Business_ID'), BUSINESSES_DF._2.alias('Full_Address'), BUSINESSES_DF._3.alias('Categories'))
BUSINESSES_DF = BUSINESSES_DF.dropDuplicates()

# Splitting the CAtegories Names
Categ_User_Def_Func = udf(lambda Li: Li[5:-1].split(", "), ArrayType(StringType(), False))
BUSINESSES_DF = BUSINESSES_DF.withColumn("Categories", Categ_User_Def_Func(BUSINESSES_DF.Categories))
output = BUSINESSES_DF.rdd.flatMap(lambda Cat: [(C, 1) for C in Cat["Categories"] ] )
OUTPUT_DataFrame = output.reduceByKey(lambda C1,C2: C1 +C2).toDF()
OUTPUT_DataFrame.withColumnRenamed("_1", "Category").withColumnRenamed("_2", "Count").show()


display(OUTPUT_DataFrame)