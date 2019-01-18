from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType, DateType, StringType 
from pyspark import SQLContext

sqlContext = SQLContext(spark)

train_schema = StructType([
    StructField("searchid", IntegerType(), True),
    StructField("adid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("objecttype", IntegerType(), True),
    StructField("histctr", DecimalType(), True),
    StructField("isclick", IntegerType(), True),])

train_df = spark.read.csv("s3a://avito-ads/load/lo/*.gz",header=False,schema=train_schema)

searchInfo_schema = StructType([
    StructField("searchid", IntegerType(), True),
    StructField("searchdate", DateType(), True),
    StructField("ipid", IntegerType(), True),
    StructField("userid", IntegerType(), True),
    StructField("isuserloggedon", IntegerType(), True),
    StructField("searchquery", StringType(), True),
    StructField("locationid", IntegerType(), True),
    StructField("categoryid", IntegerType(), True),
    StructField("searchparams", StringType(), True),])

searchInfo_df = spark.read.csv("s3a://avito-ads/load/searchInfo/*.gz",header=False,schema=searchInfo_schema)

# Register temporary tables to be able to use sqlContext.sql

train_df.createOrReplaceTempView('train_temp')
searchInfo_df.createOrReplaceTempView('searchInfo_temp')





