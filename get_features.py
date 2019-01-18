from pyspark.sql.functions import datediff, to_date, lit
import pyspark.sql.functions as funcs
import datetime

train_schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("searchdate", DateType(), True),
    StructField("isclick", IntegerType(), True)])

train_df = spark.read.csv("s3a://avito-ads/output/output.csv/*.csv",header=False,schema=train_schema)

# Calculate recency (days since last visit) for each user
last_date = datetime.date(2015,5,21)
train_df = train_df.withColumn('recency',funcs.datediff(funcs.lit(last_date), train_df.searchdate))

last_seen_df = train_df.groupby('userid').agg(funcs.max('searchdate').alias('last_seen'))

train_df = train_df.join(last_seen_df, "userid", how='left')

# Calculate frequency (number of clicks) for each user
frequency_df = train_df.groupby('userid').agg(funcs.sum('isclick').alias('total_clicks'))
train_df = train_df.join(frequency_df, "userid", how='left')

# Calculate engagement (total number of visits) for each user
engagement_df = train_df.groupby('userid').count()
train_df = train_df.join(engagement_df, 'userid', how='left')

# create dataframe most only most recent session
train_df = last_seen_df.join(train_df, 'userid', how='left').drop_duplicates(subset=['userid'])

# Register temporary tables to be able to use sqlContext.sql
train_df.createOrReplaceTempView('train_df')
train_df = sqlContext.sql('SELECT recency, totalclicks, count FROM train_df')

# Save dataframe to S3 as csv
train_df.write.format('csv').save('s3://avito-ads/train/train_data')