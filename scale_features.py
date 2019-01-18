from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from utils import *

train_schema = StructType([
    StructField("days_since_last_visit", IntegerType(), True),
    StructField("total_clicks", IntegerType(), True),
    StructField("total_sessions", IntegerType(), True)])

train_df = spark.read.csv("s3a://avito-ads/train/train_data/*.csv",header=False,schema=train_schema)

# Take sample of data
train_df = train_df.sample(False,0.05, seed=3)

# Save same to S3 as csv
train_df.write.format('csv').save('s3://avito-ads/train/sample')

# Convert features into a vector
assembler = VectorAssembler(inputCols= ['days_since_last_visit', 'total_clicks', 'total_sessions'], outputCol = "features")

# Create a scaler
scaler = StandardScaler(inputCol="features", outputCol="features_scaled",
                        withStd=True, withMean=True)

# Feed vectorized features to be scaled into a pipeline
scale_pipeline = Pipeline(stages=[assembler, scaler])
scaledData = scale_pipeline.fit(train_df).transform(train_df)

# Convert vector of scaled features back into individual columns
final_data = scaledData.select("features_scaled").rdd\
               .map(extract).toDF(train_df.columns)

# Save scaled features to S3
final_data.write.format('csv').save('s3://avito-ads/scaled_data/sample')