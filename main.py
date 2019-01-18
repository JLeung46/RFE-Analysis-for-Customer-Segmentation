from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType, DateType, StringType
from pyspark.sql.functions import monotonically_increasing_id
from utils import *

# Define schemas
search_stream_schema = StructType([
    StructField("searchid", IntegerType(), True),
    StructField("adid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("objecttype", IntegerType(), True),
    StructField("histctr", DecimalType(), True),
    StructField("isclick", IntegerType(), True),])


search_info_schema = StructType([
    StructField("searchid", IntegerType(), True),
    StructField("searchdate", DateType(), True),
    StructField("ipid", IntegerType(), True),
    StructField("userid", IntegerType(), True),
    StructField("isuserloggedon", IntegerType(), True),
    StructField("searchquery", StringType(), True),
    StructField("locationid", IntegerType(), True),
    StructField("categoryid", IntegerType(), True),
    StructField("searchparams", StringType(), True),])

# Define data directories
search_stream_dir = "s3a://avito-ads/load/lo/*.gz"
search_info_dir = "s3a://avito-ads/load/searchInfo/*.gz"


if __name__ == '__main__':


	# Load Data
	search_stream_df = Data(search_stream_schema)
	search_stream_df.load_data(search_stream_dir)

	search_info_df = Data(search_info_schema)
	search_info_df.load_data(search_info_dir)

	# Merge dataframes
	full_df = search_stream_df.dataframe.join(search_info_df.dataframe, 'searchid', how='left')
	full_df.cache()

	# Calc RFE features
	rfe_data = RFE(full_df)
	rfe_data.get_rfe_features('searchdate', 'userid', 'isclick', 'total_clicks', ["recency", "total_clicks", "count"])

	# Get sample
	sample_df = rfe_data.get_sample()
	#sample_df.cache()

	# Scale data
	scaled_data = scale_features(sample_df, ['recency', 'total_clicks', 'count'], "features", "features_scaled")
	
	# Transform array of scaled featues into columns
	scaled_data = scaled_data.select(["recency", "total_clicks", "count", "features_scaled"]).rdd\
               .map(extract).toDF().show(['recency_scaled', 'total_clicks_scaled', 'count_scaled'])

    # Create an id column to merge two dataframes
	sample_df = sample_df.withColumn("id", monotonically_increasing_id())
	scaled_data = scaled_data.withColumn("id", monotonically_increasing_id())
	final_df = sample_df.join(scaled_data, "id", "left").drop("id")

	# Save dataframe to S3 as csv
	save_data(final_df, "s3://avito-testing/data/train_data")

	# Train KMeans
	kmeans = KMeansModel()
	assembler_df = kmeans.assemble_features(final_df, ["recency_scaled", "total_clicks_scaled", "count_scaled" ], "features")
	preds = kmeans.train(k=5)
	save_data(preds, "s3://avito-testing/predictions")