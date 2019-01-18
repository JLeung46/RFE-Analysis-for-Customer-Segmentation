train_schema = StructType([
    StructField("days_since_last_visit", DoubleType(), True),
    StructField("total_clicks", DoubleType(), True),
    StructField("total_sessions", DoubleType(), True)])

train_data = spark.read.csv("s3a://avito-ads/scaled_data/sample/*.csv",header=False,schema=train_schema)

vecAssembler = VectorAssembler(inputCols=["days_since_last_visit", "total_clicks", "total_sessions" ], outputCol="features")
new_df = vecAssembler.transform(train_data)

for k in range(3,11):
    kmeans = KMeans().setK(k).setSeed(1)
    model = kmeans.fit(new_df.select("features"))
    transformed = model.transform(new_df)
    preds = transformed.select("prediction")
    preds.write.format('csv').save('s3://avito-ads/predictions/cluster_' + str(k))