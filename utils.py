from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

def extract(row):
    """
    Separate an array into columns
    """
    return tuple(row.features_scaled.toArray().tolist())

def save_data(df, save_dir):
	# Save dataframe to S3 as csv
	df.write.format('csv').save(save_dir)

def scale_features(df, inp_col_names, output_col_name, scaled_col_name):
    # Convert features into a vector
    assembler = VectorAssembler(inputCols=inp_col_names, outputCol=output_col_name)

    # Define a scaler
    scaler = StandardScaler(inputCol=output_col_name, outputCol=scaled_col_name,
                            withStd=True, withMean=True)

    # Feed vectorized features and scaler into a pipeline
    scale_pipeline = Pipeline(stages=[assembler, scaler])
    scaled_data = scale_pipeline.fit(df).transform(df)
    return scaled_data