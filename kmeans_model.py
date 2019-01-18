from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

class KMeansModel:
    """
    Class to train a KMeans model and return the predictions
    """
    def __init__(self):
        self.assembled_df = None

    def assemble_features(self, df, feature_names, output_col_name):
        vec_assembler = VectorAssembler(inputCols=feature_names, outputCol=output_col_name)
        assembler_df = vec_assembler.transform(df)
        self.assembled_df = assembler_df

    def train(self, k=5, seed=3):
        kmeans = KMeans().setK(k).setSeed(seed)
        model = kmeans.fit(new_df.select("features"))
        transformed = model.transform(new_df)
        preds = transformed.select("prediction")
        return preds
        
