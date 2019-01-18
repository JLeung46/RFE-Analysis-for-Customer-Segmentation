#"s3a://avito-ads/load/lo/*.gz"

class Data:
	def __init__(self, schema):
		self.schema = schema
		self.dataframe = None

	def load_data(self, dir_name, include_header=False):
		self.dataframe = spark.read.csv(dir_name, header=include_header, schema=self.schema)