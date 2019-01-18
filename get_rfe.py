import pyspark.sql.functions as funcs
from datetime import datetime

class RFE:
	"""
	Class to calulate the Recency, Frequency and Enagement of users.
	"""
	def __init__(self, data):
		self.data = data
		self.rfe_data = None

	def get_recency(self, date_col, last_date, date_format='%Y %m %d'):
		"""
		Calculates the number of days a user was last seen
		"""
		# Convert string to datetime object
		last_datetime = datetime.strptime(last_date, date_format).date()
		self.rfe_data = self.data.withColumn('recency',funcs.datediff(funcs.lit(last_datetime), self.data[date_col]))

	def get_frequency(self, groupby_col, agg_col, col_name):
		"""
		Calculates total number of clicks per user
		"""
		frequency_df = self.rfe_data.groupby(groupby_col).agg(funcs.sum(agg_col).alias(col_name))
		self.rfe_data = self.rfe_data.join(frequency_df, groupby_col, how='left')

	def get_engagement(self, groupby_col):
		"""
		Counts total number of sessions per user
		"""
		engagement_df = self.rfe_data.groupby(groupby_col).count()
		self.rfe_data = self.rfe_data.join(engagement_df, groupby_col, how='left')

	def get_most_recent(self, id_col, date_col):
		""" 
		Get only rows from user's last seen date
		"""
		last_seen_df = self.rfe_data.groupby(id_col).agg(funcs.max(date_col).alias('last_seen'))
		# Create dataframe with only most recent session
		self.rfe_data = last_seen_df.join(self.rfe_data, id_col, how='left').drop_duplicates(subset=[id_col])

	def get_rfe_features(self, date_col, id_col, agg_col, freq_col_name, result_col_names, last_date="2015 5 21"):
		# Calculate RFE features
		self.get_recency(date_col, last_date)
		self.get_frequency(id_col, agg_col, freq_col_name)
		self.get_engagement(id_col)
		self.get_most_recent(id_col, date_col)
		self.rfe_data = self.rfe_data.select(result_col_names)

	def get_sample(self, sample_size=0.05, sample_seed=3):
		sample_df = self.rfe_data.sample(False, sample_size, seed=sample_seed)
		return sample_df
