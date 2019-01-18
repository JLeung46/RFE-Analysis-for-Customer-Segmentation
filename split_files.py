import os
import sqlite3
import csv

def split_file(db_name, table_name, output_path, num_samples):
	"""
	Splits sql database table into mutiple csv files.
	"""

	# Define database name and table name
	db_name = db_name
	table_name = table_name

	# Define output path
	output_path = output_path
	output_name_template='output_%s.csv'
	current_piece = 1

	# Create a connection and get a cursor
	connection = sqlite3.connect(db_name)
	cursor = connection.cursor()

	# Execute the query
	cursor.execute('select * from %s', table_name)

	# Get data in batches
	while True:
	    current_out_path = os.path.join(
	    output_path,
	    output_name_template % current_piece
	    )

	    f = open(current_out_path, 'w', encoding="utf-8", newline='')
	    outcsv = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)

	    rows = cursor.fetchmany(num_samples)
	    if len(rows) == 0:
	        break
	    else:
	        outcsv.writerows(rows)
	        current_piece += 1

	# Clean up
	f.close()
	cursor.close()
	connection.close()

if __name__ == '__main__':
	split_file('database.sqlite', 'SearchInfo', 'search_info_files')
	split_file('database.sqlite', 'trainSearchStream', 'train_search_stream_files')

