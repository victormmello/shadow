import json
import pyodbc
# import pkgutil
from shadow_database import database_config

# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

class DatabaseConnection(object):
	def __init__(self):
		# database_connection_file = pkgutil.get_data(__package__, 'database_connection.json')
		# database_connection_file = open("database_connection.json", 'rb')
		# database_connection_config = json.load(database_connection_file)

		self.cnxn = pyodbc.connect('DRIVER=%s;SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (
			database_config.driver,
			database_config.server,
			database_config.database,
			database_config.username,
			database_config.password
			))
		self.cursor = self.cnxn.cursor()

	def select(self, query, strip=False, dict_format=False):
		self.cursor.execute(query)
		results = self.cursor.fetchall()

		if not dict_format:
			return results
		else:
			columns = [column[0] for column in self.cursor.description]

			result_dicts = []
			for result in results:
				result_dict = {}

				for i, value in enumerate(result):
					if strip and isinstance(value, str):
						value = value.strip()

					result_dict[columns[i]] = value

				result_dicts.append(result_dict)

			return result_dicts

	
	def execute(self, query):
		self.cursor.execute(query)
		self.cnxn.commit()

	def sanitize(self,s):
		return str(s).replace("'","''")

	def insert(self, table, values, columns=[], print_only=False):
		column_query = "(%s)" % ','.join(columns) if columns else ''
		for query_values_list in chunks(values,999):
			insert_list = []
			for values in query_values_list:
				sanitized_values = []
				for value in values:
					if value is None:
						sanitized_values.append('null')
					else:
						sanitized_values.append('\'%s\'' % self.sanitize(value))

				query_row = '(%s)' % ','.join(sanitized_values)
				insert_list.append(query_row)
			query = """INSERT INTO %s %s VALUES
			%s
			""" % (table,column_query,'\n,'.join(insert_list))
			if print_only:
				print(query)
			else:
				self.execute(query)

	def insert_dict(self, table, value_dicts, print_only=False):
		columns = value_dicts[0].keys()
		column_query = "(%s)" % ','.join(columns)
		for query_values_dicts in chunks(value_dicts, 999):
			insert_list = []
			for query_values_dict in query_values_dicts:
				sanitized_values = []
				for col in columns:
					value = query_values_dict[col]
					if value is None:
						sanitized_values.append('null')
					else:
						sanitized_values.append('\'%s\'' % self.sanitize(value))

				query_row = '(%s)' % ','.join(sanitized_values)
				insert_list.append(query_row)
			query = """INSERT INTO %s %s VALUES
			%s
			""" % (table,column_query,'\n,'.join(insert_list))
			if print_only:
				print(query)
			else:
				self.execute(query)

			