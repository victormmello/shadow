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

		self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (
			database_config.server,
			database_config.database,
			database_config.username,
			database_config.password
			))
		self.cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
		self.cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='latin1')
		self.cnxn.setencoding(encoding='latin1')
		self.cursor = self.cnxn.cursor()

	def select(self,query):
		self.cursor.execute(query)
		return self.cursor.fetchall()
	
	def execute(self,query):
		self.cursor.execute(query)
		self.cnxn.commit()

	def sanitize(self,s):
		return str(s).replace("'","''")

	def insert(self,table,values,columns=[],print_only=False):
		column_query = "(%s)" % ','.join(columns) if columns else ''
		for query_values_list in chunks(values,999):
			insert_list = []
			for value in query_values_list:
				query_row = '(%s)' % ','.join('\'%s\'' % self.sanitize(cell) for cell in value)
				insert_list.append(query_row)
			query = """INSERT INTO %s %s VALUES
			%s
			""" % (table,column_query,'\n,'.join(insert_list))
			if print_only:
				print(query)
			else:
				self.execute(query)

			