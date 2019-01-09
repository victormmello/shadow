from shadow_database import DatabaseConnection
import sys

query_file = sys.argv[1]

print('Running Query %s' % query_file)
dc = DatabaseConnection()
query = open(query_file).read()
print(query)
query_result = dc.execute(query)
print('Done!')
	