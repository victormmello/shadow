from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool

WORKBOOK_NAME = 'Modelo de Compras'
WORKSHEET_LIST = [
	{'query_file':'depth.sql','worksheet_name':'Depth'},
	{'query_file':'plan.sql','worksheet_name':'Plan'},
]

workbook = shadow_google_spreadsheet.open(WORKBOOK_NAME)
dc = DatabaseConnection()

def get_and_set_data(worksheet_dict):
	print('Updating sheet: %s...' % worksheet_dict['worksheet_name'],end='')
	worksheet = workbook.worksheet(worksheet_dict['worksheet_name'])
	query = open(worksheet_dict['query_file']).read()
	query_result = dc.select(query,strip=True)
	columns = [column[0] for column in dc.cursor.description]
	shadow_google_spreadsheet.update_cells_with_dict(worksheet,columns,query_result)
	print('Done!')

if __name__ == '__main__':

	# Single Thread:
	# for worksheet_dict in WORKSHEET_LIST:
		# get_and_set_data(worksheet_dict)

	# Multi Threading:
	with Pool(len(WORKSHEET_LIST)) as p:
		result = p.map(get_and_set_data, WORKSHEET_LIST)