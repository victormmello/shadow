from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool

WORKBOOK_NAME = 'Product Creation'
WORKSHEET_LIST = [
	{'query_file':'grupo_subgrupo.sql','worksheet_name':'Grupo e Subgrupo'},
	{'query_file':'categoria_subcategoria.sql','worksheet_name':'Categoria e Subcategoria'},
	{'query_file':'tipo.sql','worksheet_name':'Tipo'},
	{'query_file':'fabricante.sql','worksheet_name':'Fabricante'},
	{'query_file':'cor.sql','worksheet_name':'Cor'},
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