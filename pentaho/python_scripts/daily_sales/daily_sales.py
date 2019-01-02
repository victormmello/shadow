from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool

WORKBOOK_NAME = 'Daily Sales'
WORKSHEET_LIST = [
	{'query_file':'daily_sales_sales_data_mtd.sql','worksheet_name':'SalesDataMTD'},
	{'query_file':'daily_sales_sales_data_week.sql','worksheet_name':'SalesDataWEEK'},
	{'query_file':'daily_sales_sellers_data_mtd.sql','worksheet_name':'SellerDataMTD'},
	{'query_file':'daily_sales_sellers_data_week.sql','worksheet_name':'SellerDataWEEK'},
	{'query_file':'daily_sales_sales_orders.sql','worksheet_name':'SalesOrders'},
	{'query_file':'daily_sales_stock_data.sql','worksheet_name':'StockData'},
	{'query_file':'daily_sales_stock_data_wo_store.sql','worksheet_name':'StockDataWOStore'},
	{'query_file':'daily_sales_receivings_data.sql','worksheet_name':'ReceivingsData'},
	{'query_file':'daily_sales_receivings_data_wo_store.sql','worksheet_name':'ReceivingsDataWOStore'},
]
DATE_TO = 'GETDATE()-1'

workbook = shadow_google_spreadsheet.open(WORKBOOK_NAME)
dc = DatabaseConnection()

def get_and_set_data(worksheet_dict):
	print('Updating sheet: %s...' % worksheet_dict['worksheet_name'],end='')
	worksheet = workbook.worksheet(worksheet_dict['worksheet_name'])
	query = open(worksheet_dict['query_file']).read()  % {
		'date_to':DATE_TO
		}
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