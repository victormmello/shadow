from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool
import sys

PARAMETERS = {
	"multi_thread":"True",
	"max_threads":0, # 0 if threads = len(data)
	"date_to":"GETDATE()-1",
	"workbook_name":"Daily Sales",
"worksheet_list":{
	"daily_sales/sales_data_mtd.sql":"SalesDataMTD",
	"daily_sales/sales_data_week.sql":"SalesDataWEEK",
	"daily_sales/sellers_data_mtd.sql":"SellerDataMTD",
	"daily_sales/sellers_data_week.sql":"SellerDataWEEK",
	"daily_sales/sales_orders.sql":"SalesOrders",
	"daily_sales/stock_data.sql":"StockData",
	"daily_sales/stock_data_wo_store.sql":"StockDataWOStore",
	"daily_sales/receivings_data.sql":"ReceivingsData",
	"daily_sales/receivings_data_wo_store.sql":"ReceivingsDataWOStore"
}
}

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
	print(sys.argv)
	# workbook_name = workbook_tuple[0]
	# workbook_dict = workbook_tuple[1]
	# print("Running %s..." % workbook_name,end='')
	# print(workbook_dict)
	# print("Done!")

	# workbook = shadow_google_spreadsheet.open(WORKBOOK_NAME)
	# dc = DatabaseConnection()

	# Single Thread:
	# for worksheet_dict in WORKSHEET_LIST:
		# get_and_set_data(worksheet_dict)

	# Multi Threading:
	# with Pool(len(WORKSHEET_LIST)) as p:
		# result = p.map(get_and_set_data, WORKSHEET_LIST)