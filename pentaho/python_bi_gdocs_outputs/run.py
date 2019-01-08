from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool
import sys, json

# PARAMETERS = {
# 	"multi_thread":"True",
# 	"max_threads":0, # 0 if threads = len(data)
# 	"date_to":"GETDATE()-1",
# 	"workbook_name":"Daily Sales",
# 	"worksheet_list":{
# 		"daily_sales/sales_data_mtd.sql":"SalesDataMTD",
# 		"daily_sales/sales_data_week.sql":"SalesDataWEEK",
# 		"daily_sales/sellers_data_mtd.sql":"SellerDataMTD",
# 		"daily_sales/sellers_data_week.sql":"SellerDataWEEK",
# 		"daily_sales/sales_orders.sql":"SalesOrders",
# 		"daily_sales/stock_data.sql":"StockData",
# 		"daily_sales/stock_data_wo_store.sql":"StockDataWOStore",
# 		"daily_sales/receivings_data.sql":"ReceivingsData",
# 		"daily_sales/receivings_data_wo_store.sql":"ReceivingsDataWOStore"
# 	}
# }
MULTI_THREAD = sys.argv[1]
MAX_THREADS = sys.argv[2]
DATE_TO = sys.argv[3]
WORKBOOK_NAME = sys.argv[4]
WORKSHEET_LIST = json.loads(sys.argv[5])

def get_and_set_data(worksheet_tuple):
	print('Updating sheet: %s...' % worksheet_tuple[1],end='')
	worksheet = workbook.worksheet(worksheet_tuple[1])
	query = open(worksheet_tuple[0]).read()  % {
		'date_to':DATE_TO
		}
	query_result = dc.select(query,strip=True)
	columns = [column[0] for column in dc.cursor.description]
	shadow_google_spreadsheet.update_cells_with_dict(worksheet,columns,query_result)
	print('Done!')


if __name__ == '__main__':
	print("Running %s..." % WORKBOOK_NAME,end='')

	workbook = shadow_google_spreadsheet.open(WORKBOOK_NAME)
	dc = DatabaseConnection()

	query_result = dc.select("SELECT top 3 created_at from bi_vtex_order_items;",strip=True)
	# if MULTI_THREAD == "true":
	# 	thread_count = len(WORKSHEET_LIST) if MAX_THREADS == "0" else int(MAX_THREADS)
	# 	with Pool(thread_count) as p:
	# 		result = p.map(get_and_set_data, WORKSHEET_LIST.items())
	# else:
	# 	for worksheet_tuple in WORKSHEET_LIST.items():
	# 		get_and_set_data(worksheet_tuple)

	print("Done!")
