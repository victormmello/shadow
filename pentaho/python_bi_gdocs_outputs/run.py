from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool
import sys, json

MULTI_THREAD = sys.argv[1]
MAX_THREADS = sys.argv[2]
DATE_TO = sys.argv[3]
WORKBOOK_NAME = sys.argv[4]
WORKSHEET_LIST = json.loads(sys.argv[5])

workbook = shadow_google_spreadsheet.open(WORKBOOK_NAME)
dc = DatabaseConnection()

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

	if MULTI_THREAD == "true":
		thread_count = len(WORKSHEET_LIST) if MAX_THREADS == "0" else int(MAX_THREADS)
		with Pool(thread_count) as p:
			result = p.map(get_and_set_data, WORKSHEET_LIST.items())
	else:
		for worksheet_tuple in WORKSHEET_LIST.items():
			get_and_set_data(worksheet_tuple)

	print("Done!")
