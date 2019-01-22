import gspread
from shadow_google_spreadsheet.client_secret import client_secret_data
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import numbers

def open(workbook_name):
	# use creds to create a client to interact with the Google Drive API
	# scope = ['https://spreadsheets.google.com/feeds']
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	# creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
	creds = ServiceAccountCredentials.from_json_keyfile_dict(client_secret_data, scope)
	client = gspread.authorize(creds)

	# Find a workbook by name and open the first sheet
	# Make sure you use the right name here.
	# ex: sheet = client.open("MTG").worksheet("Cards Input")
	workbook = client.open(workbook_name)

	return workbook

def update_cell(worksheet,row,column,value,max_tries=3,try_number=1):
	try:
		worksheet.update_cell(row,column,value)
	except:
		if try_number < max_tries:
			time.sleep(try_number)
			try_number += 1
			update_cell(worksheet,row,column,value,try_number=try_number)

def get_column_name(col_number):
	col_names = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','AA','AB','AC','AD','AE','AF','AG','AH','AI','AJ','AK','AL','AM','AN','AO','AP','AQ','AR','AS','AT','AU','AV','AW','AX','AY','AZ']
	return col_names[col_number-1]

def excel_date(date1):
	temp = datetime(1899, 12, 30)    # Note, not 31st Dec but 30th!
	delta = date1 - temp
	return float(delta.days) + (float(delta.seconds) / 86400)

def update_cells_with_dict(worksheet,columns,query_result):
	if query_result:
		worksheet_rows = len(query_result)+1
		worksheet_cols = len(query_result[0])
		last_col_name = get_column_name(worksheet_cols)
	
		worksheet.resize(rows=worksheet_rows, cols=worksheet_cols)

		concat_list = columns
		for l in query_result:
			concat_list += l

		cell_list = worksheet.range('A1:%s%d' % (last_col_name,worksheet_rows))
		i = 0
		for cell in cell_list:
			if isinstance(concat_list[i], datetime):
				cell.value = excel_date(concat_list[i])
			elif isinstance(concat_list[i],numbers.Number):
				cell.value = int(round(concat_list[i],0))
			else:
				cell.value = str(concat_list[i])
			i += 1

		worksheet.update_cells(cell_list)
	
	else:
		worksheet.resize(rows=1)