import gspread
from shadow_google_spreadsheet.client_secret import client_secret_data
from oauth2client.service_account import ServiceAccountCredentials

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

def get_all_recods(workbook,worksheet_name):
	# Extract all of the values
	list_of_hashes = workbook.worksheet(worksheet_name).get_all_records()
	return list_of_hashes

def update_cell(worksheet,row,column,value,max_tries=3,try_number=1):
	try:
		worksheet.update_cell(row,column,value)
	except:
		if try_number < max_tries:
			time.sleep(try_number)
			try_number += 1
			update_cell(worksheet,row,column,value,try_number=try_number)