from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
import time
from shadow_database import DatabaseConnection
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
from bs4 import BeautifulSoup as Soup
retry = 3
product_succeed_selenium = []
product_fail_selenium = []
# 20.03.0030
# query = """
# 	SELECT
# 		ps.produto, v_item.product_id
# 	from dbo.PRODUTOS_BARRA ps
# 	inner join dbo.PRODUTOS p on p.produto = ps.produto
# 	inner join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
# 	where 1=1 and
# 		ps.produto in ('28.01.0052')
# 	group by ps.produto, v_item.product_id
# 	;
# """
products_to_modify = ['530656']
# products_to_modify = dc.select(query, trim=True)
# raise Exception(len(products_to_modify))

timeout = 20
exclude_var = 'Integracao'
path ="C:/geckodriver.exe"

options = Options()
options.headless = True
driver = webdriver.Firefox(options=options, executable_path=path)
driver.get("https://marciamello.myvtex.com/_v/auth-server/v1/login?ReturnUrl=%2Fadmin%2F")

action = driver.find_element_by_id("vtexIdUI-google-plus")
action.click()

window_handles = driver.window_handles
driver.switch_to.window(window_handles[1])
print("Authenticating...")
element_present = EC.presence_of_element_located((By.ID, 'identifierId'))
WebDriverWait(driver, timeout).until(element_present)

identifier_id = driver.find_element_by_id("identifierId")
identifier_id.send_keys("guilherme.sato.mmello@gmail.com")
action = driver.find_element_by_id("identifierNext")
action.click()
print("Login id validated")
time.sleep(3)
identifier_id = driver.find_element_by_xpath("//input[@class='whsOnd zHQkBf']")
identifier_id.send_keys("#googlemm")

action = driver.find_element_by_id("passwordNext")
action.click()
print("Password validated")
driver.switch_to.window(window_handles[0])

for product_info in products_to_modify:
	for i in range(0,3):
		try:
			product_id = product_info
			print(product_id)
			# product_id = product_info['product_id']
			time.sleep(5)
			driver.get("https://marciamello.vtexcommercestable.com.br/admin/Site/ProdutoCategoriaSimilar.aspx?IdProduto=%s" % product_id) 
			element_present = EC.presence_of_element_located((By.CSS_SELECTOR, "div#content table tr td"))
			WebDriverWait(driver, timeout).until(element_present)
			table_id = driver.find_element_by_css_selector('div#content table')
			rows = table_id.find_elements(By.TAG_NAME, "tr")[1:]
			# print(len(rows))
			# print(table_id)
			for i,row in enumerate(rows,1):
				cols = row.find_elements(By.TAG_NAME, "th")
				for col in cols:
			            print(col.text)
			            if col.text == exclude_var:
			            	WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div#content table tbody tr td a")))
			            	# element_present = EC.presence_of_element_located((By.CSS_SELECTOR, "div#content table tbody tr td a span"))
			            	# WebDriverWait(driver, timeout).until(element_present)
			            	# element_present = EC.presence_of_element_located((By.ID, "ctl00_Conteudo_rptProdutoCategoriaSimilar_ctl0%s_lnkApagar"% i)) 
			            	# print(element_present)
			            	# WebDriverWait(driver, timeout).until(element_present)
			            	row.find_element_by_css_selector('div#content table td a span').click()
			            	alert = driver.switch_to_alert()
			            	alert.accept()
			            	print('Removed')
			            	product_succeed_selenium.append(product_id)
			break

		except Exception as e:
			print('retrying')
			if i == retry-1:
				product_fail_selenium.append(product_id)
				print('error')
driver.quit()
with open('selenium_succeed2.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";")
	for n in product_succeed_selenium:
		csv_file.writerow({n})

with open('selenium_fail2.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";")
	for n in product_fail_selenium:
		csv_file.writerow({n})