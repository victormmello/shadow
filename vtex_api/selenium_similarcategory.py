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
query = """
	SELECT
		ps.produto, v_item.product_id
	from dbo.PRODUTOS_BARRA ps
	inner join dbo.PRODUTOS p on p.produto = ps.produto
	inner join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
	where 1=1 and
		ps.produto in ('01.02.1008','08.01.186','08.01.188','08.04.023','08.04.024','08.06.078','08.10.0010','08.10.0011','08.10.0012','08.10.0014','08.11.0002','20.02.0018','22.02.0181','22.02.0217','22.02.0246','22.03.0118','22.04.0103','22.04.0153','22.04.0156','22.04.0189','22.05.0276','22.05.0306','22.05.0472','22.05.0486','22.05.0489','22.05.0492','22.05.0505','22.05.0526','22.05.0544','22.05.0552','22.05.0566','22.05.0570','22.05.0571','22.05.0574','22.06.0467','22.06.0469','22.06.0477','22.07.0129','22.07.0250','22.07.0252','22.07.0260','22.07.0261','22.07.0269','22.07.0276','22.07.0279','22.07.0283','22.09.0086','22.12.0395','22.12.0437','22.12.0458','22.12.0465','22.12.0560','22.12.0591','22.12.0593','22.12.0597','22.12.0598','22.12.0600','22.12.0601','22.12.0611','22.12.0621','22.15.0007','22.15.0014','23.02.0013','23.04.0020','23.04.0021','23.08.0155','23.08.0208','23.11.0059','23.11.0186','29.02.0076','31.02.0119','32.02.0236','32.03.0249','32.03.0272','32.03.0273','32.07.0036','33.03.0028','33.03.0032','34.03.0016','34.05.0027','35.01.0499','35.02.0486','35.02.0487','35.02.0488','35.02.0543','35.02.0599','35.02.0829','35.04.0041','35.05.0026','35.09.0703','35.09.0854','35.09.0984','35.09.1182','41.01.0083','41.01.0086')
	group by ps.produto, v_item.product_id
	;
"""
# products_to_modify = ['530656']
products_to_modify = dc.select(query, strip=True, dict_format=True)
# raise Exception(len(products_to_modify))

timeout = 20
exclude_var = 'Black Friday 50 OFF'
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
			# product_id = product_info
			# print(product_id)
			product_id = product_info['product_id']
			print(product_info)
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
			            # print(col.text)
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