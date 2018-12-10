from shadow_database import DatabaseConnection
# from shadow_database.shadow_helpers import make_dict, get_from_dict
import os, fnmatch, shutil, requests, csv, json, requests
from bs4 import BeautifulSoup as Soup
from datetime import datetime, timedelta

dc = DatabaseConnection()

query = """
	SELECT 
		e.codigo_barra,
		vpi.item_id as sku_id,
		e.estoque_disponivel as stock_linx,
		vpi.stock_quantity as stock_vtex
	from w_estoque_disponivel_sku e
	LEFT JOIN bi_vtex_product_items vpi on e.codigo_barra = vpi.ean and e.filial = 'e-commerce'
	where vpi.stock_quantity is null or vpi.stock_quantity != e.estoque_disponivel
	;
"""

results = dc.select(query, strip=True)
columns = [column[0] for column in dc.cursor.description]

with open('query_result.csv', 'w', newline='\n') as csvfile:
	writer = csv.writer(csvfile, delimiter=';')
	writer.writerow(columns)
	writer.writerows(results)
