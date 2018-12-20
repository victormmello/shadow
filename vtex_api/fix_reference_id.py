from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
from shadow_vtex.vtex import update_vtex_product, try_to_request

import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool

# --------------------------------------------------------------------------------------------------------------------------------
api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

def get_product_id(ean):
	get_sku_url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/%s" % ean
	response = try_to_request("GET", get_sku_url, headers=api_connection_config)

	if response:
		return json.loads(response.text)['ProductId']


if __name__ == '__main__':
	query = """
		SELECT
			ps.produto,
			MAX(ps.codigo_barra) as ean,
			MAX(vpi.product_id) as product_id
		from produtos_barra ps
		left join bi_vtex_product_items vpi on vpi.ean = ps.codigo_barra
		where 1=1
			-- and vp.produto = '35.02.0766'
			-- and vpi.item_id in ('565561','576484','579452','576199','576185','576196','576204','576189','576203','576190','576205','576200','576191','576193','576197','576198','576202','576186','576201','576192','563753','563755','577973','577981','577979','577976','577977','577974','577978','581775','571642','571648','571645','571644','571646','571647','571643','571732','571729','571734','571731','571730','571727','571728','581727','581724','581725','581728','581774','581777','581772','581776','581771','571001','581722','572140','572144','572143','572145','572141','572139','581726','581723','583254','571009','571007','581384','571006','571003','571002','571005','571004','581387','581377','581370','581373','565384','565382','565386','581388','565381','565379','581379','581369','581383','581386','581371','581380','581375','565380','581382','565377','581385','581389','565376','581374','581376','581381','581378','581372')
			-- and vpi.product_id = 530739
			and ps.produto <> ''
		group by ps.produto
		;
	"""

	dc = DatabaseConnection()
	products_to_fix = dc.select(query, strip=True, dict_format=True)
	# print(products_to_fix)

	errors = []
	# Rodar sem thread:
	for product_to_fix in products_to_fix:
		product_id = product_to_fix['product_id']
		if not product_id:
			product_id = get_product_id(product_to_fix['ean'])

		if product_id:
			reference_fields = {
				'RefId': product_to_fix['produto'],
			}
			
			update_vtex_product(product_to_fix['product_id'], reference_fields)
		else:
			print('Product not registered on VTEX: %s' % product_to_fix['produto'])
		# errors.append(f(product_to_fix))

	errors = [x for x in errors if x]
	print(errors)