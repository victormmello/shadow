from shadow_database import DatabaseConnection
from shadow_vtex.vtex import try_to_request, authenticated_request
from shadow_helpers.helpers import set_in_dict
# from shadow_database.shadow_helpers import make_dict, get_from_dict
from bs4 import BeautifulSoup as Soup
from datetime import datetime, timedelta
from multiprocessing import Pool

import os, fnmatch, shutil, requests, csv, json, requests

def clean_float_str(value):
	return round(float(str(value).strip().replace(',', '.')), 2)

dc = DatabaseConnection()

# =================== atualiza para o preco do csv
products = ('35.02.0774',)
product_filter = ','.join(["'%s'" % x for x in products])

def update_price_vtex(sku):
	original_price = clean_float_str(sku['original_price'])
	cost = clean_float_str(sku['cost'])
	sale_price = clean_float_str(sku['current_sale_price'])

	data = {
		"basePrice": original_price,
		"costPrice": cost,
		# "markup": 50,
		"fixedPrices": [
			{
				"tradePolicyId": "1",
				"value": sale_price,
				"listPrice": original_price,
				"minQuantity": 1,
				'dateRange': {
					'from': '2018-01-01T23:59:59-03:00',
					'to': '2028-01-01T23:59:59-03:00',
				}
			},
		]
	}

	if sku['sku_id']:
		sku_id = sku['sku_id']
	else:
		get_sku_id_url = 'http://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/%s' % sku['ean']
		response = authenticated_request("GET", get_sku_id_url)
		if not response:
			return 'Sku not found'

		json_response = json.loads(response.text)
		sku_id = json_response['Id']

	print("%s: %s -> %s" % (sku['prod_code'], original_price, sale_price))

	update_price_url = 'https://api.vtex.com/marciamello/pricing/prices/%s' % sku_id

	response = authenticated_request('PUT', update_price_url, data=json.dumps(data))

integration_check = []
if __name__ == '__main__':
	query = """
		SELECT
			ps.produto as prod_code,
			ps.codigo_barra as ean,
			pp.preco1 as original_price,
			pp.preco1*(1-pp.PROMOCAO_DESCONTO/100) as current_sale_price,
			c.preco1 as cost,
			vpi.item_id as sku_id
		FROM PRODUTOS_BARRA ps
		INNER JOIN produtos_precos pp on ps.produto = pp.produto and pp.codigo_tab_preco = 11
		INNER JOIN produtos_precos c on ps.produto = c.produto and c.codigo_tab_preco = 2
		LEFT JOIN bi_vtex_product_items vpi on vpi.ean = ps.codigo_barra
		where 1=1 
			and ps.produto in (%s)
		;
	""" % product_filter

	print(query)
	skus_to_update = dc.select(query, strip=True, dict_format=True)
	
	with Pool(10) as p:
		integration_check = p.map(update_price_vtex, skus_to_update)

	# for sku in skus_to_update:
		# f(sku)