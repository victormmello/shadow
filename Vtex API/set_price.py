from shadow_database import DatabaseConnection
from shadow_vtex.vtex import try_to_request
from shadow_helpers.helpers import set_in_dict
# from shadow_database.shadow_helpers import make_dict, get_from_dict
import os, fnmatch, shutil, requests, csv, json, requests
from bs4 import BeautifulSoup as Soup
from datetime import datetime, timedelta

dc = DatabaseConnection()

api_connection_config = {
	"Content-Type": "application/json",
	"X-VTEX-API-AppKey": "vtexappkey-marciamello-XNZFUX",
	"X-VTEX-API-AppToken": "HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ"
}

# =================== atualiza para o preco do csv
product_price = {}
with open('repricing.csv', encoding='latin-1') as csvfile:
	reader = csv.DictReader(csvfile, delimiter=';')
	for row in reader:
		prod_code = row['produto'].strip()

		# set_in_dict(product_price, float(row['preco_de'].strip().replace(',', '.')), [prod_code, 'original_price'])
		set_in_dict(product_price, float(row['preco_por'].strip().replace(',', '.')), [prod_code, 'sale_price'])

product_filter = ','.join(["'%s'" % x for x in product_price])

query = """
	SELECT
		ps.produto as prod_code,
		vpi.item_id as sku_id,
		vp.original_price as original_price,
		c.preco1 as cost
	from bi_vtex_products vp
	INNER JOIN bi_vtex_product_items vpi on vpi.product_id = vp.product_id
	INNER JOIN PRODUTOS_BARRA ps on vpi.ean = ps.codigo_barra
	INNER JOIN produtos_precos c on ps.produto = c.produto and c.codigo_tab_preco = 2
	where 1=1 
		and ps.produto in (%s)
	order by ps.produto
	;
""" % product_filter

# =================== atualiza tudo para 50% +
# product_filter = "1=1"
# query = """
# 	SELECT
# 		ps.produto as prod_code,
# 		vpi.item_id as sku_id,
# 		CAST(vp.original_price as float)/2 as fixed_sale_price,
# 		vp.original_price as original_price,
# 		c.preco1 as cost
# 	from bi_vtex_products vp
# 	INNER JOIN bi_vtex_product_items vpi on vpi.product_id = vp.product_id
# 	INNER JOIN PRODUTOS_BARRA ps on vpi.ean = ps.codigo_barra
# 	INNER JOIN produtos_precos c on ps.produto = c.produto and c.codigo_tab_preco = 2
# 	where 1=1 
# 		and vp.sale_price/CAST(vp.original_price as float) > 0.5
# 		and (%s)
# 	;
# """ % product_filter

# ======================================================

# skus_to_update = [{'sku_id': 574838}]
print(query)
skus_to_update = dc.select(query, strip=True, dict_format=True)

def f(sku):
	# get_price_url = 'https://api.vtex.com/marciamello/pricing/prices/%(sku_id)s' % sku

	# response = try_to_request('GET', get_price_url, headers=api_connection_config)

	# sku_price_info = json.loads(response.text)


	# current_price = sku_price_info['listPrice']
	# for x in sku_price_info.get('fixedPrices', []):
	# 	if x['tradePolicyId'] == '1':
	# 		current_price = x['value']
	# sale_price = product_filter[sku['prod_code']]
	if 'original_price' in sku:
		original_price = float(sku['original_price'])
	else:
		original_price = product_price[sku['prod_code']]['original_price']

	if 'fixed_sale_price' in sku:
		sale_price = sku['fixed_sale_price']
	else:
		sale_price = product_price[sku['prod_code']]['sale_price']


	data = {
		"basePrice": original_price,
		"costPrice": float(sku['cost']),
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

	print("%s: %s -> %s" % (sku['prod_code'], original_price, sale_price))

	update_price_url = 'https://api.vtex.com/marciamello/pricing/prices/%(sku_id)s' % sku

	response = try_to_request('PUT', update_price_url, headers=api_connection_config, data=json.dumps(data))

integration_check = []
from multiprocessing import Pool
if __name__ == '__main__':
	with Pool(10) as p:
		integration_check = p.map(f, skus_to_update)

	# for sku in skus_to_update:
		# f(sku)