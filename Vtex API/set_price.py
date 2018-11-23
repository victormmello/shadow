from shadow_database import DatabaseConnection
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

def try_to_request(*args, **kwargs):
	retry = 3
	for i in range(0, retry):
		response = None
		try:
			response = requests.request(*args, **kwargs)

			if response.status_code == 200:
				break
			elif response.status_code == 429:
				import time
				time.sleep(10)
			else:
				raise Exception()


		except Exception as e:
			if i == retry-1:
				if response:
					print(response.text)

				return None
	
	return response

# product_filter = "p.produto='22.07.0254'"
# query = """
# 	SELECT
# 		vpi.item_id as sku_id,
# 		pp.preco1 as original_price,
# 		pp.preco_liquido1 as sale_price,
# 		c.preco1 as cost
# 	from dbo.PRODUTOS_BARRA ps
# 	INNER JOIN dbo.PRODUTOS p on p.produto = ps.produto
# 	INNER JOIN dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
# 	INNER JOIN dbo.bi_vtex_product_items vpi on vpi.ean = ps.codigo_barra
# 	INNER JOIN produtos_precos pp on p.produto = pp.produto and pp.codigo_tab_preco = 11
# 	INNER JOIN produtos_precos c on p.produto = c.produto and c.codigo_tab_preco = 2
# 	LEFT JOIN w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
# 	where 1=1 
# 		and pp.preco_liquido1 > (pp.preco1/2)
# 		and (%s) 
# 	;
# """ % product_filter

product_price = {}
with open('repricing.csv', encoding='latin-1') as csvfile:
	reader = csv.DictReader(csvfile, delimiter=';')
	for row in reader:
		prod_code = row['produto'].strip()
		product_price[prod_code] = float(row['novo_preco'].strip().replace(',', '.'))

product_filter = ','.join(product_price)

# product_filter = "1=1"
query = """
	SELECT
		ps.produto as prod_code,
		vpi.item_id as sku_id,
		-- CAST(vp.sale_price as float)/2 as fixed_sale_price,
		vp.original_price as original_price,
		c.preco1 as cost
	from bi_vtex_products vp
	inner join bi_vtex_product_items vpi on vpi.product_id = vp.product_id
	INNER JOIN PRODUTOS_BARRA ps on vpi.ean = ps.codigo_barra
	INNER JOIN produtos_precos c on ps.produto = c.produto and c.codigo_tab_preco = 2
	where 1=1 
		-- and vp.sale_price/CAST(vp.original_price as float) > 0.5
		and (%s)
	;
""" % product_filter

skus_to_update = dc.select(query, strip=True, dict_format=True)

def f(sku):
	get_price_url = 'https://api.vtex.com/marciamello/pricing/prices/%(sku_id)s' % sku

	# response = try_to_request('GET', get_price_url, headers=api_connection_config)

	# sku_price_info = json.loads(response.text)
	# current_price = sku_price_info['listPrice']
	# for x in sku_price_info.get('fixedPrices', []):
	# 	if x['tradePolicyId'] == '1':
	# 		current_price = x['value']
	sale_price = product_filter[sku['prod_code']]
	original_price = float(sku['original_price'])

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
