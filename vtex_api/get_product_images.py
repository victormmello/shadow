from shadow_database import DatabaseConnection
from shadow_helpers.helpers import make_dict
from shadow_vtex.vtex import try_to_request

from datetime import datetime, timedelta
from multiprocessing import Pool, Manager
import requests, json, dateutil.relativedelta

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

def make_table_row(sku, image_url):
	if not image_url:
		return

	return (sku['produto'], sku['cor_produto'], image_url)

def get_image_url(sku):
	print(sku)
	get_sku_id_url = 'http://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/%s' % sku['ean']
	response = try_to_request("GET", get_sku_id_url, headers=api_connection_config)
	if not response:
		return make_table_row(sku, None)

	json_response = json.loads(response.text)
	image = json_response['Images'][0]['ImageUrl']
	print(image)
	return make_table_row(sku, image)

if __name__ == '__main__':
	dc = DatabaseConnection()

	skus_for_image = """
		SELECT
			ps.produto,
			ps.cor_produto,
			MAX(ps.CODIGO_BARRA) as ean
		FROM produtos_barra ps
		LEFT JOIN dbo.W_ESTOQUE_DISPONIVEL_SKU e on e.codigo_barra = ps.CODIGO_BARRA
		where e.estoque_disponivel > 0
			-- and ps.produto = '23.11.0244'
			-- and ps.COR_PRODUTO = '32'
		group by ps.produto, ps.COR_PRODUTO-- , LEFT(ps.CODIGO_BARRA, LEN(ps.CODIGO_BARRA)-LEN(ps.grade))
		;
	"""

	skus_for_image = dc.select(skus_for_image, strip=True, dict_format=True)

	# images = []
	# for sku in skus_for_image:
	# 	images.append(get_image_url(sku))

	with Pool(50) as p:
		images = p.map(get_image_url, skus_for_image)

	images = [x for x in images if x]

	# brute-force remove repeated images
	images = list(set(images))

	dc = DatabaseConnection()
	print('Inserting into tables...')

	print('	bi_sku_images')
	dc.execute('TRUNCATE TABLE bi_sku_images;')
	dc.insert('bi_sku_images', images, print_only=False)

