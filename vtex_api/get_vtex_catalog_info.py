from shadow_database import DatabaseConnection
import requests, json
from datetime import datetime
from multiprocessing import Pool

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

def get_product_info(product_ref_id):
	product_info = {
		"product_categories":[],
		"product_items":[],
		"product_images":[],
		"products":[]
	}

	url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pub/products/search"
	params = {"fq":"alternateIds_RefId:%s" % product_ref_id['produto']}

	for i in range(0,20):
		try:
			response = requests.request("GET", url, headers=api_connection_config, params=params)
			json_response = json.loads(response.text)
			print('.', end='')

			break
		except Exception as e:
			pass

	for product in json_response:
		product_id = product["productId"]
		produto = product["productReference"]
		original_price = 0
		sale_price = 0
		for item in product["items"]:
			item_original_price = item["sellers"][0]["commertialOffer"]["ListPrice"]
			item_sale_price = item["sellers"][0]["commertialOffer"]["Price"]
			original_price = max(original_price,item_original_price)
			sale_price = max(sale_price,item_sale_price)

			product_info["product_items"].append([
				item["itemId"], #"item_id"
				item["ean"], #"ean"
				item["sellers"][0]["commertialOffer"]["AvailableQuantity"], #"stock_quantity"
				item["images"][0]["imageUrl"], #"image_url",
				product["productId"],
				item.get('Cor', [''])[0], #"cor vtex",
			])

			# if item["ean"] == '35020650485PP':
			# 	raise Exception(item)

			for image in item["images"]:
				product_info["product_images"].append([
					item["ean"], #"ean"
					image["imageUrl"] #"image_url"
				])

		product_info["products"].append([
			product_id, #"product_id"
			produto, #"produto"
			product["link"], #"link"
			product["categoryId"], #"category_id"
			product["categories"][0], #"category_name"
			original_price, #"original_price"
			sale_price, #"sale_price"
			product["description"], #"description"
		])

		# Product Categories:
		for i in range(0,len(product["categories"])):
			product_info["product_categories"].append([
				product_id, #"product_id"
				produto, #"produto"
				product["categoriesIds"][i], #"category_id"
				product["categories"][i] #"category_name"
			])

	return product_info

if __name__ == '__main__':
	query = """
		SELECT DISTINCT
		e.produto
		FROM w_estoque_disponivel_sku e
		INNER JOIN produtos p on e.produto = p.produto
		WHERE
		p.grupo_produto != 'GIFTCARD' and
		e.estoque_disponivel > 0 and
		e.filial = 'E-COMMERCE'
		order by e.produto
	"""

	print('Connecting to database...', end='')
	dc = DatabaseConnection()
	print('Done!')

	print('Getting products from database...', end='')
	product_ref_ids = dc.select(query, dict_format=True, strip=True)
	print('Done!')

	print('Getting product info from vtex...', end='')

	product_info = {
		"product_categories":[],
		"product_items":[],
		"product_images":[],
		"products":[]
	}

	result = []
	# for x in product_ref_ids:
	# 	result.append(get_product_info(x))

	# Multi Threading:
	with Pool(20) as p:
		result = p.map(get_product_info, product_ref_ids)

	for result_thread in result:
		for key in product_info:
			product_info[key].extend(result_thread[key])

	print('Done!')

	print('Inserting into tables...')

	print('	bi_vtex_product_items')
	dc.execute('TRUNCATE TABLE bi_vtex_product_items;')
	dc.insert('bi_vtex_product_items',product_info["product_items"])

	print('	bi_vtex_products')
	dc.execute('TRUNCATE TABLE bi_vtex_products;')
	dc.insert('bi_vtex_products',product_info["products"])

	print('	bi_vtex_product_categories')
	dc.execute('TRUNCATE TABLE bi_vtex_product_categories;')
	dc.insert('bi_vtex_product_categories',product_info["product_categories"])

	print('	bi_vtex_product_item_images')
	dc.execute('TRUNCATE TABLE bi_vtex_product_item_images;')
	dc.insert('bi_vtex_product_item_images',product_info["product_images"])

	print('Done!')

	print('Transforming item_images into product_color_images...',end='')

	dc.execute('TRUNCATE TABLE bi_vtex_product_images')
	dc.execute("""
		INSERT INTO bi_vtex_product_images
		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY produto, cor_produto ORDER BY image_url) as numero
		FROM (
			SELECT DISTINCT
			pb.produto,
			pb.cor_produto,
			pc.desc_cor_produto as cor,
			pii.image_url
			FROM bi_vtex_product_item_images pii
			INNER JOIN produtos_barra pb on pb.codigo_barra = pii.ean
			INNER JOIN produto_cores pc on
				pc.produto = pb.produto and
				pb.cor_produto = pc.cor_produto
		) t;
	""")

	print('Done!')