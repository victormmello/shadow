from shadow_database import DatabaseConnection
import requests, json, dateutil.relativedelta
from datetime import datetime

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

# API DOCS > consultar https://documenter.getpostman.com/view/845/vtex-catalog-api/Hs44

# consulta SKU:
# sku_id = "570897"
# url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/%s" % sku_id
# url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
# params = {
# 	"categoryId":"1",
# 	"_from":"1",
# 	"_to":"10"
# }

# lista de marcas:
# url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/brand/list"

# consulta pedido:
# url = "https://marciamello.vtexcommercestable.com.br/api/oms/pvt/orders/?f_invoicedDate=invoicedDate:[%sT00:00:00.000Z TO %sT00:00:00.000Z]" % (
# 	datetime.strftime(datetime.now().replace(day=1) + dateutil.relativedelta.relativedelta(months=-1),'%Y-%m-%d'),
# 	datetime.strftime(datetime.now().replace(day=1),'%Y-%m-%d'),
# )
# url = "https://marciamello.vtexcommercestable.com.br/api/oms/pvt/orders/?f_invoicedDate=invoicedDate:[2018-09-15T00:00:00.000Z TO 2018-09-19T00:00:00.000Z]"

# url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pub/category/tree/1/"

url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pub/products/search"
params = {
	# "O":"OrderByNameASC"
	"O":"OrderByReleaseDateDESC"
}

product_categories = []
product_items = []
product_images = []
products = []

product_ids = {}

with open('catalog_output.csv', 'wb') as f:

	list_set = list()

	result_range = 50
	results_from = 0
	results_to = result_range-1
	response_status_code = 206

	c=1
	while response_status_code == 206:
		print(u"iteração: %d" % c, end='')
		c+=1
		params["_from"] = results_from
		params["_to"] = results_to
		print(" (%s -> %s)" % (results_from ,results_to))
		response = requests.request("GET", url, headers=api_connection_config, params=params)

		response_status_code = response.status_code

		print(u"   status code: %d" % response_status_code)

		if response_status_code in (200,206):
			results_from += result_range
			results_to += result_range

			json_response = json.loads(response.text)
			
			for product in json_response:
				product_id = product["productId"]
				produto = product["productReference"]

				# if product_id == '530414':
				# 	raise Exception(product)

				# Não considera um produto que apareceu 2x na resposta:
				if not product_id in product_ids:
					product_ids[product_id] = True
					original_price = 0
					sale_price = 0
					for item in product["items"]:
						item_original_price = item["sellers"][0]["commertialOffer"]["ListPrice"]
						item_sale_price = item["sellers"][0]["commertialOffer"]["Price"]
						original_price = max(original_price,item_original_price)
						sale_price = max(sale_price,item_sale_price)

						product_items.append([
							item["itemId"], #"item_id"
							item["ean"], #"ean"
							item["sellers"][0]["commertialOffer"]["AvailableQuantity"], #"stock_quantity"
							item["images"][0]["imageUrl"], #"image_url",
							product["productId"]
						])

						# if item["ean"] == '35020650485PP':
						# 	raise Exception(item)

						for image in item["images"]:
							product_images.append([
								item["ean"], #"ean"
								image["imageUrl"] #"image_url"
							])

					products.append([
						product_id, #"product_id"
						produto, #"produto"
						product["link"], #"link"
						product["categoryId"], #"category_id"
						product["categories"][0], #"category_name"
						original_price, #"original_price"
						sale_price #"sale_price"
					])

					# Product Categories:
					for i in range(0,len(product["categories"])):
						product_categories.append([
							product_id, #"product_id"
							produto, #"produto"
							product["categoriesIds"][i], #"category_id"
							product["categories"][i] #"category_name"
						])

print('Connecting to database...',end='')
dc = DatabaseConnection()
print('Done!')

print('Inserting into tables...')

print('	bi_vtex_product_items')
dc.execute('TRUNCATE TABLE bi_vtex_product_items;')
dc.insert('bi_vtex_product_items',product_items)

print('	bi_vtex_products')
dc.execute('TRUNCATE TABLE bi_vtex_products;')
dc.insert('bi_vtex_products',products)

print('	bi_vtex_product_categories')
dc.execute('TRUNCATE TABLE bi_vtex_product_categories;')
dc.insert('bi_vtex_product_categories',product_categories)

print('	bi_vtex_product_item_images')
dc.execute('TRUNCATE TABLE bi_vtex_product_item_images;')
dc.insert('bi_vtex_product_item_images',product_images)

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