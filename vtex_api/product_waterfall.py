import os, shutil, csv, requests, time, json
from bs4 import BeautifulSoup as Soup
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict, get_from_dict, make_dict, for_each_leaf
from shadow_vtex.vtex import try_to_request, post_to_webservice

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

def check_vtex(ean):
	check_result = {
		'product_id': None,

		'integrado': False,
		'imagem_vtex': False,
		'ativo': False,

		'erro': None,
	}

	soap_skuget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
	      <tem:StockKeepingUnitGetByEan>
	         <!--Optional:-->
	         <tem:EAN13>%s</tem:EAN13>
	      </tem:StockKeepingUnitGetByEan>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (ean)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitGetByEan", soap_skuget)
	if not soup:
		check_result['erro'] = 'erro ao pegar sku por ean'
		return check_result

	sku_id = soup.find('a:Id')

	if not sku_id:
		return check_result

	check_result['integrado'] = True
	check_result['product_id'] = soup.find('a:ProductId').text

	sku_id = sku_id.text

	soap_imageget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
	      <tem:ImageListByStockKeepingUnitId>
	         <!--Optional:-->
	         <tem:StockKeepingUnitId>%s</tem:StockKeepingUnitId>
	         <!--Optional:-->
	      </tem:ImageListByStockKeepingUnitId>
	   </soapenv:Body>
	</soapenv:Envelope>""" % sku_id

	soup = post_to_webservice("http://tempuri.org/IService/ImageListByStockKeepingUnitId", soap_imageget)
	if not soup:
		check_result['erro'] = 'erro ao verificar imagem no vtex'
		return check_result

	image_test_name = soup.find('a:FileLocation')

	if not image_test_name:
		return check_result

	check_result['imagem_vtex'] = True

	soap_skuget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitGetByEan>
			 <!--Optional:-->
			 <tem:EAN13>%s</tem:EAN13>
		  </tem:StockKeepingUnitGetByEan>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (ean)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitGetByEan", soap_skuget)
	if not soup:
		check_result['erro'] = ('erro ao verificar se ativo')
		return check_result

	is_active_node = soup.find('a:IsActive')
	if not is_active_node:
		check_result['erro'] = ('erro ao verificar se ativo - mensagem faltando')
		return check_result

	if is_active_node.text != 'true':
		return check_result

	check_result['ativo'] = True

	return check_result

def check_stock_and_online(product_info):
	check_result = {
		'estoque': 0,
		'online': False,
	}

	produto_cor = product_info['produto_cor_code']

	url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pub/products/search"
	params = {"fq":"alternateIds_RefId:%s" % product_info['prod_code']}
	response = try_to_request("GET", url, headers=api_connection_config, params=params)
	json_response = json.loads(response.text)
	for product in json_response:
		product_id = product["productId"]
		produto = product["productReference"]
		for item in product["items"]:
			if produto_cor in item['ean']:
				check_result['estoque'] += item["sellers"][0]["commertialOffer"]["AvailableQuantity"]
				check_result['online'] = True

	return check_result

def waterfall(product_info):
	check_result = {
		'produto': product_info['prod_code'],
		'cor_produto': product_info['cod_color'],

		'has_ean': False,

		'integrado': False,
		'imagem_vtex': False,
		'imagem_hd': False,
		'ativo': False,

		'estoque': None,
		'online': False,

		'erro': None,
	}

	if not product_info['ean']:
		return check_result
	
	check_result['has_ean'] = True

	try:
		check_result.update(check_vtex(product_info['ean']))

		check_result.update(check_stock_and_online(product_info))
	except Exception as e:
		check_result['erro'] = str(e)

	return check_result

def search_directory(products_to_search):
	filename_to_product = {}
	already_found_images = set()
	found_products = {}
	paths = ["C:\\Users\\Felipe\\Projetos\\shadow\\fotos\\fotos_para_renomear"]

	for product in products_to_search:
		for n in range(1,5):
			try:
				filename = '%s.%s_0%s.jpg' % (product['produto'].replace(".",""), str(int(product['cor_produto'])), n)
				filename_to_product[filename] = product
			except Exception as e:
				pass

			filename = '%s.%s_0%s.jpg' % (product['produto'].replace(".",""), str(product['cor_produto']).zfill(2), n)
			filename_to_product[filename] = product

	for path in paths:
		for folder, subs, files in os.walk(path):
			for filename in files:
				if filename in filename_to_product and filename not in already_found_images:
					already_found_images.add(filename)

					filepath = os.path.abspath(os.path.join(folder, filename))
					# try:
					# 	shutil.copy(filepath,'..\\fotosC:\\Users\\Felipe\\Downloads\\mm\\fotos\\nakd\\')
					# except Exception as e:
					# 	print(e)
					# # print(filepath)

					product = filename_to_product[filename]
					set_in_dict(found_products, 1, [product['produto'], product['cor_produto']])

	return found_products

from multiprocessing import Pool
if __name__ == '__main__':
	start = time.time()

	dc = DatabaseConnection()

	# product_filter = "and p.griffe = 'NAKD'"
	product_filter = "e.estoque_disponivel > 0 and e.filial='e-commerce'"
	# product_filter = "(p.produto = '22.01.0007')"

	query = """
		SELECT
			p.produto as prod_code, 
			ps.COR_PRODUTO as cod_color, 
			MAX(ps.CODIGO_BARRA) as ean,
			MAX(LEFT(ps.CODIGO_BARRA, LEN(ps.CODIGO_BARRA) - LEN(ps.grade))) as produto_cor_code,
			SUM(e.estoque_disponivel) as stock
		from dbo.PRODUTOS p
		INNER JOIN dbo.PRODUTO_CORES pc on pc.produto = p.produto
		INNER JOIN dbo.PRODUTOS_BARRA ps on ps.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		LEFT JOIN w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA
		WHERE 1=1
			and (%s)
		group by p.produto, ps.COR_PRODUTO
		having SUM(e.estoque_disponivel) > 0
		;
	""" % product_filter

	products_to_check = dc.select(query, strip=True, dict_format=True)

	product_checks = []
	with Pool(10) as p:
		product_checks = p.map(waterfall, products_to_check)

	# for x in products_to_check:
	# 	product_checks.append(f(x))

	products_to_search = []
	for product_check in product_checks:
		product_check['imagem_hd'] = False

		if not product_check['imagem_vtex']:
			products_to_search.append(product_check)

	found_products = search_directory(products_to_search)
	product_check_dict = make_dict(product_checks, None, ['produto', 'cor_produto'])
	try:
		for found_product in found_products:
			set_in_dict(product_check_dict, True, [found_product['produto'], found_product['cor_produto'], 'imagem_hd'])
	except Exception as e:
		pass

	dc.execute('TRUNCATE TABLE bi_vtex_product_waterfall;')
	dc.insert_dict('bi_vtex_product_waterfall', list(for_each_leaf(product_check_dict, depth=2, return_keys=False)))

	end = time.time()
	print(end-start)