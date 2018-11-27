import os, shutil, csv, requests, time, json
from bs4 import BeautifulSoup as Soup
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict, get_from_dict, make_dict, for_each_leaf

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

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

def check_vtex(ean):
	check_result = {
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

	url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pub/products/search"
	params = {"fq":"alternateIds_RefId:%s" % product_info['prod_code']}
	response = try_to_request("GET", url, headers=api_connection_config, params=params)
	json_response = json.loads(response.text)
	for product in json_response:
		product_id = product["productId"]
		produto = product["productReference"]
		for item in product["items"]:
			check_result['estoque'] += item["sellers"][0]["commertialOffer"]["AvailableQuantity"]
			check_result['online'] = check_result['online'] or True

	return check_result

def f(product_info):
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


webserviceURL = "http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl"

params = {
	"Content-Type": "text/xml",
	"Authorization": "Basic dnRleGFwcGtleS1tYXJjaWFtZWxsby1YTlpGVVg6SEpHVkdVUFVTTVpTRllJSFZQTEpQRkJaUFlCTkxDRkhSWVRUVVRQWlNZVFlDSFRJT1BUSktBQUJISEZIVENJUEdTQUhGT01CWkxSUk1DWEhGU1lXSlZXUlhSTE5PSUdQUERTSkhMRFpDUktaSklQRktZQkJETUZMVklLT0RaTlE=",
}

auth = ("vtexappkey-marciamello-XNZFUX","HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ")
def post_to_webservice(soap_action, soap_message, retry=3):
	params["SOAPAction"] = soap_action
	request_type = soap_action.split('/')[-1]

	for i in range(0, retry):
		response = None
		try:
			response = requests.post("http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl", auth=auth, headers=params, data=soap_message.encode(), timeout=10)

			print("%s %s" % (request_type, response.status_code))
			if response.status_code == 200:
				break
			elif response.status_code == 429:
				import time
				time.sleep(15)
			else:
				raise Exception()

		except Exception as e:
			if i == retry-1:
				if response:
					print(response.text)

				return None

	return Soup(response.text, "xml")

def search_directory(products_to_search):
	filename_to_product = {}
	already_found_images = set()
	found_products = {}
	paths = ["C:\\Users\\victo\\git\\shadow\\fotos\\fotos_para_renomear"]

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
	# product_filter = """ and (
	# 	(p.produto = '35.01.0828' and pc.cor_produto = '260') OR
	# 	(p.produto = '28.06.0015' and pc.cor_produto = '491') OR
	# 	(p.produto = '35.04.0041' and pc.cor_produto = '176') OR
	# 	(p.produto = '35.04.0041' and pc.cor_produto = '105') OR
	# 	(p.produto = '35.02.0828' and pc.cor_produto = '105') OR
	# 	(p.produto = '35.02.0828' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.02.0828' and pc.cor_produto = '3') OR
	# 	(p.produto = '35.01.0859' and pc.cor_produto = '1') OR
	# 	(p.produto = '35.01.0858' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.01.0860' and pc.cor_produto = '263') OR
	# 	(p.produto = '35.01.0856' and pc.cor_produto = '1') OR
	# 	(p.produto = '35.01.0858' and pc.cor_produto = '1') OR
	# 	(p.produto = '01.02.1008' and pc.cor_produto = '2') OR
	# 	(p.produto = '01.02.1008' and pc.cor_produto = '105') OR
	# 	(p.produto = '01.02.1008' and pc.cor_produto = '482') OR
	# 	(p.produto = '35.02.0829' and pc.cor_produto = '3') OR
	# 	(p.produto = '35.02.0829' and pc.cor_produto = '105') OR
	# 	(p.produto = '35.02.0829' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.01.0825' and pc.cor_produto = '259') OR
	# 	(p.produto = '35.01.0857' and pc.cor_produto = '243') OR
	# 	(p.produto = '35.02.0830' and pc.cor_produto = '198') OR
	# 	(p.produto = '35.02.0835' and pc.cor_produto = '6') OR
	# 	(p.produto = '35.02.0835' and pc.cor_produto = '105') OR
	# 	(p.produto = '35.02.0835' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.02.0833' and pc.cor_produto = '1') OR
	# 	(p.produto = '35.02.0833' and pc.cor_produto = '3') OR
	# 	(p.produto = '35.02.0833' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.02.0834' and pc.cor_produto = '177') OR
	# 	(p.produto = '35.02.0834' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.02.0834' and pc.cor_produto = '17') OR
	# 	(p.produto = '41.01.0102' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.07.0269' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0266' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.07.0269' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.05.0472' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0267' and pc.cor_produto = '110') OR
	# 	(p.produto = '22.03.0243' and pc.cor_produto = '194') OR
	# 	(p.produto = '22.12.0591' and pc.cor_produto = '194') OR
	# 	(p.produto = '22.07.0267' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0260' and pc.cor_produto = '33') OR
	# 	(p.produto = '22.12.0594' and pc.cor_produto = '10') OR
	# 	(p.produto = '22.05.0505' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0486' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0261' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0266' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0526' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0108' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0594' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0263' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0268' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0110' and pc.cor_produto = '8') OR
	# 	(p.produto = '24.04.0536' and pc.cor_produto = '1') OR
	# 	(p.produto = '24.04.0535' and pc.cor_produto = '68') OR
	# 	(p.produto = '22.03.0243' and pc.cor_produto = '10') OR
	# 	(p.produto = '41.01.0109' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0103' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0104' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0109' and pc.cor_produto = '33') OR
	# 	(p.produto = '41.01.0101' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0110' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0106' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.07.0261' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.07.0260' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.14.0008' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.14.0008' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.14.0008' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.07.0263' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.12.0593' and pc.cor_produto = '176') OR
	# 	(p.produto = '41.01.0104' and pc.cor_produto = '33') OR
	# 	(p.produto = '22.05.0526' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0105' and pc.cor_produto = '8') OR
	# 	(p.produto = '41.01.0105' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0108' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0108' and pc.cor_produto = '8') OR
	# 	(p.produto = '41.01.0103' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0101' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0102' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0109' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0106' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0593' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0105' and pc.cor_produto = '105') OR
	# 	(p.produto = '41.01.0105' and pc.cor_produto = '2') OR
	# 	(p.produto = '24.04.0535' and pc.cor_produto = '122') OR
	# 	(p.produto = '24.04.0536' and pc.cor_produto = '5') OR
	# 	(p.produto = '24.04.0536' and pc.cor_produto = '117') OR
	# 	(p.produto = '22.07.0268' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.07.0266' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.07.0260' and pc.cor_produto = '25') OR
	# 	(p.produto = '22.03.0243' and pc.cor_produto = '2') OR
	# 	(p.produto = '41.01.0104' and pc.cor_produto = '8') OR
	# 	(p.produto = '22.07.0261' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0107' and pc.cor_produto = '8') OR
	# 	(p.produto = '41.01.0110' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0107' and pc.cor_produto = '1') OR
	# 	(p.produto = '41.01.0106' and pc.cor_produto = '33') OR
	# 	(p.produto = '41.01.0107' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.12.0591' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0267' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0505' and pc.cor_produto = '1') OR
	# 	(p.produto = '35.02.0836' and pc.cor_produto = '507') OR
	# 	(p.produto = '35.04.0049' and pc.cor_produto = '217') OR
	# 	(p.produto = '35.04.0049' and pc.cor_produto = '223') OR
	# 	(p.produto = '35.04.0049' and pc.cor_produto = '221') OR
	# 	(p.produto = '35.02.0836' and pc.cor_produto = '198') OR
	# 	(p.produto = '35.04.0048' and pc.cor_produto = '485') OR
	# 	(p.produto = '31.05.0015' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0562' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0579' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0579' and pc.cor_produto = '6') OR
	# 	(p.produto = '22.05.0579' and pc.cor_produto = '2') OR
	# 	(p.produto = '32.03.0370' and pc.cor_produto = '507') OR
	# 	(p.produto = '37.05.0029' and pc.cor_produto = '219') OR
	# 	(p.produto = '37.05.0029' and pc.cor_produto = '218') OR
	# 	(p.produto = '37.05.0029' and pc.cor_produto = '221') OR
	# 	(p.produto = '37.01.0018' and pc.cor_produto = '216') OR
	# 	(p.produto = '37.01.0018' and pc.cor_produto = '218') OR
	# 	(p.produto = '37.01.0018' and pc.cor_produto = '220') OR
	# 	(p.produto = '22.03.0247' and pc.cor_produto = '451') OR
	# 	(p.produto = '22.03.0247' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.05.0577' and pc.cor_produto = '252') OR
	# 	(p.produto = '22.05.0574' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0541' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0573' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.05.0541' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0541' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0541' and pc.cor_produto = '4') OR
	# 	(p.produto = '22.05.0553' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0566' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.12.0621' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.05.0544' and pc.cor_produto = '155') OR
	# 	(p.produto = '22.05.0565' and pc.cor_produto = '505') OR
	# 	(p.produto = '22.05.0577' and pc.cor_produto = '507') OR
	# 	(p.produto = '22.07.0284' and pc.cor_produto = '7') OR
	# 	(p.produto = '22.02.0245' and pc.cor_produto = '6') OR
	# 	(p.produto = '22.05.0572' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.05.0559' and pc.cor_produto = '4') OR
	# 	(p.produto = '22.05.0570' and pc.cor_produto = '13') OR
	# 	(p.produto = '22.07.0281' and pc.cor_produto = '7') OR
	# 	(p.produto = '22.05.0580' and pc.cor_produto = '231') OR
	# 	(p.produto = '22.02.0245' and pc.cor_produto = '25') OR
	# 	(p.produto = '22.07.0276' and pc.cor_produto = '218') OR
	# 	(p.produto = '22.07.0280' and pc.cor_produto = '485') OR
	# 	(p.produto = '22.05.0556' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.05.0575' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.07.0285' and pc.cor_produto = '220') OR
	# 	(p.produto = '22.07.0285' and pc.cor_produto = '243') OR
	# 	(p.produto = '22.05.0560' and pc.cor_produto = '472') OR
	# 	(p.produto = '22.05.0556' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.05.0573' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.05.0562' and pc.cor_produto = '64') OR
	# 	(p.produto = '22.05.0559' and pc.cor_produto = '02') OR
	# 	(p.produto = '22.05.0560' and pc.cor_produto = '68') OR
	# 	(p.produto = '22.05.0544' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0569' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0557' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.05.0552' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0574' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0543' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0542' and pc.cor_produto = '13') OR
	# 	(p.produto = '22.05.0578' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0566' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0574' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0565' and pc.cor_produto = '229') OR
	# 	(p.produto = '22.05.0554' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.01.0007' and pc.cor_produto = '216') OR
	# 	(p.produto = '22.07.0279' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.05.0556' and pc.cor_produto = '218') OR
	# 	(p.produto = '22.07.0281' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.05.0578' and pc.cor_produto = '8') OR
	# 	(p.produto = '22.05.0565' and pc.cor_produto = '507') OR
	# 	(p.produto = '22.05.0551' and pc.cor_produto = '64') OR
	# 	(p.produto = '22.05.0551' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.07.0276' and pc.cor_produto = '216') OR
	# 	(p.produto = '22.07.0283' and pc.cor_produto = '6') OR
	# 	(p.produto = '22.05.0555' and pc.cor_produto = '216') OR
	# 	(p.produto = '22.05.0561' and pc.cor_produto = '20') OR
	# 	(p.produto = '22.03.0248' and pc.cor_produto = '493') OR
	# 	(p.produto = '22.05.0571' and pc.cor_produto = '20') OR
	# 	(p.produto = '22.05.0558' and pc.cor_produto = '7') OR
	# 	(p.produto = '22.07.0276' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.05.0577' and pc.cor_produto = '198') OR
	# 	(p.produto = '22.05.0568' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0563' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.02.0245' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0572' and pc.cor_produto = '223') OR
	# 	(p.produto = '22.03.0249' and pc.cor_produto = '244') OR
	# 	(p.produto = '22.02.0249' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0565' and pc.cor_produto = '238') OR
	# 	(p.produto = '22.03.0248' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.07.0283' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0543' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0574' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0545' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.05.0569' and pc.cor_produto = '05') OR
	# 	(p.produto = '22.05.0551' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0552' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0545' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.02.0245' and pc.cor_produto = '7') OR
	# 	(p.produto = '22.07.0280' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.05.0559' and pc.cor_produto = '7') OR
	# 	(p.produto = '22.05.0562' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0568' and pc.cor_produto = '6') OR
	# 	(p.produto = '22.05.0552' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0557' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.05.0561' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0564' and pc.cor_produto = '216') OR
	# 	(p.produto = '22.05.0571' and pc.cor_produto = '4') OR
	# 	(p.produto = '22.05.0572' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.07.0285' and pc.cor_produto = '218') OR
	# 	(p.produto = '22.05.0571' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0283' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0578' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0579' and pc.cor_produto = '94') OR
	# 	(p.produto = '22.05.0554' and pc.cor_produto = '218') OR
	# 	(p.produto = '22.07.0279' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.05.0544' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0543' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0277' and pc.cor_produto = '218') OR
	# 	(p.produto = '22.05.0558' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0277' and pc.cor_produto = '457') OR
	# 	(p.produto = '22.05.0561' and pc.cor_produto = '4') OR
	# 	(p.produto = '22.07.0278' and pc.cor_produto = '337') OR
	# 	(p.produto = '22.07.0278' and pc.cor_produto = '338') OR
	# 	(p.produto = '22.07.0281' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.07.0283' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.05.0557' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.05.0564' and pc.cor_produto = '64') OR
	# 	(p.produto = '22.05.0555' and pc.cor_produto = '217') OR
	# 	(p.produto = '22.07.0277' and pc.cor_produto = '219') OR
	# 	(p.produto = '22.05.0570' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0553' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.05.0562' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0555' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.07.0279' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.05.0575' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0554' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.07.0279' and pc.cor_produto = '216') OR
	# 	(p.produto = '22.07.0284' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0575' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.05.0574' and pc.cor_produto = '64') OR
	# 	(p.produto = '22.07.0276' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.05.0558' and pc.cor_produto = '5') OR
	# 	(p.produto = '22.02.0246' and pc.cor_produto = '231') OR
	# 	(p.produto = '35.01.0842' and pc.cor_produto = '7') OR
	# 	(p.produto = '35.01.0842' and pc.cor_produto = '68') OR
	# 	(p.produto = '22.05.0524' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.12.0615' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.12.0616' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.12.0616' and pc.cor_produto = '4') OR
	# 	(p.produto = '33.02.0232' and pc.cor_produto = '259') OR
	# 	(p.produto = '22.05.0525' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.05.0523' and pc.cor_produto = '4') OR
	# 	(p.produto = '33.02.0232' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.05.0525' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0616' and pc.cor_produto = '155') OR
	# 	(p.produto = '22.05.0523' and pc.cor_produto = '2') OR
	# 	(p.produto = '33.02.0232' and pc.cor_produto = '32') OR
	# 	(p.produto = '22.12.0615' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0616' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0615' and pc.cor_produto = '7') OR
	# 	(p.produto = '33.02.0232' and pc.cor_produto = '216') OR
	# 	(p.produto = '22.05.0524' and pc.cor_produto = '16') OR
	# 	(p.produto = '22.05.0523' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.05.0525' and pc.cor_produto = '105') OR
	# 	(p.produto = '33.02.0232' and pc.cor_produto = '457') OR
	# 	(p.produto = '22.05.0524' and pc.cor_produto = '32') OR
	# 	(p.produto = '22.12.0616' and pc.cor_produto = '176') OR
	# 	(p.produto = '33.02.0232' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.12.0600' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0597' and pc.cor_produto = '94') OR
	# 	(p.produto = '22.12.0598' and pc.cor_produto = '336') OR
	# 	(p.produto = '22.12.0597' and pc.cor_produto = '1') OR
	# 	(p.produto = '22.12.0598' and pc.cor_produto = '482') OR
	# 	(p.produto = '22.12.0601' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0594' and pc.cor_produto = '341') OR
	# 	(p.produto = '22.07.0250' and pc.cor_produto = '263') OR
	# 	(p.produto = '22.07.0268' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.12.0598' and pc.cor_produto = '1') OR
	# 	(p.produto = '32.03.0290' and pc.cor_produto = '1') OR
	# 	(p.produto = '32.02.0246' and pc.cor_produto = '1') OR
	# 	(p.produto = '35.02.0483' and pc.cor_produto = '2') OR
	# 	(p.produto = '31.02.0082' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.09.1208' and pc.cor_produto = '10') OR
	# 	(p.produto = '35.02.0766' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.04.0161' and pc.cor_produto = '155') OR
	# 	(p.produto = '34.01.0065' and pc.cor_produto = '7') OR
	# 	(p.produto = '35.02.0489' and pc.cor_produto = '105') OR
	# 	(p.produto = '35.09.0981' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.01.0692' and pc.cor_produto = '176') OR
	# 	(p.produto = '35.09.0929' and pc.cor_produto = '2') OR
	# 	(p.produto = '35.09.0957' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0258' and pc.cor_produto = '10') OR
	# 	(p.produto = '22.07.0213' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.07.0213' and pc.cor_produto = '122') OR
	# 	(p.produto = '22.12.0541' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.07.0252' and pc.cor_produto = '39') OR
	# 	(p.produto = '22.12.0611' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.07.0214' and pc.cor_produto = '6') OR
	# 	(p.produto = '22.15.0014' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.12.0580' and pc.cor_produto = '9') OR
	# 	(p.produto = '33.02.0154' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0611' and pc.cor_produto = '8') OR
	# 	(p.produto = '22.06.0550' and pc.cor_produto = '10') OR
	# 	(p.produto = '37.06.0014' and pc.cor_produto = '4') OR
	# 	(p.produto = '33.02.0194' and pc.cor_produto = '217') OR
	# 	(p.produto = '32.02.0216' and pc.cor_produto = '2') OR
	# 	(p.produto = '32.03.0290' and pc.cor_produto = '4') OR
	# 	(p.produto = '32.03.0297' and pc.cor_produto = '16') OR
	# 	(p.produto = '33.02.0153' and pc.cor_produto = '105') OR
	# 	(p.produto = '33.03.0027' and pc.cor_produto = '5') OR
	# 	(p.produto = '23.03.0027' and pc.cor_produto = '7') OR
	# 	(p.produto = '35.01.0602' and pc.cor_produto = '259') OR
	# 	(p.produto = '35.09.0612' and pc.cor_produto = '94') OR
	# 	(p.produto = '31.01.0106' and pc.cor_produto = '7') OR
	# 	(p.produto = '31.02.0089' and pc.cor_produto = '176') OR
	# 	(p.produto = '35.09.0702' and pc.cor_produto = '3') OR
	# 	(p.produto = '22.02.0192' and pc.cor_produto = '2') OR
	# 	(p.produto = '32.06.0051' and pc.cor_produto = '461') OR
	# 	(p.produto = '32.03.0297' and pc.cor_produto = '460') OR
	# 	(p.produto = '23.11.0238' and pc.cor_produto = '35') OR
	# 	(p.produto = '23.03.0027' and pc.cor_produto = '4') OR
	# 	(p.produto = '23.11.0170' and pc.cor_produto = '4') OR
	# 	(p.produto = '22.05.0577' and pc.cor_produto = '244') OR
	# 	(p.produto = '32.03.0293' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.07.0191' and pc.cor_produto = '94') OR
	# 	(p.produto = '32.07.0048' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.07.0213' and pc.cor_produto = '68') OR
	# 	(p.produto = '22.12.0443' and pc.cor_produto = '155') OR
	# 	(p.produto = '32.03.0328' and pc.cor_produto = '2') OR
	# 	(p.produto = '37.06.0014' and pc.cor_produto = '94') OR
	# 	(p.produto = '32.06.0052' and pc.cor_produto = '2') OR
	# 	(p.produto = '32.03.0293' and pc.cor_produto = '105') OR
	# 	(p.produto = '22.02.0192' and pc.cor_produto = '1') OR
	# 	(p.produto = '32.03.0294' and pc.cor_produto = '2') OR
	# 	(p.produto = '32.03.0291' and pc.cor_produto = '94') OR
	# 	(p.produto = '32.03.0291' and pc.cor_produto = '105') OR
	# 	(p.produto = '23.11.0206' and pc.cor_produto = '453') OR
	# 	(p.produto = '32.07.0020' and pc.cor_produto = '460') OR
	# 	(p.produto = '32.03.0292' and pc.cor_produto = '24') OR
	# 	(p.produto = '32.06.0040' and pc.cor_produto = '5') OR
	# 	(p.produto = '32.07.0082' and pc.cor_produto = '244') OR
	# 	(p.produto = '23.11.0234' and pc.cor_produto = '14') OR
	# 	(p.produto = '22.07.0192' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0447' and pc.cor_produto = '6') OR
	# 	(p.produto = '22.05.0513' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.12.0467' and pc.cor_produto = '94') OR
	# 	(p.produto = '22.02.0201' and pc.cor_produto = '2') OR
	# 	(p.produto = '22.11.0211' and pc.cor_produto = '94') OR
	# 	(p.produto = '22.03.0187' and pc.cor_produto = '221') OR
	# 	(p.produto = '22.12.0467' and pc.cor_produto = '25') OR
	# 	(p.produto = '21.01.0008' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.05.0517' and pc.cor_produto = '176') OR
	# 	(p.produto = '22.05.0517' and pc.cor_produto = '3') OR
	# 	(p.produto = '31.02.0070' and pc.cor_produto = '257') OR
	# 	(p.produto = '31.02.0070' and pc.cor_produto = '223') OR
	# 	(p.produto = '22.12.0395' and pc.cor_produto = '94') OR
	# 	(p.produto = '20.03.0034' and pc.cor_produto = '2')
	# )"""
	product_filter = 'e.estoque_disponivel > 0'
	# product_filter = "(p.produto = '41.01.0108' and pc.cor_produto='02')"

	query = """
		SELECT
			p.produto as prod_code, 
			ps.COR_PRODUTO as cod_color, 
			MAX(ps.CODIGO_BARRA) as ean,
			SUM(e.estoque_disponivel) as stock
		from dbo.PRODUTOS p
		INNER JOIN dbo.PRODUTO_CORES pc on pc.produto = p.produto
		INNER JOIN dbo.PRODUTOS_BARRA ps on ps.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		LEFT JOIN w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		WHERE 1=1
			and (%s)
		group by p.produto, ps.COR_PRODUTO
		;
	""" % product_filter

	products_to_check = dc.select(query, strip=True, dict_format=True)

	dc.execute('TRUNCATE TABLE bi_vtex_product_waterfall;')

	product_checks = []
	with Pool(10) as p:
		product_checks = p.map(f, products_to_check)

	products_to_search = []
	for product_check in product_checks:
		product_check['imagem_hd'] = False

		if not product_check['imagem_vtex']:
			products_to_search.append(product_check)

	found_products = search_directory(products_to_search)
	product_check_dict = make_dict(product_checks, None, ['produto', 'cor_produto'])
	for found_product in found_products:
		set_in_dict(product_check_dict, True, [found_product['produto'], found_product['cor_produto'], 'imagem_hd'])

	dc.insert_dict('bi_vtex_product_waterfall', list(for_each_leaf(product_check_dict, depth=2, return_keys=False)))

	end = time.time()
	print(end-start)