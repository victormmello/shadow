import os, shutil, csv, requests, time
from bs4 import BeautifulSoup as Soup
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict, get_from_dict

def f(product_info):
	def make_table_row(status):
		return [
			product_info['prod_code'],
			product_info['cod_color'],
			status,
		]

	ean = product_info['ean']

	# --------------------------------------------------------------------------------------------------------------------------------

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
		return make_table_row('erro ao pegar sku por ean')

	sku_id = soup.find('a:Id')

	if not sku_id:
		return make_table_row('sem integracao')

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
		return make_table_row('erro ao verificar imagem no vtex')

	image_test_name = soup.find('a:FileLocation')

	if not image_test_name:
		return make_table_row('sem foto')

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
		return make_table_row('erro ao verificar se ativo')

	if not soup.find('a:Id'):
		return make_table_row('error: soap_skuget - no ID %s' % ean)

	if soup.find('a:IsActive').text != 'true':
		return make_table_row('produto inativo')

	return make_table_row('ok')

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
				time.sleep(10)
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
				filename = '%s.%s_0%s.jpg' % (product[0].replace(".",""), str(int(product[1])), n)
				filename_to_product[filename] = product
			except Exception as e:
				pass

			filename = '%s.%s_0%s.jpg' % (product[0].replace(".",""), str(product[1]).zfill(2), n)
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
					set_in_dict(found_products, 1, [product[0], product[1]])

	return found_products
	
from multiprocessing import Pool
if __name__ == '__main__':
	start = time.time()

	dc = DatabaseConnection()

	# product_filter = "and p.griffe = 'NAKD'"
	product_filter = """ and (
		(p.produto = '35.01.0828' and pc.cor_produto = '260') OR
		(p.produto = '28.06.0015' and pc.cor_produto = '491') OR
		(p.produto = '35.04.0041' and pc.cor_produto = '176') OR
		(p.produto = '35.04.0041' and pc.cor_produto = '105') OR
		(p.produto = '35.02.0828' and pc.cor_produto = '105') OR
		(p.produto = '35.02.0828' and pc.cor_produto = '2') OR
		(p.produto = '35.02.0828' and pc.cor_produto = '3') OR
		(p.produto = '35.01.0859' and pc.cor_produto = '1') OR
		(p.produto = '35.01.0858' and pc.cor_produto = '2') OR
		(p.produto = '35.01.0860' and pc.cor_produto = '263') OR
		(p.produto = '35.01.0856' and pc.cor_produto = '1') OR
		(p.produto = '35.01.0858' and pc.cor_produto = '1') OR
		(p.produto = '01.02.1008' and pc.cor_produto = '2') OR
		(p.produto = '01.02.1008' and pc.cor_produto = '105') OR
		(p.produto = '01.02.1008' and pc.cor_produto = '482') OR
		(p.produto = '35.02.0829' and pc.cor_produto = '3') OR
		(p.produto = '35.02.0829' and pc.cor_produto = '105') OR
		(p.produto = '35.02.0829' and pc.cor_produto = '2') OR
		(p.produto = '35.01.0825' and pc.cor_produto = '259') OR
		(p.produto = '35.01.0857' and pc.cor_produto = '243') OR
		(p.produto = '35.02.0830' and pc.cor_produto = '198') OR
		(p.produto = '35.02.0835' and pc.cor_produto = '6') OR
		(p.produto = '35.02.0835' and pc.cor_produto = '105') OR
		(p.produto = '35.02.0835' and pc.cor_produto = '2') OR
		(p.produto = '35.02.0833' and pc.cor_produto = '1') OR
		(p.produto = '35.02.0833' and pc.cor_produto = '3') OR
		(p.produto = '35.02.0833' and pc.cor_produto = '2') OR
		(p.produto = '35.02.0834' and pc.cor_produto = '177') OR
		(p.produto = '35.02.0834' and pc.cor_produto = '2') OR
		(p.produto = '35.02.0834' and pc.cor_produto = '17') OR
		(p.produto = '41.01.0102' and pc.cor_produto = '1') OR
		(p.produto = '22.07.0269' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0266' and pc.cor_produto = '176') OR
		(p.produto = '22.07.0269' and pc.cor_produto = '176') OR
		(p.produto = '22.05.0472' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0267' and pc.cor_produto = '110') OR
		(p.produto = '22.03.0243' and pc.cor_produto = '194') OR
		(p.produto = '22.12.0591' and pc.cor_produto = '194') OR
		(p.produto = '22.07.0267' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0260' and pc.cor_produto = '33') OR
		(p.produto = '22.12.0594' and pc.cor_produto = '10') OR
		(p.produto = '22.05.0505' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0486' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0261' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0266' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0526' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0108' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0594' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0263' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0268' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0110' and pc.cor_produto = '8') OR
		(p.produto = '24.04.0536' and pc.cor_produto = '1') OR
		(p.produto = '24.04.0535' and pc.cor_produto = '68') OR
		(p.produto = '22.03.0243' and pc.cor_produto = '10') OR
		(p.produto = '41.01.0109' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0103' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0104' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0109' and pc.cor_produto = '33') OR
		(p.produto = '41.01.0101' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0110' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0106' and pc.cor_produto = '1') OR
		(p.produto = '22.07.0261' and pc.cor_produto = '105') OR
		(p.produto = '22.07.0260' and pc.cor_produto = '2') OR
		(p.produto = '22.14.0008' and pc.cor_produto = '2') OR
		(p.produto = '22.14.0008' and pc.cor_produto = '1') OR
		(p.produto = '22.14.0008' and pc.cor_produto = '3') OR
		(p.produto = '22.07.0263' and pc.cor_produto = '176') OR
		(p.produto = '22.12.0593' and pc.cor_produto = '176') OR
		(p.produto = '41.01.0104' and pc.cor_produto = '33') OR
		(p.produto = '22.05.0526' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0105' and pc.cor_produto = '8') OR
		(p.produto = '41.01.0105' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0108' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0108' and pc.cor_produto = '8') OR
		(p.produto = '41.01.0103' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0101' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0102' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0109' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0106' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0593' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0105' and pc.cor_produto = '105') OR
		(p.produto = '41.01.0105' and pc.cor_produto = '2') OR
		(p.produto = '24.04.0535' and pc.cor_produto = '122') OR
		(p.produto = '24.04.0536' and pc.cor_produto = '5') OR
		(p.produto = '24.04.0536' and pc.cor_produto = '117') OR
		(p.produto = '22.07.0268' and pc.cor_produto = '105') OR
		(p.produto = '22.07.0266' and pc.cor_produto = '5') OR
		(p.produto = '22.07.0260' and pc.cor_produto = '25') OR
		(p.produto = '22.03.0243' and pc.cor_produto = '2') OR
		(p.produto = '41.01.0104' and pc.cor_produto = '8') OR
		(p.produto = '22.07.0261' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0107' and pc.cor_produto = '8') OR
		(p.produto = '41.01.0110' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0107' and pc.cor_produto = '1') OR
		(p.produto = '41.01.0106' and pc.cor_produto = '33') OR
		(p.produto = '41.01.0107' and pc.cor_produto = '105') OR
		(p.produto = '22.12.0591' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0267' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0505' and pc.cor_produto = '1') OR
		(p.produto = '35.02.0836' and pc.cor_produto = '507') OR
		(p.produto = '35.04.0049' and pc.cor_produto = '217') OR
		(p.produto = '35.04.0049' and pc.cor_produto = '223') OR
		(p.produto = '35.04.0049' and pc.cor_produto = '221') OR
		(p.produto = '35.02.0836' and pc.cor_produto = '198') OR
		(p.produto = '35.04.0048' and pc.cor_produto = '485') OR
		(p.produto = '31.05.0015' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0562' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0579' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0579' and pc.cor_produto = '6') OR
		(p.produto = '22.05.0579' and pc.cor_produto = '2') OR
		(p.produto = '32.03.0370' and pc.cor_produto = '507') OR
		(p.produto = '37.05.0029' and pc.cor_produto = '219') OR
		(p.produto = '37.05.0029' and pc.cor_produto = '218') OR
		(p.produto = '37.05.0029' and pc.cor_produto = '221') OR
		(p.produto = '37.01.0018' and pc.cor_produto = '216') OR
		(p.produto = '37.01.0018' and pc.cor_produto = '218') OR
		(p.produto = '37.01.0018' and pc.cor_produto = '220') OR
		(p.produto = '22.03.0247' and pc.cor_produto = '451') OR
		(p.produto = '22.03.0247' and pc.cor_produto = '221') OR
		(p.produto = '22.05.0577' and pc.cor_produto = '252') OR
		(p.produto = '22.05.0574' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0541' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0573' and pc.cor_produto = '217') OR
		(p.produto = '22.05.0541' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0541' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0541' and pc.cor_produto = '4') OR
		(p.produto = '22.05.0553' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0566' and pc.cor_produto = '1') OR
		(p.produto = '22.12.0621' and pc.cor_produto = '217') OR
		(p.produto = '22.05.0544' and pc.cor_produto = '155') OR
		(p.produto = '22.05.0565' and pc.cor_produto = '505') OR
		(p.produto = '22.05.0577' and pc.cor_produto = '507') OR
		(p.produto = '22.07.0284' and pc.cor_produto = '7') OR
		(p.produto = '22.02.0245' and pc.cor_produto = '6') OR
		(p.produto = '22.05.0572' and pc.cor_produto = '221') OR
		(p.produto = '22.05.0559' and pc.cor_produto = '4') OR
		(p.produto = '22.05.0570' and pc.cor_produto = '13') OR
		(p.produto = '22.07.0281' and pc.cor_produto = '7') OR
		(p.produto = '22.05.0580' and pc.cor_produto = '231') OR
		(p.produto = '22.02.0245' and pc.cor_produto = '25') OR
		(p.produto = '22.07.0276' and pc.cor_produto = '218') OR
		(p.produto = '22.07.0280' and pc.cor_produto = '485') OR
		(p.produto = '22.05.0556' and pc.cor_produto = '219') OR
		(p.produto = '22.05.0575' and pc.cor_produto = '176') OR
		(p.produto = '22.07.0285' and pc.cor_produto = '505') OR
		(p.produto = '22.07.0285' and pc.cor_produto = '243') OR
		(p.produto = '22.05.0560' and pc.cor_produto = '472') OR
		(p.produto = '22.05.0556' and pc.cor_produto = '221') OR
		(p.produto = '22.05.0573' and pc.cor_produto = '219') OR
		(p.produto = '22.05.0562' and pc.cor_produto = '64') OR
		(p.produto = '22.05.0559' and pc.cor_produto = '64') OR
		(p.produto = '22.05.0560' and pc.cor_produto = '68') OR
		(p.produto = '22.05.0544' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0569' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0557' and pc.cor_produto = '219') OR
		(p.produto = '22.05.0552' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0574' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0543' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0542' and pc.cor_produto = '20') OR
		(p.produto = '22.05.0578' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0566' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0574' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0565' and pc.cor_produto = '229') OR
		(p.produto = '22.05.0554' and pc.cor_produto = '219') OR
		(p.produto = '22.01.0007' and pc.cor_produto = '216') OR
		(p.produto = '22.07.0279' and pc.cor_produto = '219') OR
		(p.produto = '22.05.0556' and pc.cor_produto = '218') OR
		(p.produto = '22.07.0281' and pc.cor_produto = '1') OR
		(p.produto = '22.05.0578' and pc.cor_produto = '8') OR
		(p.produto = '22.05.0565' and pc.cor_produto = '507') OR
		(p.produto = '22.05.0551' and pc.cor_produto = '64') OR
		(p.produto = '22.05.0551' and pc.cor_produto = '3') OR
		(p.produto = '22.07.0276' and pc.cor_produto = '216') OR
		(p.produto = '22.07.0283' and pc.cor_produto = '6') OR
		(p.produto = '22.05.0555' and pc.cor_produto = '216') OR
		(p.produto = '22.05.0561' and pc.cor_produto = '20') OR
		(p.produto = '22.03.0248' and pc.cor_produto = '493') OR
		(p.produto = '22.05.0571' and pc.cor_produto = '20') OR
		(p.produto = '22.05.0558' and pc.cor_produto = '7') OR
		(p.produto = '22.07.0276' and pc.cor_produto = '219') OR
		(p.produto = '22.05.0577' and pc.cor_produto = '198') OR
		(p.produto = '22.05.0568' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0563' and pc.cor_produto = '219') OR
		(p.produto = '22.02.0245' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0572' and pc.cor_produto = '223') OR
		(p.produto = '22.03.0249' and pc.cor_produto = '244') OR
		(p.produto = '22.02.0249' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0565' and pc.cor_produto = '238') OR
		(p.produto = '22.03.0248' and pc.cor_produto = '105') OR
		(p.produto = '22.07.0283' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0543' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0574' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0545' and pc.cor_produto = '217') OR
		(p.produto = '22.05.0569' and pc.cor_produto = '105') OR
		(p.produto = '22.05.0551' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0552' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0545' and pc.cor_produto = '221') OR
		(p.produto = '22.02.0245' and pc.cor_produto = '7') OR
		(p.produto = '22.07.0280' and pc.cor_produto = '217') OR
		(p.produto = '22.05.0559' and pc.cor_produto = '7') OR
		(p.produto = '22.05.0562' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0568' and pc.cor_produto = '6') OR
		(p.produto = '22.05.0552' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0557' and pc.cor_produto = '217') OR
		(p.produto = '22.05.0561' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0564' and pc.cor_produto = '216') OR
		(p.produto = '22.05.0571' and pc.cor_produto = '4') OR
		(p.produto = '22.05.0572' and pc.cor_produto = '217') OR
		(p.produto = '22.07.0285' and pc.cor_produto = '231') OR
		(p.produto = '22.05.0571' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0283' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0578' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0579' and pc.cor_produto = '94') OR
		(p.produto = '22.05.0554' and pc.cor_produto = '218') OR
		(p.produto = '22.07.0279' and pc.cor_produto = '217') OR
		(p.produto = '22.05.0544' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0543' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0277' and pc.cor_produto = '218') OR
		(p.produto = '22.05.0558' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0277' and pc.cor_produto = '457') OR
		(p.produto = '22.05.0561' and pc.cor_produto = '4') OR
		(p.produto = '22.07.0278' and pc.cor_produto = '337') OR
		(p.produto = '22.07.0278' and pc.cor_produto = '338') OR
		(p.produto = '22.07.0281' and pc.cor_produto = '5') OR
		(p.produto = '22.07.0283' and pc.cor_produto = '3') OR
		(p.produto = '22.05.0557' and pc.cor_produto = '221') OR
		(p.produto = '22.05.0564' and pc.cor_produto = '452') OR
		(p.produto = '22.05.0555' and pc.cor_produto = '217') OR
		(p.produto = '22.07.0277' and pc.cor_produto = '219') OR
		(p.produto = '22.05.0570' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0553' and pc.cor_produto = '5') OR
		(p.produto = '22.05.0562' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0555' and pc.cor_produto = '221') OR
		(p.produto = '22.07.0279' and pc.cor_produto = '221') OR
		(p.produto = '22.05.0575' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0554' and pc.cor_produto = '221') OR
		(p.produto = '22.07.0279' and pc.cor_produto = '216') OR
		(p.produto = '22.07.0284' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0575' and pc.cor_produto = '105') OR
		(p.produto = '22.05.0574' and pc.cor_produto = '64') OR
		(p.produto = '22.07.0276' and pc.cor_produto = '221') OR
		(p.produto = '22.05.0558' and pc.cor_produto = '5') OR
		(p.produto = '22.02.0246' and pc.cor_produto = '231') OR
		(p.produto = '35.01.0842' and pc.cor_produto = '7') OR
		(p.produto = '35.01.0842' and pc.cor_produto = '68') OR
		(p.produto = '22.05.0524' and pc.cor_produto = '176') OR
		(p.produto = '22.12.0615' and pc.cor_produto = '176') OR
		(p.produto = '22.12.0616' and pc.cor_produto = '105') OR
		(p.produto = '22.12.0616' and pc.cor_produto = '4') OR
		(p.produto = '33.02.0232' and pc.cor_produto = '259') OR
		(p.produto = '22.05.0525' and pc.cor_produto = '176') OR
		(p.produto = '22.05.0523' and pc.cor_produto = '4') OR
		(p.produto = '33.02.0232' and pc.cor_produto = '2') OR
		(p.produto = '22.05.0525' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0616' and pc.cor_produto = '155') OR
		(p.produto = '22.05.0523' and pc.cor_produto = '2') OR
		(p.produto = '33.02.0232' and pc.cor_produto = '32') OR
		(p.produto = '22.12.0615' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0616' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0615' and pc.cor_produto = '7') OR
		(p.produto = '33.02.0232' and pc.cor_produto = '216') OR
		(p.produto = '22.05.0524' and pc.cor_produto = '16') OR
		(p.produto = '22.05.0523' and pc.cor_produto = '176') OR
		(p.produto = '22.05.0525' and pc.cor_produto = '105') OR
		(p.produto = '33.02.0232' and pc.cor_produto = '457') OR
		(p.produto = '22.05.0524' and pc.cor_produto = '32') OR
		(p.produto = '22.12.0616' and pc.cor_produto = '176') OR
		(p.produto = '33.02.0232' and pc.cor_produto = '105') OR
		(p.produto = '22.12.0600' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0597' and pc.cor_produto = '94') OR
		(p.produto = '22.12.0598' and pc.cor_produto = '336') OR
		(p.produto = '22.12.0597' and pc.cor_produto = '1') OR
		(p.produto = '22.12.0598' and pc.cor_produto = '482') OR
		(p.produto = '22.12.0601' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0594' and pc.cor_produto = '341') OR
		(p.produto = '22.07.0250' and pc.cor_produto = '263') OR
		(p.produto = '22.07.0268' and pc.cor_produto = '176') OR
		(p.produto = '22.12.0598' and pc.cor_produto = '1') OR
		(p.produto = '32.03.0290' and pc.cor_produto = '1') OR
		(p.produto = '32.02.0246' and pc.cor_produto = '1') OR
		(p.produto = '35.02.0483' and pc.cor_produto = '2') OR
		(p.produto = '31.02.0082' and pc.cor_produto = '2') OR
		(p.produto = '35.09.1208' and pc.cor_produto = '10') OR
		(p.produto = '35.02.0766' and pc.cor_produto = '2') OR
		(p.produto = '22.04.0161' and pc.cor_produto = '155') OR
		(p.produto = '34.01.0065' and pc.cor_produto = '7') OR
		(p.produto = '35.02.0489' and pc.cor_produto = '105') OR
		(p.produto = '35.09.0981' and pc.cor_produto = '2') OR
		(p.produto = '35.01.0692' and pc.cor_produto = '176') OR
		(p.produto = '35.09.0929' and pc.cor_produto = '2') OR
		(p.produto = '35.09.0957' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0258' and pc.cor_produto = '10') OR
		(p.produto = '22.07.0213' and pc.cor_produto = '105') OR
		(p.produto = '22.07.0213' and pc.cor_produto = '122') OR
		(p.produto = '22.12.0541' and pc.cor_produto = '105') OR
		(p.produto = '22.07.0252' and pc.cor_produto = '39') OR
		(p.produto = '22.12.0611' and pc.cor_produto = '2') OR
		(p.produto = '22.07.0214' and pc.cor_produto = '6') OR
		(p.produto = '22.15.0014' and pc.cor_produto = '176') OR
		(p.produto = '22.12.0580' and pc.cor_produto = '9') OR
		(p.produto = '33.02.0154' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0611' and pc.cor_produto = '8') OR
		(p.produto = '22.06.0550' and pc.cor_produto = '10') OR
		(p.produto = '37.06.0014' and pc.cor_produto = '4') OR
		(p.produto = '33.02.0194' and pc.cor_produto = '217') OR
		(p.produto = '32.02.0216' and pc.cor_produto = '2') OR
		(p.produto = '32.03.0290' and pc.cor_produto = '4') OR
		(p.produto = '32.03.0297' and pc.cor_produto = '16') OR
		(p.produto = '33.02.0153' and pc.cor_produto = '105') OR
		(p.produto = '33.03.0027' and pc.cor_produto = '5') OR
		(p.produto = '23.03.0027' and pc.cor_produto = '7') OR
		(p.produto = '35.01.0602' and pc.cor_produto = '259') OR
		(p.produto = '35.09.0612' and pc.cor_produto = '94') OR
		(p.produto = '31.01.0106' and pc.cor_produto = '7') OR
		(p.produto = '31.02.0089' and pc.cor_produto = '176') OR
		(p.produto = '35.09.0702' and pc.cor_produto = '3') OR
		(p.produto = '22.02.0192' and pc.cor_produto = '2') OR
		(p.produto = '32.06.0051' and pc.cor_produto = '461') OR
		(p.produto = '32.03.0297' and pc.cor_produto = '460') OR
		(p.produto = '23.11.0238' and pc.cor_produto = '35') OR
		(p.produto = '23.03.0027' and pc.cor_produto = '4') OR
		(p.produto = '23.11.0170' and pc.cor_produto = '4') OR
		(p.produto = '22.05.0577' and pc.cor_produto = '244') OR
		(p.produto = '32.03.0293' and pc.cor_produto = '176') OR
		(p.produto = '22.07.0191' and pc.cor_produto = '94') OR
		(p.produto = '32.07.0048' and pc.cor_produto = '176') OR
		(p.produto = '22.07.0213' and pc.cor_produto = '68') OR
		(p.produto = '22.12.0443' and pc.cor_produto = '155') OR
		(p.produto = '32.03.0328' and pc.cor_produto = '2') OR
		(p.produto = '37.06.0014' and pc.cor_produto = '94') OR
		(p.produto = '32.06.0052' and pc.cor_produto = '2') OR
		(p.produto = '32.03.0293' and pc.cor_produto = '105') OR
		(p.produto = '22.02.0192' and pc.cor_produto = '1') OR
		(p.produto = '32.03.0294' and pc.cor_produto = '2') OR
		(p.produto = '32.03.0291' and pc.cor_produto = '94') OR
		(p.produto = '32.03.0291' and pc.cor_produto = '105') OR
		(p.produto = '23.11.0206' and pc.cor_produto = '453') OR
		(p.produto = '32.07.0020' and pc.cor_produto = '460') OR
		(p.produto = '32.03.0292' and pc.cor_produto = '24') OR
		(p.produto = '32.06.0040' and pc.cor_produto = '5') OR
		(p.produto = '32.07.0082' and pc.cor_produto = '244') OR
		(p.produto = '23.11.0234' and pc.cor_produto = '14') OR
		(p.produto = '22.07.0192' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0447' and pc.cor_produto = '6') OR
		(p.produto = '22.05.0513' and pc.cor_produto = '2') OR
		(p.produto = '22.12.0467' and pc.cor_produto = '94') OR
		(p.produto = '22.02.0201' and pc.cor_produto = '2') OR
		(p.produto = '22.11.0211' and pc.cor_produto = '94') OR
		(p.produto = '22.03.0187' and pc.cor_produto = '221') OR
		(p.produto = '22.12.0467' and pc.cor_produto = '25') OR
		(p.produto = '21.01.0008' and pc.cor_produto = '176') OR
		(p.produto = '22.05.0517' and pc.cor_produto = '176') OR
		(p.produto = '22.05.0517' and pc.cor_produto = '3') OR
		(p.produto = '31.02.0070' and pc.cor_produto = '257') OR
		(p.produto = '31.02.0070' and pc.cor_produto = '223') OR
		(p.produto = '22.12.0395' and pc.cor_produto = '94') OR
		(p.produto = '20.03.0034' and pc.cor_produto = '2')
	)"""
	
	products_without_ean = dc.select("""
		SELECT 
			p.produto, 
			pc.cor_produto
		FROM dbo.PRODUTOS p
		INNER JOIN dbo.PRODUTO_CORES pc on pc.produto = p.produto
		LEFT JOIN dbo.PRODUTOS_BARRA ps on ps.produto = pc.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		WHERE 1=1
			and ps.codigo_barra is null
			-- and p.produto in ('35.01.0596','35.01.0672','35.01.0820','35.01.0845','35.01.0846','35.01.0852','35.01.0597','35.01.0599','35.01.0599','35.01.0593','35.04.0027','35.02.0614','35.01.0734','35.01.0686','35.01.0596','35.01.0686','35.01.0748','35.01.0748','35.01.0556','35.01.0636','35.01.0636','35.01.0727','35.01.0737','35.01.0737','35.01.0737','35.01.0732','35.02.0730','35.07.0034','35.02.0786','35.09.0702')
			%s
	""" % product_filter, strip=True, dict_format=True)

	products_without_ean_insert = []
	for product in products_without_ean:
		products_without_ean_insert.append([
			product['produto'],
			product['cor_produto'],
			'sem cod. barras',
		])

	dc.execute('TRUNCATE TABLE bi_vtex_product_waterfall;')
	dc.insert('bi_vtex_product_waterfall', products_without_ean_insert, print_only=False)

	query = """
		SELECT
			p.produto as prod_code, 
			ps.COR_PRODUTO as cod_color, 
			MAX(ps.CODIGO_BARRA) as ean
		from dbo.PRODUTOS_BARRA ps
		inner join dbo.PRODUTOS p on p.produto = ps.produto
		inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		left join dbo.bi_vtex_product_images vpc on vpc.produto = p.produto and vpc.cor_produto = pc.cor_produto
		where 1=1
		 	--and p.DATA_REPOSICAO > '2018-01-01'
		 	-- and vpc.produto is null
		 	-- and pc.DESC_COR_PRODUTO NOT LIKE '%%(CANCELADO)%%'
			-- and color.vtex_color is not null
			-- and p.produto in ('35.01.0596','35.01.0672','35.01.0820','35.01.0845','35.01.0846','35.01.0852','35.01.0597','35.01.0599','35.01.0599','35.01.0593','35.04.0027','35.02.0614','35.01.0734','35.01.0686','35.01.0596','35.01.0686','35.01.0748','35.01.0748','35.01.0556','35.01.0636','35.01.0636','35.01.0727','35.01.0737','35.01.0737','35.01.0737','35.01.0732','35.02.0730','35.07.0034','35.02.0786','35.09.0702')
			%s
			-- and pc.COR_PRODUTO in ('256')
			-- and e.estoque_disponivel > 0
		group by p.produto, ps.COR_PRODUTO
		;
	""" % product_filter


	# query = """
	# 		SELECT
	# 		p.produto as prod_code,
	# 		e.cor_produto as cod_color,
	# 		max(e.codigo_barra) as ean
	# 		FROM w_estoque_disponivel_sku e
	# 		INNER JOIN produtos_barra pb on e.codigo_barra = pb.codigo_barra
	# 		INNER JOIN produtos p on e.produto = p.produto
	# 		INNER JOIN produto_cores pc on pc.produto = e.produto and pc.cor_produto = e.cor_produto
	# 		INNER JOIN produtos_precos c on
	# 			e.produto = c.produto and
	# 			c.codigo_tab_preco = 02 -- 02 custo
	# 		LEFT JOIN bi_disponibilidade_produto_cor dpc on
	# 			dpc.produto = e.produto and
	# 			dpc.cor_produto = e.cor_produto
	# 		LEFT JOIN bi_vtex_product_items vpi on
	# 			vpi.ean = e.codigo_barra
	# 		WHERE
	# 		p.grupo_produto != 'GIFTCARD' and
	# 		c.codigo_tab_preco = 02 and
	# 		e.estoque_disponivel > 0 and
	# 		e.filial = 'E-COMMERCE' and
	# 		vpi.ean is null
	# 		GROUP BY
	# 		p.produto,
	# 		e.cor_produto
	# 		HAVING
	# 		CASE WHEN MAX(CASE WHEN pb.grade IN ('P','M') THEN 1 ELSE 0 END) = 1 and COUNT(DISTINCT e.codigo_barra) > 1 AND MAX(dpc.disponibilidade_filial) > 0.33 and SUM(e.estoque_disponivel * c.preco1) > 100 THEN 1 ELSE 0 END = 1
	# """



	products_to_test = dc.select(query, strip=True, dict_format=True)

	products_to_search_photo = []

	integration_check = []
	with Pool(10) as p:
		integration_check = p.map(f, products_to_test)

	products_to_search_photo = [x for x in integration_check if x[2] == 'sem foto']
	skus_info = [x for x in integration_check if x[2] != 'sem foto']

	found_products = search_directory(integration_check)
	for product in products_to_search_photo:
		image_found = get_from_dict(found_products, [product[0], product[1]])
		if image_found:
			skus_info.append([
				product[0],
				product[1],
				'imagem encontrada',
			])
		else:
			skus_info.append([
				product[0],
				product[1],
				'sem foto',
			])

	dc.insert('bi_vtex_product_waterfall', skus_info, print_only=False)

	end = time.time()
	print(end-start)