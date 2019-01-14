import os, shutil, csv, requests, time
from bs4 import BeautifulSoup as Soup
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict, get_from_dict


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


category_ids = [
	(1000000, "Integracao"),
	(1000222, "Novidades"),
	(1000123, "Roupas"),
	(1000143, "Bermudas"),
	(1000144, "Blazers"),
	(1000145, "Body"),
	(1000146, "Cachequer"),
	(1000147, "Calças"),
	(1000148, "Camisas"),
	(1000150, "Casacos"),
	(1000151, "Chemises"),
	(1000152, "Coletes"),
	(1000153, "Jaquetas"),
	(1000155, "Macacões"),
	(1000156, "Regatas"),
	(1000157, "Saias"),
	(1000158, "Shorts"),
	(1000159, "Túnicas"),
	(1000160, "Vestidos"),
	(1000161, "Blusas"),
	(1000068, "Calçados"),
	(1000115, "Tênis"),
	(1000126, "Botas"),
	(1000138, "Sandálias"),
	(1000139, "Sapatilhas"),
	(1000225, "Sapatos"),
	(1000097, "Acessórios"),
	(1000057, "Braceletes"),
	(1000070, "Chapéu"),
	(1000073, "Brincos"),
	(1000098, "Colares"),
	(1000099, "Cintos"),
	(1000100, "Echarpes"),
	(1000102, "Perfumes"),
	(1000107, "Bolsas"),
	(1000124, "Lenços"),
	(1000125, "Pulseiras"),
	(1000127, "Anéis"),
	(1000140, "Xale"),
	(1000090, "Casa"),
	(1000091, "Decorações"),
	(1000092, "Potes e Garrafas"),
	(1000094, "Pet e Jardim"),
	(1000095, "Relógios"),
	(1000096, "Quadros"),
	(1000104, "Sale"),
	(1000035, "Regatas"),
	(1000036, "Macacões"),
	(1000037, "Chemises"),
	(1000040, "Jaquetas"),
	(1000042, "Coletes"),
	(1000043, "Jeans"),
	(1000053, "Tops"),
	(1000055, "Túnicas"),
	(1000065, "Body"),
	(1000067, "Praia"),
	(1000109, "Vestidos"),
	(1000110, "Blusas"),
	(1000111, "Camisas"),
	(1000112, "Camisetas"),
	(1000113, "Bermudas"),
	(1000114, "Saias"),
	(1000116, "Batas"),
	(1000117, "Blazers"),
	(1000118, "Casacos"),
	(1000119, "Calças"),
	(1000128, "Shorts"),
	(1000163, "Cachequer"),
	(1000191, "Acessórios"),
	(1000192, "Calçados"),
	(1000193, "Home"),
	(1000205, "Coleções"),
	(1000206, "Un Amour en Côte d'Azur"),
	(1000209, "Inverno 2017"),
	(1000214, "Portofino Mood"),
	(1000215, "Verão 2018"),
	(1000218, "Newport"),
	(1000219, "Giulietta"),
	(1000220, "Inverno 2018"),
	(1000194, "Tendências"),
	(1000195, "Floral print"),
	(1000197, "Ombro a mostra"),
	(1000198, "Basic chic"),
	(1000199, "Camisaria"),
	(1000200, "Denim"),
	(1000204, "Party"),
	(1000210, "Veludo"),
	(1000211, "Midi"),
	(1000212, "Geometric print"),
	(1000213, "Black lovers"),
	(1000216, "Verde Aruba"),
	(1000217, "Babados"),
	(1000221, "Tricot"),
	(1000044, "Look"),
	(1000226, "Todos"),
	(1000227, "NAKD"),
	(1000228, "Promoções"),
	(1000229, "Black Friday 3 por 99"),
	(1000230, "Black Friday 3 por 149"),
	(1000231, "Black Friday 3 por 199"),
	(1000232, "Black Friday 50 OFF"),
	(1000233, "Black Friday 60 OFF"),
	(1000234, "Black Friday 70 OFF"),
	(1000235, "Black Friday 80 OFF"),
	(1000236, "Black Friday Vestidos Festa"),
	(1000237, "Black Friday 33 50 66"),
]

for category_id, category_name in category_ids:
	category_skuget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
		   <soapenv:Header/>
		   <soapenv:Body>
		      <tem:CategoryGet>		
		         <!--Optional:-->
		         <tem:idCategory>%s</tem:idCategory>
		      </tem:CategoryGet>
		   </soapenv:Body>
		</soapenv:Envelope>""" % (category_id)

	soup = post_to_webservice("http://tempuri.org/IService/CategoryGet", category_skuget)
	if not soup:
		print('error: category_skuget %s' % EAN)
		continue
	
	is_active = soup.find('a:IsActive').text
	name = soup.find('a:Name').text
	cat_id = soup.find('a:Id').text

	if is_active != 'true':
		print("%s - %s: %s" % (category_id, name, is_active))
