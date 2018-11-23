from database.database_connection import DatabaseConnection
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup

product_fail = []
product_list = []

def post_to_webservice(soap_action, soap_message, retry=3):
	params["SOAPAction"] = soap_action
	request_type = soap_action.split('/')[-1]

	for i in range(0, retry):
		try:
			response = requests.post("http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl", auth=auth, headers=params, data=soap_message.encode(), timeout=10)

			if response.status_code == 200:
				print("%s %s" % (request_type, response.status_code))
				break
			else:
				print("%s %s" % (request_type, response.status_code))
				raise Exception()

		except Exception as e:

			if i == retry-1:
				print('desistindo')

				id_type = {}
				id_type['id'] = product_id
				id_type['error'] = request_type
				product_fail.append(id_type)

				return None

	return Soup(response.text, "xml")

# --------------------------------------------------------------------------------------------------------------------------------

webserviceURL = "http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl"

params = {
	"Content-Type": "text/xml",
	"Authorization": "Basic dnRleGFwcGtleS1tYXJjaWFtZWxsby1YTlpGVVg6SEpHVkdVUFVTTVpTRllJSFZQTEpQRkJaUFlCTkxDRkhSWVRUVVRQWlNZVFlDSFRJT1BUSktBQUJISEZIVENJUEdTQUhGT01CWkxSUk1DWEhGU1lXSlZXUlhSTE5PSUdQUERTSkhMRFpDUktaSklQRktZQkJETUZMVklLT0RaTlE=",
}

auth = ("vtexappkey-marciamello-XNZFUX","HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ")

query = """
SELECT
	v_item.product_id
from dbo.PRODUTOS_BARRA ps
inner join dbo.PRODUTOS p on p.produto = ps.produto
inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
inner join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
inner join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
where 1=1
	 and p.produto in ('22.12.0447','35.09.0981','77.99.2308','77.73.0354','77.61.0107','22.12.0621','77.61.0106','22.05.0553','77.61.0139','77.62.0023','22.05.0562','22.05.0565','77.61.0123','77.61.0105','77.61.0136','22.05.0543','22.07.0213','77.22.0243','77.99.2300','22.05.0554')
group by v_item.product_id
;"""


products_to_categorize = dc.select(query, trim=True)
# products_to_categorize = ['530212','531792','531834','531809','531799','531827','531795','531247','531798','531811','531810','529963','530774','531805','530796','530974','531564','530897','531763','531266','530951','531678','531301','531837','530952','531184','528024','531294','530993','530808','530740','530809','531297','530954','530960','530955','531287','531465','530981','531374','531775','530980','531067','531774','531199','530979']

# raise Exception(len(products_to_categorize))

for product_info in products_to_categorize:

	# product_id = product_info
	product_id = product_info['product_id']
	category_id = '1000226'
	print(product_id)
	soap_similarcategory = """
	<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	<soapenv:Header/>
	<soapenv:Body>
	  <tem:ProductSetSimilarCategory>
	     <!--Optional:-->
	     <tem:productId>%s</tem:productId>
	     <!--Optional:-->
	     <tem:categoryId>%s</tem:categoryId>
	  </tem:ProductSetSimilarCategory>
	</soapenv:Body>
	</soapenv:Envelope>""" % (product_id, category_id)

	soup = post_to_webservice("http://tempuri.org/IService/ProductSetSimilarCategory", soap_similarcategory)
	if not soup:
		continue
	if product_id not in product_list:
		product_list.append(product_id)

with open('similar_succeed.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";")
	for n in product_list:
		csv_file.writerow({n})

with open('similar_fail.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";")
	for n in product_fail:
		csv_file.writerow({n['id'], n['error']})