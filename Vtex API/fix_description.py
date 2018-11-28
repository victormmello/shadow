from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool

# --------------------------------------------------------------------------------------------------------------------------------

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
		try:
			response = requests.post("http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl", auth=auth, headers=params, data=soap_message.encode(), timeout=10)

			print("%s %s" % (request_type, response.status_code))
			if response.status_code == 200:
				break
			elif response.status_code == 429:
				import time
				time.sleep(10)
				raise Exception()
			else:
				raise Exception()

		except Exception as e:
			if i == retry-1:
				print('desistindo')
				return None

	return Soup(response.text, "xml")

def f(product_info):
	# print(product_info)
	product_id = product_info['product_id']

	# --------------------------------------------------------------------------------------------------------------------------------

	soap_productget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:ProductGet>
			 <!--Optional:-->
			 <tem:idProduct>%s</tem:idProduct>
		  </tem:ProductGet>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (product_id)

	soup = post_to_webservice("http://tempuri.org/IService/ProductGet", soap_productget)
	if not soup:
		return 'error: soap_productget %s' % product_id

	product_name = soup.find('a:Name').text
	link_id = soup.find('a:LinkId').text
	product_refid = soup.find('a:RefId').text
	product_isactive = soup.find('a:IsActive').text
	brand_id = soup.find('a:BrandId').text
	category_id = soup.find('a:CategoryId').text
	department_id = soup.find('a:DepartmentId').text

	# --------------------------------------------------------------------------------------------------------------------------------

	soap_productupdate = """
		<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
		<soapenv:Header/>
		<soapenv:Body>
			<tem:ProductInsertUpdate>
				 <!--Optional:-->
				 <tem:productVO>
					<vtex:BrandId>%s</vtex:BrandId>
					<vtex:CategoryId>%s</vtex:CategoryId>
					<vtex:DepartmentId>%s</vtex:DepartmentId>
					<vtex:Description>%s</vtex:Description>
					<vtex:DescriptionShort>%s</vtex:DescriptionShort>
					<vtex:Id>%s</vtex:Id>
					<vtex:IsActive>true</vtex:IsActive>
					<vtex:IsVisible>true</vtex:IsVisible>
					<vtex:LinkId>%s</vtex:LinkId>
					<vtex:ListStoreId>
					   <arr:int>1</arr:int>
					   <arr:int>3</arr:int>
					</vtex:ListStoreId>
					<vtex:Name>%s</vtex:Name>
					<vtex:RefId>%s</vtex:RefId>
					<vtex:ShowWithoutStock>false</vtex:ShowWithoutStock>
				 </tem:productVO>
			  </tem:ProductInsertUpdate>
		   </soapenv:Body>
		</soapenv:Envelope>""" % (brand_id, category_id, department_id, product_name, product_name, product_id, link_id, product_name, product_refid)


	soup = post_to_webservice("http://tempuri.org/IService/ProductInsertUpdate", soap_productupdate)
	if not soup:
		return 'error: soap_productupdate %s' % product_id

if __name__ == '__main__':
	filter_str = "vp.produto = '22.05.0448'"

	query = """
		SELECT distinct
			vpi.product_id
		from dbo.bi_vtex_products vp
		inner join bi_vtex_product_items vpi on vpi.product_id = vp.product_id
		where 1=1
			-- and vpi.item_id = 571599
			-- and vpi.product_id = 530739
			-- and len(vp.description) = 0
			and (%s)
		;
	""" % filter_str
	products_to_fix = dc.select(query, strip=True, dict_format=True)
	# print(products_to_fix)


	errors = []
	# Rodar sem thread:
	for product_to_fix in products_to_fix:
		errors.append(f(product_to_fix))

	errors = [x for x in errors if x]
	print(errors)