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
	print(product_info['ean'])

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
		return 'error: soap_productget %s' % EAN

	product_name = soup.find('a:Name').text
	link_id = soup.find('a:LinkId').text
	product_refid = soup.find('a:RefId').text
	brand_id = soup.find('a:BrandId').text
	category_id = soup.find('a:CategoryId').text
	department_id = soup.find('a:DepartmentId').text

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
		</soapenv:Envelope>""" % (brand_id, category_id, department_id, product_id, link_id, product_name, product_refid)

	soup = post_to_webservice("http://tempuri.org/IService/ProductInsertUpdate", soap_productupdate)

	if not soup:
		return 'error: soap_productupdate %s' % EAN


	sku_costprice = soup.find('a:CostPrice').text
	sku_height = soup.find('a:Height').text
	sku_id = soup.find('a:Id').text
	# sku_isactive = soup.find('a:IsActive').text
	sku_isactive = 'false'
	sku_isavailable = soup.find('a:IsAvaiable').text
	sku_iskit = soup.find('a:IsKit').text
	sku_length = soup.find('a:Length').text
	sku_listprice = soup.find('a:ListPrice').text if float(soup.find('a:ListPrice').text) > 0 else list_price
	sku_modalid = soup.find('a:ModalId').text
	sku_name = soup.find('a:Name').text
	sku_price = soup.find('a:Price').text if float(soup.find('a:Price').text) > 0 else price
	# sku_weightkg = soup.find('a:WeightKg').text
	sku_width = soup.find('a:Width').text
	sku_cubicweight = soup.find('a:CubicWeight').text
	sku_weightkg = "300"

	product_id = soup.find('a:ProductId').text

	soap_skuupdate = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitInsertUpdate>
			 <tem:stockKeepingUnitVO>
				<vtex:CostPrice>%(sku_costprice)s</vtex:CostPrice>
				<vtex:CubicWeight>%(sku_cubicweight)s</vtex:CubicWeight>
				<vtex:Height>%(sku_height)s</vtex:Height>
				<vtex:Id>%(sku_id)s</vtex:Id>
				<vtex:IsActive>%(sku_isactive)s</vtex:IsActive>
				<vtex:IsAvaiable>%(sku_isavailable)s</vtex:IsAvaiable>
				<vtex:IsKit>%(sku_iskit)s</vtex:IsKit>
				<vtex:Length>%(sku_length)s</vtex:Length>
				<vtex:ListPrice>%(sku_listprice)s</vtex:ListPrice>
				<vtex:ModalId>%(sku_modalid)s</vtex:ModalId>
				<vtex:Name>%(sku_name)s</vtex:Name>
				<vtex:Price>%(sku_price)s</vtex:Price>
				<vtex:ProductId>%(product_id)s</vtex:ProductId>
				<vtex:RefId>%(ean)s</vtex:RefId>
				<vtex:StockKeepingUnitEans>
				   <vtex:StockKeepingUnitEanDTO>
					  <vtex:Ean>%(ean)s</vtex:Ean>
				   </vtex:StockKeepingUnitEanDTO>
				</vtex:StockKeepingUnitEans>
				<vtex:WeightKg>%(sku_weightkg)s</vtex:WeightKg>
				<vtex:Width>%(sku_width)s</vtex:Width>
			 </tem:stockKeepingUnitVO>
		  </tem:StockKeepingUnitInsertUpdate>
	   </soapenv:Body>
	</soapenv:Envelope>""" % {
		'sku_costprice':sku_costprice,
		'sku_cubicweight':sku_cubicweight,
		'sku_height':sku_height,
		'sku_id':sku_id,
		'sku_isactive':sku_isactive,
		'sku_isavailable':sku_isavailable,
		'sku_iskit':sku_iskit,
		'sku_length':sku_length,
		'sku_listprice':sku_listprice,
		'sku_modalid':sku_modalid,
		'sku_name':sku_name,
		'sku_price':sku_price,
		'product_id':product_id,
		'ean':ean,
		'sku_weightkg':sku_weightkg,
		'sku_width':sku_width
	}

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitInsertUpdate", soap_skuupdate)
	if not soup:
		return 'error: soap_skuupdate %s' % ean



if __name__ == '__main__':
	filter_str = "p.produto = '41.01.0108' and pc.cor_produto='02'"

	query = """
		SELECT 
			ps.CODIGO_BARRA as ean,
			pp.preco1 as list_price,
			pp.preco_liquido1 as price
		from PRODUTOS_BARRA ps
		inner join PRODUTOS p on p.produto = ps.produto
		inner join PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		inner join produtos_precos pp on pp.produto = ps.produto and pp.codigo_tab_preco = '11'
		left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		left join bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
		where 1=1
			and (%s)
		order by ps.CODIGO_BARRA
		;
	""" % filter_str
	products_to_register = dc.select(query, strip=True, dict_format=True)
	# print(products_to_register)


	errors = []
	# Rodar sem thread:
	for product_to_register in products_to_register:
		errors.append(f(product_to_register))

	errors = [x for x in errors if x]
	print(errors)