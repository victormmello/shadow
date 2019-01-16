from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv, json
import requests
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool, Manager

# --------------------------------------------------------------------------------------------------------------------------------

webserviceURL = "http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl"

params = {
	"Content-Type": "text/xml",
	"Authorization": "Basic dnRleGFwcGtleS1tYXJjaWFtZWxsby1YTlpGVVg6SEpHVkdVUFVTTVpTRllJSFZQTEpQRkJaUFlCTkxDRkhSWVRUVVRQWlNZVFlDSFRJT1BUSktBQUJISEZIVENJUEdTQUhGT01CWkxSUk1DWEhGU1lXSlZXUlhSTE5PSUdQUERTSkhMRFpDUktaSklQRktZQkJETUZMVklLT0RaTlE=",
}

auth = ("vtexappkey-marciamello-XNZFUX","HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ")

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)


product_dict = {}
error_list = []

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
				print(response.text)
				return None

	return Soup(response.text, "xml")

def f(product_info):

	print(product_info)

	EAN = product_info['ean']
	brand_id = product_info['brand_id']
	description_color = product_info['desc_vtex_color']
	size = product_info['size']
	department_id = product_info['vtex_department_id']
	category_id = product_info['vtex_category_id']
	product_name = product_info['product_name']
	list_price = product_info['list_price']
	price = product_info['price']
	cost_price = product_info['cost_price']
	product_refid = product_info['product_refid']
	link_id = product_name.replace(' ','-') + '-' +  product_refid.replace('.','-')
	keywords = product_name + ', ' + product_refid
	product_id = None
	estoque_disponivel = product_info['estoque_disponivel']

	if None in (brand_id, description_color, category_id, department_id):
		return 'Error none ' + EAN

# --------------------------------------------------------------------------------------------------------------------------------

	soap_skuget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitGetByEan>
			 <!--Optional:-->
			 <tem:EAN13>%s</tem:EAN13>
		  </tem:StockKeepingUnitGetByEan>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (EAN)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitGetByEan", soap_skuget)

	if not soup:
		print('error: soap_skuget %s' % EAN)  

	if soup.find('a:Id'):
		product_id = soup.find('a:ProductId').text
		found_sku = True
	else:
		found_sku = False 

	if not found_sku:

		if product_refid not in product_dict:
			soap_productget = """
				<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
				<soapenv:Header/>
				<soapenv:Body>
				  <tem:ProductGetByRefId>
				     <!--Optional:-->
				     <tem:refId>%s</tem:refId>
				  </tem:ProductGetByRefId>
				</soapenv:Body>
				</soapenv:Envelope>""" % (product_refid)

			soup = post_to_webservice("http://tempuri.org/IService/ProductGetByRefId", soap_productget)

			if not soup:
				print('error: soap_productget %s' % EAN)
			if soup.find('a:Id'):
				product_id = soup.find('a:Id').text
		else:
			product_id = product_dict[product_refid]

		if not product_id:
			soap_create_product = """
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
							<vtex:IsActive>true</vtex:IsActive>
							<vtex:IsVisible>true</vtex:IsVisible>
							<vtex:KeyWords>%s</vtex:KeyWords>
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
				</soapenv:Envelope>""" % (brand_id, category_id, department_id, product_name, product_name, keywords, link_id, product_name, product_refid)
			soup = post_to_webservice("http://tempuri.org/IService/ProductInsertUpdate", soap_create_product)
			# print(soap_create_product)
			product_id = soup.find('a:Id').text
			if not soup:
				print('error: soap_create_product %s' % EAN)

		sku_name = product_name + '-' + description_color + '-' + size
		sku_modalid = 1
		sku_height = 10
		sku_length = 10
		sku_width = 1000
		sku_weightkg = 300
		sku_cubicweight = 101
		sku_isactive = 'false'
		sku_iskit = 'false'
		sku_isavailable = 'false'

		soap_skuupdate = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts">
		   <soapenv:Header/>
		   <soapenv:Body>
			  <tem:StockKeepingUnitInsertUpdate>
				 <!--Optional:-->
				 <tem:stockKeepingUnitVO>
					<vtex:CostPrice>%(cost_price)s</vtex:CostPrice>
					<vtex:CubicWeight>%(sku_cubicweight)s</vtex:CubicWeight>
					<vtex:Height>%(sku_height)s</vtex:Height>
					<vtex:IsActive>%(sku_isactive)s</vtex:IsActive>
					<vtex:IsAvaiable>%(sku_isavailable)s</vtex:IsAvaiable>
					<vtex:IsKit>%(sku_iskit)s</vtex:IsKit>
					<vtex:Length>%(sku_length)s</vtex:Length>
					<vtex:ListPrice>%(list_price)s</vtex:ListPrice>
					<vtex:ModalId>%(sku_modalid)s</vtex:ModalId>
					<vtex:Name>%(sku_name)s</vtex:Name>
					<vtex:Price>%(price)s</vtex:Price>
					<vtex:ProductId>%(product_id)s</vtex:ProductId>
					<vtex:RefId>%(EAN)s</vtex:RefId>
					<vtex:StockKeepingUnitEans>
					   <vtex:StockKeepingUnitEanDTO>
						  <vtex:Ean>%(EAN)s</vtex:Ean>
					   </vtex:StockKeepingUnitEanDTO>
					</vtex:StockKeepingUnitEans>
					<vtex:WeightKg>%(sku_weightkg)s</vtex:WeightKg>
					<vtex:Width>%(sku_width)s</vtex:Width>
				 </tem:stockKeepingUnitVO>
			  </tem:StockKeepingUnitInsertUpdate>
		   </soapenv:Body>
		</soapenv:Envelope>""" % {
			'cost_price':cost_price,
			'sku_cubicweight':sku_cubicweight,
			'sku_height':sku_height,
			'sku_isactive':sku_isactive,
			'sku_isavailable':sku_isavailable,
			'sku_iskit':sku_iskit,
			'sku_length':sku_length,
			'list_price':list_price,
			'sku_modalid':sku_modalid,
			'sku_name':sku_name,
			'price':price,
			'product_id':product_id,
			'EAN':EAN,
			'EAN':EAN,
			'sku_weightkg':sku_weightkg,
			'sku_width':sku_width
		}

		soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitInsertUpdate", soap_skuupdate)

		if soup.find('a:Id'):
			sku_id = soup.find('a:Id').text
			set_stock_url = 'http://logistics.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/%s/warehouses/1_1?an=marciamello' % sku_id
			response = requests.request("PUT", set_stock_url, headers=api_connection_config, data='{"quantity": %s}' % sku_id)
	if product_refid not in product_dict:
		product_dict[product_refid] = product_id


if __name__ == '__main__':

	query = """
		SELECT 
			p.produto as product_refid,
			p.desc_produto as product_name,
			ps.grade as size, 
			-- ps.COR_PRODUTO as cod_color, 
			-- pc.DESC_COR_PRODUTO as desc_color, 
			color.vtex_color as desc_vtex_color,
			ps.CODIGO_BARRA as ean,
			case 
				when p.griffe = 'NAKD' then '2000001' 
				else '2000000' 
			end as brand_id,
			COALESCE(cat1.vtex_category_id, cat2.vtex_category_id) as vtex_category_id,
			COALESCE(cat1.vtex_department_id, cat2.vtex_department_id) as vtex_department_id,
			pp.preco1 as list_price,
			pp.preco_liquido1 as price,
			pcusto.preco1 as cost_price,
			e.estoque_disponivel as estoque_disponivel
		from PRODUTOS_BARRA ps
		inner join PRODUTOS p on p.produto = ps.produto
		inner join PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		inner join produtos_precos pp on pp.produto = ps.produto and pp.codigo_tab_preco = '11'
		inner join produtos_precos pcusto on pcusto.produto = ps.produto and pcusto.codigo_tab_preco = '02'
		left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		left join bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
		left join bi_vtex_categorizacao_categoria cat1 on cat1.grupo_produto = p.GRUPO_PRODUTO and cat1.subgrupo_produto = p.SUBGRUPO_PRODUTO
		left join bi_vtex_categorizacao_categoria cat2 on cat2.grupo_produto = p.GRUPO_PRODUTO and cat2.subgrupo_produto is null
		left join bi_vtex_categorizacao_cor color on color.linx_color = pc.DESC_COR_PRODUTO
		where 1=1
			and p.produto = '35.01.0833'
		order by ps.CODIGO_BARRA
		;
	"""
	# print(query)
	products_to_register = dc.select(query, strip=True, dict_format=True)
	# print(products_to_register)
	# raise Exception(len(products_to_register))
	errors = []
	Rodar sem thread:
	for product_to_register in products_to_register:
		f(product_to_register)

	# with Pool(5) as p:
	# 	errors = p.map(f, products_to_register)

	# errors = [x for x in errors if x]
	# print(errors)