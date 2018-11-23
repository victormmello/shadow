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

path = 'C:\\Users\\victo\\git\\shadow\\fotos\\fotos_para_renomear'
filters = set()

photo_count_dict = {}
for folder, subs, files in os.walk(path):
	for filename in files:
		filename = filename[:-4]

		prod_code, resto = filename.split('.')
		prod_code = '%s.%s.%s' % (prod_code[:2], prod_code[2:4], prod_code[4:])
		color_code = resto.split('_')[0].zfill(2)

		set_in_dict(photo_count_dict, 1, [prod_code, color_code], repeated_key='sum')

		filters.add("(ps.produto = '%s' and ps.COR_PRODUTO = '%s')" % (prod_code, color_code))



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
	if not (product_info['vtex_color'] or product_info['vtex_category_id'] or product_info['vtex_department_id']):
		return product_info

	# print(product_info)

	EAN = product_info['ean']
	brand_id = product_info['brand_id']
	department_id = product_info['vtex_department_id']
	category_id = product_info['vtex_category_id']
	sku_size = product_info['size']
	sku_color = product_info['vtex_color']
	color_id = product_info['cod_color']
	list_price = product_info['list_price']
	price = product_info['price']

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
		return 'error: soap_skuget %s' % EAN

	if not soup.find('a:Id'):
		return 'error: soap_skuget - no ID %s' % EAN

	sku_costprice = soup.find('a:CostPrice').text
	sku_height = soup.find('a:Height').text
	sku_id = soup.find('a:Id').text
	sku_isactive = soup.find('a:IsActive').text
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

	# Comentar esse bloco para atualizar informações de produto:
	if sku_isactive == 'true':
		return

	# soap_imageremove = """
	# 	<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	# 	   <soapenv:Header/>
	# 	   <soapenv:Body>
	# 	      <tem:StockKeepingUnitImageRemove>
	# 	         <!--Optional:-->
	# 	         <tem:stockKeepingUnitId>%s</tem:stockKeepingUnitId>
	# 	      </tem:StockKeepingUnitImageRemove>
	# 	   </soapenv:Body>
	# 	</soapenv:Envelope>
	# """ % sku_id

	# soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitImageRemove", soap_imageremove)
	# if not soup:
	# 	continue
	# continue

	# --------------------------------------------------------------------------------------------------------------------------------

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
		return 'error: soap_imageget %s' % EAN
	image_test_name = soup.find('a:Name')

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
	product_isactive = soup.find('a:IsActive').text

	if not product_refid:
		product_refid = product_info['prod_code']

	# --------------------------------------------------------------------------------------------------------------------------------

	# if product_refid not in product_list:
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

	# --------------------------------------------------------------------------------------------------------------------------------

	sku_description = ""
	sku_fileLocation = "marciamello/catalog/"
	sku_label = ""
	sku_tag= ""
	# sku_imagename = "-".join(sku_name.split('-')[:-1])
	sku_imagename = ""

	if not image_test_name:
		photo_count = photo_count_dict[product_info['prod_code']][product_info['cod_color']]
		for x in range(1,photo_count+1):
			sku_url = "http://blog.marciamello.com.br/Ecommerce/Cadastro_Produtos/%s.%s_0%s.jpg" % (product_refid.replace(".", ""), color_id, x)
			print(sku_url)
			if x==2:
				sku_label = "detail"
				sku_tag = "detail"
			else:
				sku_label = ""
				sku_tag = ""

			soap_imageinsert = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts">
			 <soapenv:Header/>
			 <soapenv:Body>
				<tem:ImageInsertUpdate>
				   <!--Optional:-->
				   <tem:image>
					  <vtex:Description>%s</vtex:Description>
					  <vtex:FileLocation>%s</vtex:FileLocation>
					  <vtex:Label>%s</vtex:Label>
					  <vtex:Name>%s</vtex:Name>
					  <vtex:StockKeepingUnitId>%s</vtex:StockKeepingUnitId>
					  <vtex:Tag>%s</vtex:Tag>
					  <vtex:Url>%s</vtex:Url>
				   </tem:image>
				</tem:ImageInsertUpdate>
			 </soapenv:Body>
			</soapenv:Envelope>""" % (sku_description, sku_fileLocation, sku_label, sku_imagename, sku_id, sku_tag, sku_url)

			soup = post_to_webservice("http://tempuri.org/IService/ImageInsertUpdate", soap_imageinsert)
			if not soup:
				continue

	# --------------------------------------------------------------------------------------------------------------------------------

	soap_size = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitEspecificationInsert>
			 <!--number, identificador do SKU dono do campo-->
			 <tem:idSku>%s</tem:idSku>
			 <!--string, identificador do campo, nome do campo-->
			 <tem:fieldName>Tamanho</tem:fieldName>
			 <!--array, lista de valores dos campos-->
			 <tem:fieldValues>
				<!--string, valor de campo-->
				<arr:string>%s</arr:string>
			 </tem:fieldValues>
		  </tem:StockKeepingUnitEspecificationInsert>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (sku_id, sku_size)

	soap_color = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitEspecificationInsert>
			 <!--number, identificador do SKU dono do campo-->
			 <tem:idSku>%s</tem:idSku>
			 <!--string, identificador do campo, nome do campo-->
			 <tem:fieldName>Cor</tem:fieldName>
			 <!--array, lista de valores dos campos-->
			 <tem:fieldValues>
				<!--string, valor de campo-->
				<arr:string>%s</arr:string>
			 </tem:fieldValues>
		  </tem:StockKeepingUnitEspecificationInsert>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (sku_id, sku_color)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitEspecificationInsert", soap_size)
	if not soup:
		return 'error: soap_size %s' % EAN
	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitEspecificationInsert", soap_color)
	if not soup:
		return 'error: soap_color %s' % EAN

	# --------------------------------------------------------------------------------------------------------------------------------

	soap_skuupdate = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitInsertUpdate>
			 <!--Optional:-->
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
		'EAN':EAN,
		'EAN':EAN,
		'sku_weightkg':sku_weightkg,
		'sku_width':sku_width
	}

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitInsertUpdate", soap_skuupdate)
	if not soup:
		return 'error: soap_skuupdate %s' % EAN

	soap_skuactive = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitActive>
			 <tem:idStockKeepingUnit>%s</tem:idStockKeepingUnit>
		  </tem:StockKeepingUnitActive>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (sku_id)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitActive", soap_skuactive)
	if not soup:
		return 'error: soap_skuactive %s' % EAN



if __name__ == '__main__':
	
	if not filters:
		raise Exception('Nenhuma imagem encontrada')

	filter_str = ' OR '.join(filters)

	# filter_str = "ps.CODIGO_BARRA='28060015491G'"

	# filter_str = "((p.produto='35.01.0828' AND pc.cor_produto='260') OR (p.produto='35.02.0803' AND pc.cor_produto='10'))"

# 	filter_str = """
# 		(p.produto = '22.05.0569' and pc.cor_produto = '105') OR
# 		(p.produto = '22.12.0616' and pc.cor_produto = '105') OR
# 		(p.produto = '33.02.0232' and pc.cor_produto = '216') OR
# 		(p.produto = '35.04.0049' and pc.cor_produto = '221') OR
# 		(p.produto = '22.05.0525' and pc.cor_produto = '105') OR
# 		(p.produto = '22.05.0564' and pc.cor_produto = '216') OR
# 		(p.produto = '22.05.0580' and pc.cor_produto = '231') OR
# 		(p.produto = '22.12.0615' and pc.cor_produto = '176') OR
# 		(p.produto = '35.02.0830' and pc.cor_produto = '198') OR
# 		(p.produto = '22.05.0524' and pc.cor_produto = '16') OR
# 		(p.produto = '37.05.0029' and pc.cor_produto = '219') OR
# 		(p.produto = '22.05.0579' and pc.cor_produto = '94') OR
# 		(p.produto = '22.05.0577' and pc.cor_produto = '252') OR
# 		(p.produto = '37.05.0029' and pc.cor_produto = '218') OR
# 		(p.produto = '35.04.0048' and pc.cor_produto = '485') OR
# 		(p.produto = '22.05.0542' and pc.cor_produto = '20') OR
# 		(p.produto = '33.02.0232' and pc.cor_produto = '105') OR
# 		(p.produto = '22.05.0577' and pc.cor_produto = '198') OR
# 		(p.produto = '22.05.0524' and pc.cor_produto = '32') OR
# 		(p.produto = '33.02.0232' and pc.cor_produto = '457') OR
# 		(p.produto = '22.05.0564' and pc.cor_produto = '452') OR
# 		(p.produto = '22.05.0577' and pc.cor_produto = '244') OR
# 		(p.produto = '33.02.0232' and pc.cor_produto = '259') OR
# 		(p.produto = '35.04.0049' and pc.cor_produto = '223') OR
# 		(p.produto = '22.05.0523' and pc.cor_produto = '176') OR
# 		(p.produto = '35.02.0836' and pc.cor_produto = '507') OR
# 		(p.produto = '37.05.0029' and pc.cor_produto = '221') OR
# 		(p.produto = '33.02.0232' and pc.cor_produto = '32') OR
# 		(p.produto = '22.12.0616' and pc.cor_produto = '155') OR
# 		(p.produto = '35.02.0836' and pc.cor_produto = '198') OR
# 		(p.produto = '35.04.0049' and pc.cor_produto = '217') OR
# 		(p.produto = '22.05.0525' and pc.cor_produto = '176') OR
# 		(p.produto = '32.03.0370' and pc.cor_produto = '507') OR
# 		(p.produto = '22.12.0616' and pc.cor_produto = '176') OR
# 		(p.produto = '22.05.0577' and pc.cor_produto = '507') OR
# 		(p.produto = '22.07.0285' and pc.cor_produto = '505') OR
# 		(p.produto = '22.07.0285' and pc.cor_produto = '231') OR
# 		(p.produto = '22.05.0524' and pc.cor_produto = '176') OR
# 		(p.produto = '28.06.0015' and pc.cor_produto = '491')
# """

	query = """
		SELECT 
			p.produto as prod_code,
			ps.grade as size, 
			ps.COR_PRODUTO as cod_color, 
			pc.DESC_COR_PRODUTO as desc_color, 
			color.vtex_color,
			ps.CODIGO_BARRA as ean,
			case 
				when p.griffe = 'NAKD' then '2000001' 
				else '2000000' 
			end as brand_id,
			COALESCE(cat1.vtex_category_id, cat2.vtex_category_id) as vtex_category_id,
			COALESCE(cat1.vtex_department_id, cat2.vtex_department_id) as vtex_department_id,
			pp.preco1 as list_price,
			pp.preco_liquido1 as price
		from PRODUTOS_BARRA ps
		inner join PRODUTOS p on p.produto = ps.produto
		inner join PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		inner join produtos_precos pp on pp.produto = ps.produto and pp.codigo_tab_preco = '11'
		left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		left join bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
		left join bi_vtex_categorizacao_categoria cat1 on cat1.grupo_produto = p.GRUPO_PRODUTO and cat1.subgrupo_produto = p.SUBGRUPO_PRODUTO
		left join bi_vtex_categorizacao_categoria cat2 on cat2.grupo_produto = p.GRUPO_PRODUTO and cat2.subgrupo_produto is null
		left join bi_vtex_categorizacao_cor color on color.linx_color = pc.DESC_COR_PRODUTO
		where 1=1
			and (%s)
		order by ps.CODIGO_BARRA
		;
	""" % filter_str
	# print(query)
	products_to_register = dc.select(query, strip=True, dict_format=True)
	# print(products_to_register)

	errors = []
	# Rodar sem thread:
	# for product_to_register in products_to_register:
		# f(product_to_register)

	with Pool(5) as p:
		errors = p.map(f, products_to_register,)

	errors = [x for x in errors if x]
	print(errors)