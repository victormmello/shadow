from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
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

path = 'C:\\Users\\victo\\git\\shadow\\fotos\\fotos_para_renomear'
filters = set()

photo_count_dict = {}
image_dict = {}

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

def f(product_and_img_dict):
	print(product_and_img_dict)
	product_info = product_and_img_dict[0]
	image_dict = product_and_img_dict[1]

	if not (product_info['vtex_color'] or product_info['vtex_category_id'] or product_info['vtex_department_id']):
		return product_info

	print(product_info)
	print(image_dict)
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
	print(sku_id)
	product_id = soup.find('a:ProductId').text



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
	image_verification = bool(soup.find('a:Name'))

	if not image_verification:
		found = False

		if product_id in image_dict:
			if color_id in image_dict[product_id]:
				found = True
				image_verification = True
				sku_found = image_dict[product_id][color_id]

		if found:
			soap_image_copy = """
				<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
				   <soapenv:Header/>
				   <soapenv:Body>
				      <tem:ImageServiceCopyAllImagesFromSkuToSku>
				         <!--Optional:-->
				         <tem:stockKeepingUnitIdFrom>%s</tem:stockKeepingUnitIdFrom>
				         <!--Optional:-->
				         <tem:stockKeepingUnitIdTo>%s</tem:stockKeepingUnitIdTo>
				      </tem:ImageServiceCopyAllImagesFromSkuToSku>
				   </soapenv:Body>
				</soapenv:Envelope>""" % (sku_found, sku_id)
			soup = post_to_webservice("http://tempuri.org/IService/ImageServiceCopyAllImagesFromSkuToSku", soap_image_copy)
			if not soup:
				return 'error: soap_image_copy %s' % EAN
		else:
			if product_id not in image_dict:
				image_color = {}
				image_color[color_id] = sku_id
				image_dict[product_id] = image_color
			else:
				image_color = image_dict[product_id]
				image_color[color_id] = sku_id
	else:
		if product_id not in image_dict:
			image_color = {}
			image_color[color_id] = sku_id
			image_dict[product_id] = image_color
		else:
			image_color = image_dict[product_id]
			image_color[color_id] = sku_id
	
	# Comentar esse bloco para atualizar informações de produto:
	if sku_isactive == 'true':
		return

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
	description = soup.find('a:Description').text

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
					<vtex:Description>%s</vtex:Description>
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
		</soapenv:Envelope>""" % (brand_id, category_id, department_id, description, product_id, link_id, product_name, product_refid)

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

	if not image_verification:
		try:
			photo_count = photo_count_dict[product_info['prod_code']][product_info['cod_color']]
		except Exception as e:
			photo_count = 4
		
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

	image_dict = Manager().dict()

	# if not filters:
	# 	raise Exception('Nenhuma imagem encontrada')

	filter_str = ' OR '.join(filters)

	# filter_str = "ps.CODIGO_BARRA='28060015491G'"

	# filter_str = "((p.produto='35.01.0828' AND pc.cor_produto='260') OR (p.produto='35.02.0803' AND pc.cor_produto='10'))"

	filter_str = """p.produto in ('08.04.023','20.02.0005','22.02.0238','22.02.0240','22.02.0245','22.02.0246','22.03.0217','22.03.0239','22.03.0243','22.03.0248','22.03.0249','22.05.0292','22.05.0368','22.05.0439','22.05.0464','22.05.0467','22.05.0472','22.05.0473','22.05.0512','22.05.0555','22.05.0560','22.05.0565','22.05.0572','22.06.0457','22.07.0129','22.07.0236','22.07.0267','22.07.0276','22.07.0279','22.07.0285','22.12.0206','22.12.0541','22.12.0563','22.12.0570','22.12.0576','22.12.0582','22.12.0584','22.12.0591','22.15.0007','23.08.0217','23.11.0206','23.11.0234','23.11.0248','23.11.0263','24.04.0535','29.02.0083','31.01.0140','31.02.0070','32.02.0237','32.06.0051','32.06.0062','32.07.0071','32.07.0082','33.02.0153','33.02.0209','35.01.0635','35.01.0734','35.01.0735','35.01.0748','35.01.0840','35.01.0857','35.02.0766','35.02.0786','35.02.0787','35.02.0830','35.02.0833','35.02.0834','35.09.0702','35.09.0981','35.09.1044','35.09.1201','37.01.0018','37.09.0002','77.22.0243','77.61.0105','77.61.0106','77.61.0107','77.61.0123','77.61.0136','77.61.0139','77.62.0023','77.73.0354','77.99.2300','77.99.2308')"""

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
	# 	f(product_to_register)

	with Pool(5) as p:
		errors = p.map(f, [(x, image_dict) for x in products_to_register])

	errors = [x for x in errors if x]
	print(errors)