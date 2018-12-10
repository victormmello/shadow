from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
from shadow_vtex.vtex import update_vtex_product, post_to_webservice
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool, Manager

# --------------------------------------------------------------------------------------------------------------------------------

program_params = {
	'remove_sku_photos': True,
	'skip_active_skus': False,
	'dafiti_store': True,
}

path = 'C:\\Users\\Felipe\\Downloads\\mm\\fotos\\substituir'

photo_count_dict = {}
image_dict = {}
filters = set()
for folder, subs, files in os.walk(path):
	for filename in files:
		filename = filename[:-4]

		prod_code, resto = filename.split('.')
		prod_code = '%s.%s.%s' % (prod_code[:2], prod_code[2:4], prod_code[4:])
		color_code = resto.split('_')[0].zfill(2)

		set_in_dict(photo_count_dict, 1, [prod_code, color_code], repeated_key='sum')

		filters.add("(ps.produto = '%s' and ps.COR_PRODUTO = '%s')" % (prod_code, color_code))

def f(product_and_img_dict):
	product_info = product_and_img_dict[0]
	image_dict = product_and_img_dict[1]

	if not (product_info['vtex_color'] or product_info['vtex_category_id'] or product_info['vtex_department_id']):
		return product_info

	EAN = product_info['ean']
	brand_id = product_info['brand_id']
	department_id = product_info['vtex_department_id']
	category_id = product_info['vtex_category_id']
	sku_size = product_info['size']
	sku_color = product_info['vtex_color']
	color_id = product_info['cod_color']
	list_price = product_info['list_price']
	price = product_info['price']
	prod_code = product_info['prod_code']

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

	if program_params['remove_sku_photos']:
		soap_imageremove = """
			<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
			   <soapenv:Header/>
			   <soapenv:Body>
			      <tem:StockKeepingUnitImageRemove>
			         <!--Optional:-->
			         <tem:stockKeepingUnitId>%s</tem:stockKeepingUnitId>
			      </tem:StockKeepingUnitImageRemove>
			   </soapenv:Body>
			</soapenv:Envelope>
		""" % sku_id

		soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitImageRemove", soap_imageremove)
		if not soup:
			return 'error: soap_removeimage %s' % EAN

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
	# image_verification = bool(soup.find('a:Name'))
	image_verification = False

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
	
	if program_params['skip_active_skus'] and sku_isactive == 'true':
		return

	# --------------------------------------------------------------------------------------------------------------------------------

	update_vtex_product(product_id, {
		'RefId': product_info['prod_code'],
		'BrandId': brand_id,
		'CategoryId': category_id,
		'DepartmentId': department_id,
		'Description': '%(Name)s',
		'IsActive': 'true',
		'IsVisible': 'true',
		'ShowWithoutStock': 'false',
	}, dafiti_store=program_params['dafiti_store'])

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
			sku_url = "http://blog.marciamello.com.br/Ecommerce/Cadastro_Produtos/%s.%s_0%s.jpg" % (prod_code.replace(".", ""), color_id, x)
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
			 <tem:idSku>%s</tem:idSku>
			 <tem:fieldName>Tamanho</tem:fieldName>
			 <tem:fieldValues>
				<arr:string>%s</arr:string>
			 </tem:fieldValues>
		  </tem:StockKeepingUnitEspecificationInsert>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (sku_id, sku_size)

	soap_color = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitEspecificationInsert>
			 <tem:idSku>%s</tem:idSku>
			 <tem:fieldName>Cor</tem:fieldName>
			 <tem:fieldValues>
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

	if not filters:
		raise Exception('Nenhuma imagem encontrada')

	# Atualização Manual:
	# product_list = [
	# 	{'produto':'41.01.0109','cor':'02','photo_count':4}
	# ]
	# if product_list:
	# 	filters = set()
	# 	photo_count_dict = {}
	# 	for product in product_list:
	# 		filters.add("(ps.produto = '%s' and ps.COR_PRODUTO = '%s')" % (product['produto'],product['cor']))
	# 		set_in_dict(photo_count_dict, product['photo_count'], [product['produto'], product['cor']], repeated_key='sum')

	# filter_str = "ps.CODIGO_BARRA='08060790234'"
	# filter_str = "ps.CODIGO_BARRA='08011950239'"


	# filter_str = "((p.produto='35.01.0828' AND pc.cor_produto='260') OR (p.produto='35.02.0803' AND pc.cor_produto='10'))"

	# filter_str = """(p.produto='32.06.0040' AND pc.cor_produto='05') OR (p.produto='32.07.0082' AND pc.cor_produto='244')"""

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
			-- '2000000' as brand_id,
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
	for product_to_register in products_to_register:
		f([product_to_register, image_dict])

	# ============== nao rodar com threads ainda
	# with Pool(5) as p:
	# 	errors = p.map(f, [(x, image_dict) for x in products_to_register])

	errors = [x for x in errors if x]
	print(errors)