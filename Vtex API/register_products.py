from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup

# --------------------------------------------------------------------------------------------------------------------------------

webserviceURL = "http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl"

params = {
	"Content-Type": "text/xml",
	"Authorization": "Basic dnRleGFwcGtleS1tYXJjaWFtZWxsby1YTlpGVVg6SEpHVkdVUFVTTVpTRllJSFZQTEpQRkJaUFlCTkxDRkhSWVRUVVRQWlNZVFlDSFRJT1BUSktBQUJISEZIVENJUEdTQUhGT01CWkxSUk1DWEhGU1lXSlZXUlhSTE5PSUdQUERTSkhMRFpDUktaSklQRktZQkJETUZMVklLT0RaTlE=",
}

auth = ("vtexappkey-marciamello-XNZFUX","HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ")

product_fail = []
def post_to_webservice(soap_action, soap_message, retry=3):
	params["SOAPAction"] = soap_action
	request_type = soap_action.split('/')[-1]

	for i in range(0, retry):
		try:
			response = requests.post("http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl", auth=auth, headers=params, data=soap_message.encode(), timeout=10)

			print("%s %s" % (request_type, response.status_code))
			if response.status_code == 200:
				break
			else:
				raise Exception()

		except Exception as e:
			if i == retry-1:
				print('desistindo')

				id_type = {}
				id_type['id'] = product_refid
				id_type['error'] = request_type
				product_fail.append(id_type)

				return None

	return Soup(response.text, "xml")

path = 'C:\\Users\\Felipe\\Downloads\\mm\\fotos\\upar4'
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

if not filters:
	raise Exception('Nenhuma imagem encontrada')

filter_str = ' OR '.join(filters)

filter_str = "((p.produto='22.02.0238' AND pc.cor_produto='105') OR (p.produto='22.05.0439' AND pc.cor_produto='05') OR (p.produto='22.05.0467' AND pc.cor_produto='04') OR (p.produto='22.05.0467' AND pc.cor_produto='155') OR (p.produto='22.07.0236' AND pc.cor_produto='02') OR (p.produto='22.12.0570' AND pc.cor_produto='176') OR (p.produto='23.11.0263' AND pc.cor_produto='02') OR (p.produto='30.04.0104' AND pc.cor_produto='01') OR (p.produto='32.02.0237' AND pc.cor_produto='35') OR (p.produto='32.02.0246' AND pc.cor_produto='02') OR (p.produto='35.02.0747' AND pc.cor_produto='05') OR (p.produto='35.02.0785' AND pc.cor_produto='32') OR (p.produto='35.02.0803' AND pc.cor_produto='10') OR (p.produto='35.04.0035' AND pc.cor_produto='01') OR (p.produto='35.04.0036' AND pc.cor_produto='24') OR (p.produto='35.09.1120' AND pc.cor_produto='02') OR (p.produto='37.09.0002' AND pc.cor_produto='105') OR (p.produto='19.06.1277' AND pc.cor_produto='12') OR (p.produto='21.01.0005' AND pc.cor_produto='454') OR (p.produto='22.02.0002' AND pc.cor_produto='10') OR (p.produto='22.02.0097' AND pc.cor_produto='14') OR (p.produto='22.03.0188' AND pc.cor_produto='220') OR (p.produto='22.03.0216' AND pc.cor_produto='02') OR (p.produto='22.03.0216' AND pc.cor_produto='176') OR (p.produto='22.04.0150' AND pc.cor_produto='122') OR (p.produto='22.04.0163' AND pc.cor_produto='01') OR (p.produto='22.04.0177' AND pc.cor_produto='259') OR (p.produto='22.04.0206' AND pc.cor_produto='176') OR (p.produto='22.05.0292' AND pc.cor_produto='94') OR (p.produto='22.05.0352' AND pc.cor_produto='02') OR (p.produto='22.05.0367' AND pc.cor_produto='217') OR (p.produto='22.05.0380' AND pc.cor_produto='259') OR (p.produto='22.06.0333' AND pc.cor_produto='03') OR (p.produto='22.06.0457' AND pc.cor_produto='453') OR (p.produto='22.06.0537' AND pc.cor_produto='04') OR (p.produto='22.07.0129' AND pc.cor_produto='02') OR (p.produto='22.07.0129' AND pc.cor_produto='03') OR (p.produto='22.07.0202' AND pc.cor_produto='221') OR (p.produto='22.12.0206' AND pc.cor_produto='02') OR (p.produto='22.12.0361' AND pc.cor_produto='176') OR (p.produto='22.12.0361' AND pc.cor_produto='94') OR (p.produto='22.12.0394' AND pc.cor_produto='01') OR (p.produto='22.12.0459' AND pc.cor_produto='460') OR (p.produto='22.12.0483' AND pc.cor_produto='221') OR (p.produto='22.12.0601' AND pc.cor_produto='20') OR (p.produto='22.12.0601' AND pc.cor_produto='211') OR (p.produto='22.12.0611' AND pc.cor_produto='01') OR (p.produto='22.14.0004' AND pc.cor_produto='94') OR (p.produto='22.15.0005' AND pc.cor_produto='461') OR (p.produto='22.15.0012' AND pc.cor_produto='176') OR (p.produto='23.04.0020' AND pc.cor_produto='08') OR (p.produto='23.06.0052' AND pc.cor_produto='176') OR (p.produto='23.06.0068' AND pc.cor_produto='454') OR (p.produto='23.06.0081' AND pc.cor_produto='02') OR (p.produto='23.06.0081' AND pc.cor_produto='16') OR (p.produto='23.08.0196' AND pc.cor_produto='105') OR (p.produto='23.08.0217' AND pc.cor_produto='01') OR (p.produto='23.11.0107' AND pc.cor_produto='04') OR (p.produto='23.11.0176' AND pc.cor_produto='14') OR (p.produto='23.11.0190' AND pc.cor_produto='24') OR (p.produto='23.11.0222' AND pc.cor_produto='02') OR (p.produto='23.11.0248' AND pc.cor_produto='105') OR (p.produto='23.11.0263' AND pc.cor_produto='01') OR (p.produto='24.03.0026' AND pc.cor_produto='07') OR (p.produto='24.04.0497' AND pc.cor_produto='337') OR (p.produto='24.04.0501' AND pc.cor_produto='105') OR (p.produto='28.04.0056' AND pc.cor_produto='221') OR (p.produto='31.02.0092' AND pc.cor_produto='02') OR (p.produto='31.02.0092' AND pc.cor_produto='16') OR (p.produto='31.02.0097' AND pc.cor_produto='04') OR (p.produto='31.02.0108' AND pc.cor_produto='176') OR (p.produto='32.02.0153' AND pc.cor_produto='04') OR (p.produto='32.02.0206' AND pc.cor_produto='04') OR (p.produto='32.03.0305' AND pc.cor_produto='02') OR (p.produto='32.03.0345' AND pc.cor_produto='217') OR (p.produto='32.05.0007' AND pc.cor_produto='16') OR (p.produto='32.06.0062' AND pc.cor_produto='04') OR (p.produto='32.06.0070' AND pc.cor_produto='105') OR (p.produto='32.07.0035' AND pc.cor_produto='33') OR (p.produto='33.02.0137' AND pc.cor_produto='04') OR (p.produto='33.02.0140' AND pc.cor_produto='23') OR (p.produto='33.02.0229' AND pc.cor_produto='01') OR (p.produto='34.01.0009' AND pc.cor_produto='04') OR (p.produto='34.01.0014' AND pc.cor_produto='03') OR (p.produto='34.01.0049' AND pc.cor_produto='07') OR (p.produto='34.01.0079' AND pc.cor_produto='218') OR (p.produto='35.01.0495' AND pc.cor_produto='105') OR (p.produto='35.01.0593' AND pc.cor_produto='04') OR (p.produto='35.01.0597' AND pc.cor_produto='05') OR (p.produto='35.01.0599' AND pc.cor_produto='105') OR (p.produto='35.01.0599' AND pc.cor_produto='68') OR (p.produto='35.01.0607' AND pc.cor_produto='259') OR (p.produto='35.01.0672' AND pc.cor_produto='04') OR (p.produto='35.01.0680' AND pc.cor_produto='451') OR (p.produto='35.01.0732' AND pc.cor_produto='05') OR (p.produto='35.01.0846' AND pc.cor_produto='01') OR (p.produto='35.02.0599' AND pc.cor_produto='04') OR (p.produto='35.02.0614' AND pc.cor_produto='218') OR (p.produto='35.02.0640' AND pc.cor_produto='217') OR (p.produto='35.02.0683' AND pc.cor_produto='216') OR (p.produto='35.02.0746' AND pc.cor_produto='16') OR (p.produto='35.03.0013' AND pc.cor_produto='04') OR (p.produto='35.04.0027' AND pc.cor_produto='05') OR (p.produto='35.09.0656' AND pc.cor_produto='07') OR (p.produto='35.09.0702' AND pc.cor_produto='04') OR (p.produto='35.09.0832' AND pc.cor_produto='02') OR (p.produto='35.09.0985' AND pc.cor_produto='16') OR (p.produto='35.09.1000' AND pc.cor_produto='221') OR (p.produto='35.09.1100' AND pc.cor_produto='105') OR (p.produto='35.09.1105' AND pc.cor_produto='105') OR (p.produto='77.22.0243' AND pc.cor_produto='00') OR (p.produto='77.61.0105' AND pc.cor_produto='00') OR (p.produto='77.61.0106' AND pc.cor_produto='00') OR (p.produto='77.61.0107' AND pc.cor_produto='00') OR (p.produto='77.61.0123' AND pc.cor_produto='00') OR (p.produto='77.61.0136' AND pc.cor_produto='00') OR (p.produto='77.61.0139' AND pc.cor_produto='00') OR (p.produto='77.62.0023' AND pc.cor_produto='00') OR (p.produto='77.73.0354' AND pc.cor_produto='00') OR (p.produto='77.99.2300' AND pc.cor_produto='00') OR (p.produto='77.99.2308' AND pc.cor_produto='00')) "

query = """
	SELECT 
		-- count(*), count(distinct p.produto + pc.COR_PRODUTO), sum(e.estoque_disponivel)
		-- , p.DATA_REPOSICAO 
		p.produto as prod_code,
		ps.grade as size, 
		ps.COR_PRODUTO as cod_color, 
		pc.DESC_COR_PRODUTO as desc_color, 
		color.vtex_color,
		ps.CODIGO_BARRA as ean,
		'2000000' as brand_id,
		COALESCE(cat1.vtex_category_id, cat2.vtex_category_id) as vtex_category_id,
		COALESCE(cat1.vtex_department_id, cat2.vtex_department_id) as vtex_department_id
		-- p.DATA_REPOSICAO
	from dbo.PRODUTOS_BARRA ps
	inner join dbo.PRODUTOS p on p.produto = ps.produto
	inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
	left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
	left join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
	left join bi_vtex_categorizacao_categoria cat1 on cat1.grupo_produto = p.GRUPO_PRODUTO and cat1.subgrupo_produto = p.SUBGRUPO_PRODUTO
	left join bi_vtex_categorizacao_categoria cat2 on cat2.grupo_produto = p.GRUPO_PRODUTO and cat2.subgrupo_produto is null
	left join bi_vtex_categorizacao_cor color on color.linx_color = pc.DESC_COR_PRODUTO
	where 1=1
		-- and pc.COR_PRODUTO not in ('%%')
		-- and p.DATA_REPOSICAO > '2018-01-01'
		-- and v_item.image_url is null
		-- p.produto = '35.01.0608' and
		-- and pc.COR_PRODUTO in ('256')
		-- and e.estoque_disponivel > 0
		-- and p.produto = '22.12.0620'
		and (%s)
		-- and ps.codigo_barra > '3501059604P'
	order by ps.CODIGO_BARRA
	;
""" % filter_str

products_to_register = dc.select(query, strip=True, dict_format=True)

# raise Exception(len(products_to_register))
product_list = []

for product_info in products_to_register:
	if not (product_info['vtex_color'] or product_info['vtex_category_id'] or product_info['vtex_department_id']):
		print(product_info)
		continue

	# print(product_info)

	EAN = product_info['ean']
	brand_id = product_info['brand_id']
	department_id = product_info['vtex_department_id']
	category_id = product_info['vtex_category_id']
	sku_size = product_info['size']
	sku_color = product_info['vtex_color']
	color_id = product_info['cod_color']
	
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
		continue

	if not soup.find('a:Id'):
		id_type = {}
		id_type['id'] = EAN
		id_type['error'] = 'sku_get'
		product_fail.append(id_type)
		continue

	sku_costprice = soup.find('a:CostPrice').text
	sku_height = soup.find('a:Height').text
	sku_id = soup.find('a:Id').text
	sku_isactive = soup.find('a:IsActive').text
	sku_isavailable = soup.find('a:IsAvaiable').text
	sku_iskit = soup.find('a:IsKit').text
	sku_length = soup.find('a:Length').text
	sku_listprice = soup.find('a:ListPrice').text
	sku_modalid = soup.find('a:ModalId').text
	sku_name = soup.find('a:Name').text
	sku_price = soup.find('a:Price').text
	# sku_weightkg = soup.find('a:WeightKg').text
	sku_width = soup.find('a:Width').text
	sku_cubicweight = soup.find('a:CubicWeight').text
	sku_weightkg = "300"

	product_id = soup.find('a:ProductId').text

	# if sku_isactive == 'true':
	# 	continue

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
		continue
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
		continue


	product_name = soup.find('a:Name').text
	link_id = soup.find('a:LinkId').text
	product_refid = soup.find('a:RefId').text
	product_isactive = soup.find('a:IsActive').text

	# --------------------------------------------------------------------------------------------------------------------------------

	if product_refid not in product_list:
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
			continue

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
		continue
	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitEspecificationInsert", soap_color)
	if not soup:
		continue

	# --------------------------------------------------------------------------------------------------------------------------------

	soap_skuupdate = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitInsertUpdate>
			 <!--Optional:-->
			 <tem:stockKeepingUnitVO>
				<vtex:CostPrice>%s</vtex:CostPrice>
				<vtex:CubicWeight>%s</vtex:CubicWeight>
				<vtex:Height>%s</vtex:Height>
				<vtex:Id>%s</vtex:Id>
				<vtex:IsActive>%s</vtex:IsActive>
				<vtex:IsAvaiable>%s</vtex:IsAvaiable>
				<vtex:IsKit>%s</vtex:IsKit>
				<vtex:Length>%s</vtex:Length>
				<vtex:ListPrice>%s</vtex:ListPrice>
				<vtex:ModalId>%s</vtex:ModalId>
				<vtex:Name>%s</vtex:Name>
				<vtex:Price>%s</vtex:Price>
				<vtex:ProductId>%s</vtex:ProductId>
				<vtex:RefId>%s</vtex:RefId>
				<vtex:StockKeepingUnitEans>
				   <vtex:StockKeepingUnitEanDTO>
					  <vtex:Ean>%s</vtex:Ean>
				   </vtex:StockKeepingUnitEanDTO>
				</vtex:StockKeepingUnitEans>
				<vtex:WeightKg>%s</vtex:WeightKg>
				<vtex:Width>%s</vtex:Width>
			 </tem:stockKeepingUnitVO>
		  </tem:StockKeepingUnitInsertUpdate>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (sku_costprice, sku_cubicweight, sku_height, sku_id, sku_isactive, sku_isavailable, sku_iskit, sku_length, sku_listprice, sku_modalid, sku_name, sku_price, product_id, EAN, EAN, sku_weightkg, sku_width)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitInsertUpdate", soap_skuupdate)
	if not soup:
		continue

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
		continue

	if product_refid not in product_list:
		product_list.append(product_refid)

with open('product_list.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";")
	for n in product_list:
		csv_file.writerow({n})

with open('product_fail.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";")
	for n in product_fail:
		csv_file.writerow({n['id'], n['error']})
		
