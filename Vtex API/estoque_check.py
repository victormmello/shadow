import requests
import os, fnmatch, shutil
import csv
from bs4 import BeautifulSoup as Soup
from shadow_database import DatabaseConnection
dc = DatabaseConnection()

def search_directory(products_tested):
	filename_to_product = {}
	already_found_images = set()
	found_products = []
	paths = ["C:\\Users\\Felipe\\Downloads\\mm\\fotos\\maisa_nov", "D:\\"]

	for product in products_tested:
		for n in range(1,5):
			filename = '%s.%s_0%s.jpg' % (product['prod_code'].replace(".",""), product['cod_color'], n)
			filename_to_product[filename] = product

			filename = '%s.%s_0%s.jpg' % (product['prod_code'].replace(".",""), str(product['cod_color']).zfill(2), n)
			filename_to_product[filename] = product

	for path in paths:
		for folder, subs, files in os.walk(path):
			for filename in files:
				if filename in filename_to_product and filename not in already_found_images:
					already_found_images.add(filename)

					filepath = os.path.abspath(os.path.join(folder, filename))
					shutil.copy(filepath,'C:\\Users\\Felipe\\Downloads\\mm\\fotos\\upar3\\')
					# print(filepath)

					product = filename_to_product[filename]
					found_products.append(product)

	return found_products

products_tested = dc.select("""
	SELECT distinct
		-- count(*), sum(e.estoque_disponivel)
		-- , p.DATA_REPOSICAO 
		p.produto as prod_code, 
		CAST(ps.COR_PRODUTO AS INT) as cod_color, 
		MAX(ps.CODIGO_BARRA) as ean
	from dbo.PRODUTOS_BARRA ps
	inner join dbo.PRODUTOS p on p.produto = ps.produto
	inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
	left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
	left join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
	left join bi_vtex_categorizacao_categoria cat1 on cat1.grupo_produto = p.GRUPO_PRODUTO and cat1.subgrupo_produto = p.SUBGRUPO_PRODUTO
	left join bi_vtex_categorizacao_categoria cat2 on cat2.grupo_produto = p.GRUPO_PRODUTO and cat2.subgrupo_produto is null
	left join bi_vtex_categorizacao_cor color on color.linx_color = pc.DESC_COR_PRODUTO
	where 1=1
	 	--and p.DATA_REPOSICAO > '2018-01-01'
	 	-- and v_item.image_url is null
	 	and pc.DESC_COR_PRODUTO NOT LIKE '%%(CANCELADO)%%'
	 	-- and ps.codigo_barra = '3302022901P'
		-- and color.vtex_color is not null
		-- and p.produto in ('35.01.0828','28.06.2016','35.04.0041','35.04.0041','35.02.0828','35.02.0828','35.02.0828','35.01.0859','35.01.0858','35.01.0860','35.01.0856','35.01.0858','01.02.1008','01.02.1008','01.02.1008','35.02.0829','35.02.0829','35.02.0829','35.01.0825','35.01.0857','35.02.0830','35.02.0835','35.02.0835','35.02.0835','35.02.0833','35.02.0833','35.02.0833','35.02.0834','35.02.0834','35.02.0834','41.01.0102','22.07.0269','22.07.0266','22.07.0269','22.05.0472','22.07.0267','22.03.0243','22.12.0591','22.07.0267','22.07.0260','22.12.0594','22.05.0505','22.05.0486','22.07.0261','22.07.0266','22.05.0526','41.01.0108','22.12.0594','22.07.0263','22.07.0268','41.01.0110','24.04.0536','24.04.0535','22.03.0243','41.01.0109','41.01.0103','41.01.0104','41.01.0109','41.01.0101','41.01.0110','41.01.0106','22.07.0261','22.07.0260','22.14.0008','22.14.0008','22.14.0008','22.07.0263','22.12.0593','41.01.0104','22.05.0526','41.01.0105','41.01.0105','41.01.0108','41.01.0108','41.01.0103','41.01.0101','41.01.0109','41.01.0107','41.01.0102','41.01.0106','22.12.0593','41.01.0105','41.01.0105','24.04.0535','24.04.0536','24.04.0536','22.07.0268','22.07.0266','22.07.0260','22.03.0243','41.01.0104','41.01.0110','41.01.0107','41.01.0107','41.01.0106','41.01.0107','22.12.0591','22.07.0267','22.07.0261','22.05.0505')
		and p.produto = '22.04.0163'
		and pc.COR_PRODUTO in ('01')
		-- and e.estoque_disponivel > 0
		-- and ((p.produto='22.12.0598' and pc.cor_produto='01') OR (p.produto='22.12.0598' and pc.cor_produto='336') OR (p.produto='22.12.0598' and pc.cor_produto='482') OR (p.produto='22.12.0598' and pc.cor_produto='64') OR (p.produto='22.07.0250' and pc.cor_produto='263') OR (p.produto='22.07.0252' and pc.cor_produto='39') OR (p.produto='22.12.0597' and pc.cor_produto='01') OR (p.produto='22.12.0597' and pc.cor_produto='94') OR (p.produto='22.12.0601' and pc.cor_produto='02') OR (p.produto='22.12.0601' and pc.cor_produto='20') OR (p.produto='22.12.0601' and pc.cor_produto='211') OR (p.produto='22.12.0600' and pc.cor_produto='02') OR (p.produto='22.07.0269' and pc.cor_produto='02') OR (p.produto='22.07.0269' and pc.cor_produto='176') OR (p.produto='22.07.0267' and pc.cor_produto='02') OR (p.produto='22.07.0267' and pc.cor_produto='03') OR (p.produto='22.07.0267' and pc.cor_produto='110') OR (p.produto='22.05.0472' and pc.cor_produto='02') OR (p.produto='22.05.0505' and pc.cor_produto='01') OR (p.produto='22.05.0505' and pc.cor_produto='02') OR (p.produto='22.14.0008' and pc.cor_produto='01') OR (p.produto='22.14.0008' and pc.cor_produto='02') OR (p.produto='22.14.0008' and pc.cor_produto='03') OR (p.produto='22.07.0266' and pc.cor_produto='02') OR (p.produto='22.07.0266' and pc.cor_produto='176') OR (p.produto='22.03.0243' and pc.cor_produto='02') OR (p.produto='22.03.0243' and pc.cor_produto='10') OR (p.produto='22.03.0243' and pc.cor_produto='194') OR (p.produto='35.04.0041' and pc.cor_produto='105') OR (p.produto='35.04.0041' and pc.cor_produto='176') OR (p.produto='22.12.0594' and pc.cor_produto='02') OR (p.produto='22.12.0594' and pc.cor_produto='10') OR (p.produto='22.12.0594' and pc.cor_produto='341') OR (p.produto='22.07.0261' and pc.cor_produto='01') OR (p.produto='22.07.0261' and pc.cor_produto='02') OR (p.produto='22.07.0261' and pc.cor_produto='105') OR (p.produto='22.07.0260' and pc.cor_produto='02') OR (p.produto='22.07.0260' and pc.cor_produto='25') OR (p.produto='22.07.0260' and pc.cor_produto='33') OR (p.produto='22.12.0593' and pc.cor_produto='02') OR (p.produto='22.07.0263' and pc.cor_produto='02') OR (p.produto='22.07.0263' and pc.cor_produto='176') OR (p.produto='22.07.0268' and pc.cor_produto='02') OR (p.produto='22.07.0268' and pc.cor_produto='176') OR (p.produto='24.04.0536' and pc.cor_produto='01') OR (p.produto='24.04.0536' and pc.cor_produto='05') OR (p.produto='24.04.0536' and pc.cor_produto='117') OR (p.produto='22.12.0591' and pc.cor_produto='02') OR (p.produto='22.12.0591' and pc.cor_produto='194') OR (p.produto='24.04.0535' and pc.cor_produto='122') OR (p.produto='24.04.0535' and pc.cor_produto='68') OR (p.produto='22.05.0486' and pc.cor_produto='02') )
	group by p.produto, ps.COR_PRODUTO
	;
""", strip=True, dict_format=True)


product_list = []
stock_on = []
stock_off = []
image_on = []
image_off = []

for product_info in products_tested:
	EAN = product_info['ean']

	# --------------------------------------------------------------------------------------------------------------------------------

	webserviceURL = "http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl"

	params = {
	    "Content-Type": "text/xml",
	    "Authorization": "Basic dnRleGFwcGtleS1tYXJjaWFtZWxsby1YTlpGVVg6SEpHVkdVUFVTTVpTRllJSFZQTEpQRkJaUFlCTkxDRkhSWVRUVVRQWlNZVFlDSFRJT1BUSktBQUJISEZIVENJUEdTQUhGT01CWkxSUk1DWEhGU1lXSlZXUlhSTE5PSUdQUERTSkhMRFpDUktaSklQRktZQkJETUZMVklLT0RaTlE=",
	}

	auth = ("vtexappkey-marciamello-XNZFUX","HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ")

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

	params["SOAPAction"] = "http://tempuri.org/IService/StockKeepingUnitGetByEan"
	response = requests.post(webserviceURL, auth=auth, headers=params, data=soap_skuget)

	print("SKU Get %s" % response.status_code)

	soup = Soup(response.text, "xml")
	sku_name = soup.find('a:Name')

	import pdb; pdb.set_trace()

	if sku_name:
		stock_on.append(product_info)
		sku_id = soup.find('a:Id').text

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

		params["SOAPAction"] = "http://tempuri.org/IService/ImageListByStockKeepingUnitId"
		response = requests.post("http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl", auth=auth, headers=params, data=soap_imageget)

		soup = Soup(response.text, "xml")
		image_test_name = soup.find('a:FileLocation')

		if image_test_name:
			image_on.append(product_info)
		else:
			image_off.append(product_info)
	else:
		stock_off.append(product_info)

found_products = search_directory(image_off)

with open('file_stock_off.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";", lineterminator='\n')
	csv_file.writerows({(x['prod_code'], x['cod_color']) for x in stock_off})

with open('file_image_off.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";", lineterminator='\n')
	csv_file.writerows({(x['prod_code'], x['cod_color']) for x in image_off})

with open('file_image_found.csv', 'w') as f:
	csv_file = csv.writer(f, delimiter=";", lineterminator='\n')
	csv_file.writerows({(x['prod_code'], x['cod_color']) for x in found_products})
