from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict, make_dict
from shadow_vtex.vtex import post_to_webservice, authenticated_request
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool, Manager

import os, fnmatch, shutil, requests, csv, json, requests


# --------------------------------------------------------------------------------------------------------------------------------
# py C:\Users\Felipe\Projetos\shadow\vtex_api\ms4.py

dc = DatabaseConnection()

error_list = []

def register_product(product_infos):
	product_dict = {}

	print('--- %s ---' % product_infos[0]['product_refid'])
	for product_info in product_infos:
		print(product_info['ean'])

		ean = product_info['ean']
		brand_id = product_info['brand_id']
		description_color = product_info['desc_vtex_color']
		size = product_info['size']
		department_id = product_info['vtex_department_id']
		category_id = product_info['vtex_category_id']
		product_name = product_info['product_name']
		list_price = round(float(product_info['list_price']), 2)
		price = round(float(product_info['price'] or product_info['list_price']), 2)
		cost_price = product_info['cost_price']
		product_refid = product_info['product_refid']
		link_id = product_name.replace(' ','-') + '-' +  product_refid.replace('.','-')
		keywords = product_name + ', ' + product_refid
		product_id = None
		estoque_disponivel = product_info['estoque_disponivel']

		if None in (brand_id, description_color, category_id, department_id):
			return 'Error none ' + ean

		soap_skuget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
		   <soapenv:Header/>
		   <soapenv:Body>
			  <tem:StockKeepingUnitGetByEan>
				 <!--Optional:-->
				 <tem:EAN13>%s</tem:EAN13>
			  </tem:StockKeepingUnitGetByEan>
		   </soapenv:Body>
		</soapenv:Envelope>""" % (ean)

		soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitGetByEan", soap_skuget)

		if not soup:
			return 'skuget no response: %s' %  ean
			
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
					return ('error: soap_productget %s' % ean)
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
				if not soup:
					return ('error: soap_create_product %s' % ean)

				product_id = soup.find('a:Id').text

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
				'ean':ean,
				'ean':ean,
				'sku_weightkg':sku_weightkg,
				'sku_width':sku_width
			}

			soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitInsertUpdate", soap_skuupdate)

			if not soup:
				return ('error: soap_skuupdate %s' % ean)

			if soup.find('a:Id'):
				sku_id = soup.find('a:Id').text
				set_stock_url = 'http://logistics.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/%s/warehouses/1_1?an=marciamello' % sku_id
				response = authenticated_request("PUT", set_stock_url, data='{"quantity": %s}' % sku_id)

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
			COALESCE(pp.preco_liquido1, pp.preco1*(1-pp.PROMOCAO_DESCONTO/100)) as price,
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
			-- and p.produto in ('08.01.0202','08.01.0204','08.01.186','08.01.188','08.01.190','08.01.191','08.01.192','08.01.193','08.01.195','08.01.197','08.01.198','08.01.199','08.04.022','08.04.023','08.04.024','08.06.0080','08.06.0081','08.08.0014','08.08.0015','08.08.0016','08.09.0003','08.10.0010','08.10.0012','08.10.0013','08.11.0006','08.11.0007')
			and p.produto='35.02.0771' AND pc.cor_produto='10'
		order by ps.CODIGO_BARRA
		;
	"""
	# print(query)
	products_to_register = dc.select(query, strip=True, dict_format=True)
	# products_to_register = products_to_register[:10]
	# print(products_to_register)
	# raise Exception(len(products_to_register))
	products_by_refid_dict = make_dict(products_to_register, None, ['product_refid'], repeated_key='append')

	errors = []

	with Pool(20) as p:
		errors = p.map(register_product, products_by_refid_dict.values())
	errors = [x for x in errors if x]
	print(errors)

	raise Exception()
	# ean_soup_dict = {ean:soup for ean, soup in ean_soup_tuples}
	# for product_to_register in products_to_register:
	# 	product_to_register['skuget_rest_str'] = ean_soup_dict.get(product_to_register['ean'], None)

	# Rodar sem thread:
	print('comecando cadastro')
	# products_to_register = products_to_register[:1]
	for product_to_register in products_to_register:
		error = register_product(product_to_register)
		errors.append(error)
