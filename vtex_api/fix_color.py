from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
from shadow_vtex.vtex import post_to_webservice
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool, Manager

# --------------------------------------------------------------------------------------------------------------------------------

def f(product_info):
	ean = product_info['ean']
	if not product_info['vtex_color']:
		return 'sem cor: %s' % ean

	print(product_info)
	sku_size = product_info['size']
	sku_color = product_info['vtex_color']

	# --------------------------------------------------------------------------------------------------------------------------------

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
		return 'error: soap_skuget %s' % ean

	if not soup.find('a:Id'):
		return 'error: soap_skuget - no ID %s' % ean

	sku_id = soup.find('a:Id').text

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
		return 'error: soap_size %s' % ean
	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitEspecificationInsert", soap_color)
	if not soup:
		return 'error: soap_color %s' % ean


if __name__ == '__main__':
	# filter_str = """p.produto in ('20.02.0005','22.02.0238','22.03.0188','22.03.0248','22.05.0577','22.06.0333','22.06.0457','22.12.0483','22.15.0007','23.11.0206','31.02.0070','32.06.0051','33.02.0232','35.01.0748','35.02.0785','35.09.1000','35.09.1044')"""

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
			-- and p.produto in (
			-- 	select distinct
			-- 		vp.produto
			-- 	from bi_vtex_product_items vpi
			-- 	inner join bi_vtex_products vp on vpi.product_id = vp.product_id
			-- 	where vpi.vtex_color = 'None'
			-- )
			and p.produto = '22.05.0577'
		order by ps.CODIGO_BARRA
		;
	"""
	# print(query)
	products_to_register = dc.select(query, strip=True, dict_format=True)
	# print(products_to_register)

	errors = []
	# Rodar sem thread:
	# for product_to_register in products_to_register:
	# 	errors.append(f(product_to_register))

	# ============== nao rodar com threads ainda
	with Pool(20) as p:
		errors = p.map(f, products_to_register)

	errors = [x for x in errors if x]
	print(errors)