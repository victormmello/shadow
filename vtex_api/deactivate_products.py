from shadow_database import DatabaseConnection
from shadow_vtex.vtex import update_vtex_product, post_to_webservice
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup
from multiprocessing import Pool

# --------------------------------------------------------------------------------------------------------------------------------


def f(product_info):
	ean = product_info['ean']

	soap_skuget = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
	   <soapenv:Header/>
	   <soapenv:Body>
		  <tem:StockKeepingUnitGetByEan>
			 <tem:EAN13>%s</tem:EAN13>
		  </tem:StockKeepingUnitGetByEan>
	   </soapenv:Body>
	</soapenv:Envelope>""" % (ean)

	soup = post_to_webservice("http://tempuri.org/IService/StockKeepingUnitGetByEan", soap_skuget)
	if not soup:
		return 'error: soap_skuget %s' % ean

	if not soup.find('a:Id'):
		return 'error: soap_skuget - no ID %s' % ean

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
	sku_weightkg = soup.find('a:WeightKg').text
	# sku_weightkg = "300"
	sku_width = soup.find('a:Width').text
	sku_cubicweight = soup.find('a:CubicWeight').text
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

	if not soup:
		return 'error: soap_skuupdate %s' % ean

	if not soup.find('a:Id'):
		return 'error: soap_skuupdate - no ID %s' % ean


	update_vtex_product(product_id, {'IsActive': 'false'}, dafiti_store=True)	

if __name__ == '__main__':
	filter_str = "p.produto in ('07.44.009', '07.44.011')"

	query = """
		SELECT distinct
			v_item.product_id,
			ps.codigo_barra as ean
		from PRODUTOS_BARRA ps
		inner join PRODUTOS p on p.produto = ps.produto
		inner join PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
		inner join produtos_precos pp on pp.produto = ps.produto and pp.codigo_tab_preco = '11'
		left join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
		inner join bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
		where 1=1
			and (%s)
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