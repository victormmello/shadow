from shadow_database import DatabaseConnection
from shadow_vtex.vtex import update_vtex_product
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup

dc = DatabaseConnection()

# query = """
# SELECT
# 	v_item.product_id, 
# 	p.produto as produto
# from dbo.PRODUTOS_BARRA ps
# inner join dbo.PRODUTOS p on p.produto = ps.produto
# inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
# inner join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
# inner join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
# where 1=1
# 	and p.produto in ('23.09.0059','31.05.0003','35.02.0338','31.02.0110','35.02.0767','24.03.0030','22.02.0243','35.04.0025','25.05.0006','24.04.0521','37.05.0026','35.02.0599','22.12.0607','35.01.0489','37.05.0019','35.09.0612','22.07.0256','34.02.0032','32.05.0007')
# group by v_item.product_id, p.produto
# ;""" 

query = """
	SELECT
	ppc.produto,
	MAX(vp.product_id) as product_id,
	SUM(ppc.potencial_receita_sorting) as potencial_receita_sorting
	from bi_potencial_produto_cor ppc
	inner join produtos p on p.produto = ppc.produto
	inner join bi_vtex_products vp on vp.produto = ppc.produto
	inner join bi_disponibilidade_produto_cor dpc on
	dpc.produto = ppc.produto and
	dpc.cor_produto = ppc.cor_produto and
	dpc.filial = 'E-COMMERCE'
	group by
	ppc.produto
	ORDER BY
	CASE WHEN MAX(dpc.disponibilidade_filial) >= 0.5 THEN 1 ELSE 0 END asc,
	SUM(ppc.potencial_receita_sorting) asc;
"""

products_to_set_score = dc.select(query, strip=True, dict_format=True)

# raise Exception(len(products_to_set_score))

score = 0
for product_info in products_to_set_score:
	print("%s: %s" % (product_info['produto'], score))
	error = update_vtex_product(product_info['product_id'], {
		'Score': str(score),
		'Description': '%(Name)s',
		'DescriptionShort': '%(Name)s',
	})
	if error:
		print(error)

	score += 1

