from shadow_database import DatabaseConnection
from shadow_vtex.vtex import update_vtex_product
import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup

dc = DatabaseConnection()

# query = """
# SELECT
# 	v_item.product_id, 
# 	p.produto as prod_code
# from dbo.PRODUTOS_BARRA ps
# inner join dbo.PRODUTOS p on p.produto = ps.produto
# inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
# inner join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
# inner join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
# where 1=1
# 	and p.produto in ('35.02.0766','35.02.0833','41.01.0107','35.02.0828','41.01.0106','24.04.0535','24.04.0536','41.01.0101','41.01.0102','22.05.0526','22.02.0240','31.01.0140','35.02.0786','22.12.0562','01.02.1008','32.07.0082','35.01.0842','35.01.0860','41.01.0108','23.08.0217','41.01.0103','22.07.0263','35.01.0821','35.01.0823','41.01.0104','41.01.0105','31.02.0131','22.07.0267','22.12.0587','23.03.0031','35.02.08sorting','22.05.0517','35.02.0787','22.04.0237','35.01.0852','35.09.1208','22.05.0472','22.07.0261','35.02.0834','22.05.0466','35.02.0732','35.02.0805','22.05.0511','35.02.0829','22.05.0512','22.05.0473','35.02.0803','22.05.0513','23.11.0282','31.02.0125','23.01.0003','22.12.0602','22.07.0268','35.01.0857','08.01.188','35.02.0744','08.10.0014','35.02.0764','22.03.0239','23.08.0232','35.02.0835','35.01.0828','35.01.0846','22.12.0591','37.09.0002','33.02.0227','35.09.1201','22.07.0218','31.02.0129','22.05.0467','23.11.0263','32.03.03sorting','23.09.0093','22.12.0576','22.05.0515','22.07.0266','22.07.0258','35.02.0765','22.07.0233','22.05.0461','08.10.0010','32.07.0071','22.14.0008','22.03.0243','35.04.0041','35.01.0739','23.02.0013','32.02.0256','22.07.0260','33.02.0209','32.07.0067','35.02.0776','22.05.0489','33.02.0181','35.01.0820','35.01.0592','32.02.0252','35.01.0632','22.05.0464','22.05.0507','08.11.0002','22.12.0598','33.02.0231','35.06.0035','33.02.0190','22.05.0498','35.08.0007','08.01.187','08.01.186','35.02.0800','22.12.0578','33.02.0197','35.09.1202','22.07.0269','22.03.0242','22.12.0600','22.07.0236','32.02.0257','08.04.023','33.02.0215','22.12.0570','31.05.0006','22.12.0593','23.08.0236','23.11.0280','22.05.0448','35.01.0806','22.12.0543','08.10.0012','23.06.0088','22.07.0226','22.05.0493','22.05.0501','35.07.0034','23.11.0244','22.12.0585','33.02.0211','22.02.0239','35.06.00sorting','22.12.0597','22.03.0240','23.08.0237','22.02.0238','22.13.0065','22.06.0477','32.03.0359','32.07.0080','35.01.0819','22.12.0611','41.01.0109')
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
	error = update_vtex_product(product_info['product_id'], {'Score': str(score)})
	if error:
		print(error)

	score += 1

