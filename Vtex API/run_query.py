from shadow_database import DatabaseConnection
# from shadow_database.shadow_helpers import make_dict, get_from_dict
import os, fnmatch, shutil, requests, csv, json, requests
from bs4 import BeautifulSoup as Soup
from datetime import datetime, timedelta

dc = DatabaseConnection()

query = """
	select 
		p.DESC_PRODUTO, pc.DESC_COR_PRODUTO, e.estoque, ev.estoque, w.*
	from bi_vtex_product_waterfall w
	inner join produtos p on p.produto = w.produto
	inner join produto_cores pc on pc.produto = w.produto and pc.COR_PRODUTO = w.cor_produto
	LEFT join (
		SELECT produto, cor_produto, sum(estoque_disponivel) as estoque
		from w_estoque_disponivel_sku
		where filial = 'e-commerce'
		group by produto, cor_produto
	) e on pc.produto = e.produto and pc.cor_produto = e.cor_produto
	LEFT join (
		SELECT ps.produto, ps.cor_produto, sum(vpi.stock_quantity) as estoque
		from bi_vtex_product_items vpi
		inner join produtos_barra ps on ps.codigo_barra = vpi.ean
		group by ps.produto, ps.cor_produto
	) ev on pc.produto = ev.produto and pc.cor_produto = ev.cor_produto
	where 1=1
		-- and w.online = 1 and (w.has_ean = 0 or w.integrado = 0 or w.imagem_vtex = 0 or w.ativo = 0)
		and ev.estoque > 0 and w.imagem_vtex = 1 and w.online = 0
		-- and w.produto='35.02.0766' -- and w.cor_produto = '10'
	;
"""

results = dc.select(query, strip=True)

with open('query_result.csv', 'w', newline='\n') as csvfile:
	writer = csv.writer(csvfile, delimiter=';')
	writer.writerows(results)
