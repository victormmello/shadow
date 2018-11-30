from shadow_database import DatabaseConnection
# from shadow_database.shadow_helpers import make_dict, get_from_dict
import os, fnmatch, shutil, requests, csv, json, requests
from bs4 import BeautifulSoup as Soup
from datetime import datetime, timedelta

dc = DatabaseConnection()

query = """
	SELECT
		pp.PRODUTO,
		case
			when pp.CODIGO_TAB_PRECO = '01' then 'Outras'
			when pp.CODIGO_TAB_PRECO = '10' then 'Outlet'
			when pp.CODIGO_TAB_PRECO = '11' then 'e-commerce'
		end as filial,
		pp.preco1 as original_price,
		pp.PRECO_LIQUIDO1 as sale_price
	from produtos_precos pp
	where 1=1 
		and pp.CODIGO_TAB_PRECO in ('01', '10', '11')
		and exists (
			select 1 
			from W_ESTOQUE_DISPONIVEL_SKU e 
			where 
			e.produto = pp.produto and
			case e.filial
				when 'E-commerce' then 11 
				when 'Premium outlet itupeva' then 10 
				else 1 
			end = pp.CODIGO_TAB_PRECO)
	;
"""

results = dc.select(query, strip=True)

with open('query_result.csv', 'w', newline='\n') as csvfile:
	writer = csv.writer(csvfile, delimiter=';')
	writer.writerows(results)
