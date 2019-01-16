from shadow_database import DatabaseConnection
# from shadow_database.shadow_helpers import make_dict, get_from_dict
import os, fnmatch, shutil, requests, csv, json, requests
from bs4 import BeautifulSoup as Soup
from datetime import datetime, timedelta

dc = DatabaseConnection()

query = """
	SELECT 
voi.order_sequence, 
voi.created_at, 
voi.ean, 
voi.name, 
COALESCE(vpi.imagem,'') as imagem, 
voi.courier, 
voi.city, 
voi.postal_code, 
voi.neighborhood, 
voi.street, 
voi.street_number, 
voi.complement, 
voi.client_name, 
voi.quantity as quantidade_venda, 
CASE WHEN p.fabricante = 'CATIVA' THEN 'CATIVA' ELSE 'ESTOQUE' END as grupo_posicao, 
CASE WHEN p.fabricante = 'CATIVA' THEN p.refer_fabricante ELSE '' END as posicoes,
COALESCE(e.estoque,0) as estoque,
voi.status
FROM bi_vtex_order_items voi 
LEFT JOIN ( 
	SELECT 
	e.codigo_barra, 
	SUM(e.estoque_disponivel) as estoque
	FROM w_estoque_disponivel_sku e 
	WHERE e.filial = 'e-commerce'
	GROUP BY 
	e.codigo_barra 
) e on 
	e.codigo_barra = voi.ean 
INNER JOIN produtos_barra pb on pb.codigo_barra = voi.ean 
INNER JOIN produtos p on p.produto = pb.produto 
LEFT JOIN ( 
	SELECT 
	produto, 
	cor_produto, 
	MIN(vpi.image_url) as imagem 
	FROM bi_vtex_product_images vpi 
	GROUP BY 
	produto, 
	cor_produto 
) vpi on vpi.produto = pb.produto and vpi.cor_produto = pb.cor_produto 
WHERE 1=1
-- and voi.created_at >= GETDATE()-14 
and voi.order_sequence IN ('558432','558598','558813','559025','559085','559161','559499','559700','559951','560004','560018','560053','560253','560260','560354','560563','560840','560865','560907','560945','560970','560974','561041','561194','561268','561304','561421','561528','561549','561766','561857','561882','561966','561973','562000','562092','562162','562235','562771','562786','562791','562797')
and COALESCE(e.estoque,0) = 0
ORDER BY
CASE voi.courier WHEN 'Retira Fácil - MM Cambuí' THEN 0 WHEN 'SEDEX' THEN 1 ELSE 2 END, 
voi.order_sequence
;
"""

results = dc.select(query, strip=True)
columns = [column[0] for column in dc.cursor.description]

with open('query_result.csv', 'w', newline='\n') as csvfile:
	writer = csv.writer(csvfile, delimiter=';')
	writer.writerow(columns)
	writer.writerows(results)
