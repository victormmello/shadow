from shadow_database import DatabaseConnection
import shadow_google_spreadsheet
from multiprocessing import Pool
import sys, json

MULTI_THREAD = sys.argv[1]
MAX_THREADS = sys.argv[2]
DATE_TO = sys.argv[3]
WORKBOOK_NAME = sys.argv[4]
WORKSHEET_LIST = json.loads(sys.argv[5])

workbook = shadow_google_spreadsheet.open(WORKBOOK_NAME)
dc = DatabaseConnection()

def get_and_set_data(worksheet_tuple):
	print('Updating sheet: %s...' % worksheet_tuple[1],end='')
	worksheet = workbook.worksheet(worksheet_tuple[1])
	query = open(worksheet_tuple[0]).read()  % {
		'date_to':DATE_TO
		}
	# query = """
	# 	SELECT
	# 	p.colecao,
	# 	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
	# 	'' as aux1,
	# 	'' as aux2,
	# 	'' as aux3,
	# 	COUNT(DISTINCT e.produto + e.cor_produto) as produtos
	# 	FROM w_estoque_produtos_transito e
	# 	INNER JOIN produtos p on e.produto = p.produto
	# 	INNER JOIN produtos_precos c on
	# 		e.produto = c.produto and
	# 		c.codigo_tab_preco = 02 -- 02 custo
	# 	LEFT JOIN bi_categorizacao_categoria ccc on
	# 		ccc.grupo_produto = p.grupo_produto and
	# 		ccc.cod_categoria = p.cod_categoria and
	# 		ccc.subgrupo_produto is null and
	# 		ccc.tipo_produto is null
	# 	LEFT JOIN bi_categorizacao_categoria cct on
	# 		cct.grupo_produto = p.grupo_produto and
	# 		cct.tipo_produto = p.tipo_produto and
	# 		cct.subgrupo_produto is null and
	# 		cct.cod_categoria is null
	# 	LEFT JOIN bi_categorizacao_categoria ccs on
	# 		ccs.grupo_produto = p.grupo_produto and
	# 		ccs.subgrupo_produto = p.subgrupo_produto and
	# 		ccs.tipo_produto is null and
	# 		ccs.cod_categoria is null
	# 	LEFT JOIN bi_categorizacao_categoria cc on
	# 		cc.grupo_produto = p.grupo_produto and
	# 		cc.subgrupo_produto is null and
	# 		cc.tipo_produto is null and
	# 		cc.cod_categoria is null
	# 	WHERE
	# 	p.grupo_produto != 'GIFTCARD' and
	# 	c.codigo_tab_preco = 02 and
	# 	e.estoque_disponivel > 0 and
	# 	e.filial in ('iguatemi campinas','santa ursula','mm dom pedro','premium outlet itupeva','santos praiamar','piracicaba shopping','jundiai shopping','e-commerce')
	# 	GROUP BY
	# 	p.colecao,
	# 	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros')
	# """
	query_result = dc.select(query,strip=True)
	columns = [column[0] for column in dc.cursor.description]
	shadow_google_spreadsheet.update_cells_with_dict(worksheet,columns,query_result)
	print('Done!')


if __name__ == '__main__':
	print("Running %s:" % WORKBOOK_NAME)

	if MULTI_THREAD == "true":
		thread_count = len(WORKSHEET_LIST) if MAX_THREADS == "0" else int(MAX_THREADS)
		with Pool(thread_count) as p:
			result = p.map(get_and_set_data, WORKSHEET_LIST.items())
	else:
		for worksheet_tuple in WORKSHEET_LIST.items():
			get_and_set_data(worksheet_tuple)

	print("Done!")
