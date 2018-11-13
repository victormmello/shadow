from shadow_database import DatabaseConnection
from datetime import datetime

product_dict = {
	'_BLAZER':{
		'MANGA LONGA':{
			'products':[
				{
					'descricao':'Produto teste',
					'colecao':'V2019',
					'cod_categoria':'01',
					'cod_subcategoria':'01',
					'referencia':'123',
					'grade':'XPP/PP/P/M/G/GG/XGG',
					'grupo_produto':'_BLAZER',
					'subgrupo_produto':'MANGA LONGA',
					'tipo_produto':'ALFAIATARIA',
					'griffe':'MARCIA MELLO',
					'linha':'CASUAL',
					'fabricante':'ALPELO',
					'ncm':'6106.20.00',
					'custo':59,
					'preco_original':239,
					'preco_por':189,
					'tipo_cod_barras':'04',
					'cores':{
						'05':'AZUL',
						'13':'MARROM'
					},
					'classificacao':{
						'manga':'',
						'cumprimento':'',
						'detalhes':['']
					}
				}
			]
		}
	}
}

current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
dc = DatabaseConnection()

subgroup_filter_list = []
for group in product_dict:
	for subgroup in product_dict[group]:
		subgroup_filter_list.append("(ps.grupo_produto = '%s' and ps.subgrupo_produto = '%s')" % (group,subgroup))

query = """
	SELECT
	RTRIM(ps.grupo_produto) as grupo_produto,
	RTRIM(ps.subgrupo_produto) as subgrupo_produto,
	RTRIM(pg.codigo_grupo) as cod_grupo,
	RTRIM(ps.codigo_subgrupo) as cod_subgrupo,
	ps.codigo_sequencial as sequence_code
	FROM produtos_subgrupo ps
	INNER JOIN produtos_grupo pg on pg.grupo_produto = ps.grupo_produto 
	WHERE
	ps.inativo = 0 and
	pg.inativo = 0 and
	(
		%s
	)
""" % '\n OR '.join(subgroup_filter_list)
sequence_codes = dc.select(query)
for sequence_code in sequence_codes:
	product_dict[sequence_code[0]][sequence_code[1]]['group_code'] = sequence_code[2]
	product_dict[sequence_code[0]][sequence_code[1]]['subgroup_code'] = sequence_code[3]
	product_dict[sequence_code[0]][sequence_code[1]]['current_sequence_code'] = sequence_code[4]
	product_dict[sequence_code[0]][sequence_code[1]]['final_sequence_code'] = sequence_code[4]

sequence_code_update_query_list = []
product_insert_query_values_list = []
product_price_insert_query_values_list = []
product_color_insert_query_values_list = []
product_size_insert_query_values_list = []
for group in product_dict:
	for subgroup in product_dict[group]:
		subgroup_dict = product_dict[group][subgroup]
		for product in subgroup_dict['products']:
			sequence_code = '%04d' % (int(subgroup_dict['final_sequence_code']) + 1)
			if (int(subgroup_dict['final_sequence_code']) + 1) > 9999:
				raise Exception('Codigo Sequencial maior que 9999!')

			subgroup_dict['final_sequence_code'] = sequence_code
			product['sequence_code'] = sequence_code
			product['produto'] = '.'.join((subgroup_dict['group_code'],subgroup_dict['subgroup_code'],sequence_code))
			product_insert_query_values_list.append([
				product['produto'],
				product['descricao'],
				product['grupo_produto'],
				product['subgrupo_produto'],
				1,
				product['ncm'],
				product['tipo_produto'],
				product['colecao'],
				product['grade'],
				product['descricao'],
				product['linha'],
				product['griffe'],
				'PC',
				1,
				product['referencia'],
				product['fabricante'],
				'111111111111111111111111111111111111111111111111',
				'00',
				0,
				current_timestamp,
				1,
				1,
				2,
				current_timestamp,
				'01',
				3,
				1,
				'11110001',
				11,
				1,
				'11110001',
				'31101001',
				'11110001',
				'31102001',
				product['cod_categoria'],
				product['cod_subcategoria'],
				0,
				0,
				0,
				0,
				'04',
				0
			])

			product['promocao_desconto'] = 100 * (1-float(product['preco_por'])/float(product['preco_original']))
			product_price_insert_query_values_list.append([product['produto'],'02',product['custo'],current_timestamp,product['custo'],0,0,0,0])
			product_price_insert_query_values_list.append([product['produto'],'99',product['preco_original'],current_timestamp,product['preco_original'],0,0,0,0])
			# product_price_insert_query_values_list.append([product['produto'],'01',product['preco_original'],current_timestamp,product['preco_por'],0,0,0,product['promocao_desconto']])
			# product_price_insert_query_values_list.append([product['produto'],'10',product['preco_original'],current_timestamp,product['preco_por'],0,0,0,product['promocao_desconto']])
			# product_price_insert_query_values_list.append([product['produto'],'11',product['preco_original'],current_timestamp,product['preco_por'],0,0,0,product['promocao_desconto']])
			# product_price_insert_query_values_list.append([product['produto'],'97',product['preco_por'],current_timestamp,product['preco_por'],0,0,0,0])
			
			for color in product['cores']:
				product_color_insert_query_values_list.append([
					product['produto'],
					color,
					product['cores'][color],
					'1990-01-01 00:00:00',
					'2050-12-31 00:00:00',
					0,
					'1',
					color,
					0,
					0,
					0,
					product['ncm'],
					'0'
				])
				size_number = 1
				for size in product['grade'].split('/'):
					barcode_size = size
					if size == 'UNICO':
						size = 'U'
						barcode_size = 'M'

					barcode = subgroup_dict['group_code']+subgroup_dict['subgroup_code']+sequence_code+color+barcode_size
					product_size_insert_query_values_list.append([
						product['produto'],
						color,
						barcode,
						size,
						size_number,
						0,
						product['tipo_cod_barras']
					])
					size_number += 1

	sequence_code_update_query_list.append("UPDATE produtos_subgrupo SET codigo_sequencial = '%s' WHERE inativo = 0 and grupo_produto = '%s' and subgrupo_produto = '%s';" % (sequence_code,group,subgroup))

product_columns = ['PRODUTO','DESC_PRODUTO','GRUPO_PRODUTO','SUBGRUPO_PRODUTO','FATOR_OPERACOES','CLASSIF_FISCAL','TIPO_PRODUTO','COLECAO','GRADE','DESC_PROD_NF','LINHA','GRIFFE','UNIDADE','REVENDA','REFER_FABRICANTE','FABRICANTE','PONTEIRO_PRECO_TAM','TRIBUT_ICMS','TRIBUT_ORIGEM','DATA_REPOSICAO','TAMANHO_BASE','ENVIA_LOJA_VAREJO','TAXA_JUROS_DEFLACIONAR','DATA_CADASTRAMENTO','STATUS_PRODUTO','TIPO_STATUS_PRODUTO','EMPRESA','CONTA_CONTABIL','INDICADOR_CFOP','QUALIDADE','CONTA_CONTABIL_COMPRA','CONTA_CONTABIL_VENDA','CONTA_CONTABIL_DEV_COMPRA','CONTA_CONTABIL_DEV_VENDA','COD_CATEGORIA','COD_SUBCATEGORIA','PERC_COMISSAO','ACEITA_ENCOMENDA','DIAS_GARANTIA_LOJA','DIAS_GARANTIA_FABRICANTE','TIPO_ITEM_SPED','POSSUI_GTIN']
dc.insert('PRODUTOS',product_insert_query_values_list,columns=product_columns,print_only=True)
product_color_columns = ['PRODUTO','COR_PRODUTO','DESC_COR_PRODUTO','INICIO_VENDAS','FIM_VENDAS','COR_SORTIDA','STATUS_VENDA_ATUAL','COR','CUSTO_REPOSICAO1','PRECO_REPOSICAO_1','PRECO_A_VISTA_REPOSICAO_1','CLASSIF_FISCAL','TRIBUT_ORIGEM']
dc.insert('PRODUTO_CORES',product_color_insert_query_values_list,columns=product_color_columns,print_only=True)
product_price_columns = ['PRODUTO','CODIGO_TAB_PRECO','PRECO1','ULT_ATUALIZACAO','PRECO_LIQUIDO1','PRECO_LIQUIDO2','PRECO_LIQUIDO3','PRECO_LIQUIDO4','PROMOCAO_DESCONTO']
dc.insert('PRODUTOS_PRECOS',product_price_insert_query_values_list,columns=product_price_columns,print_only=True)
product_size_columns = ['PRODUTO','COR_PRODUTO','CODIGO_BARRA','GRADE','TAMANHO','CODIGO_BARRA_PADRAO','TIPO_COD_BAR']
dc.insert('PRODUTOS_BARRA',product_size_insert_query_values_list,columns=product_size_columns,print_only=True)
sequence_code_update_query = '\n'.join(sequence_code_update_query_list)
print(sequence_code_update_query)