from shadow_database import DatabaseConnection
from shadow_helpers.helpers import make_dict
from shadow_vtex.vtex import try_to_request

from datetime import datetime, timedelta
from multiprocessing import Pool, Manager
import requests, json, dateutil.relativedelta

def format_int_to_float(int_value):
	str_value = str(int_value)
	return float(str_value[:-2] + '.' + str_value[-2:])

ORDER_STATUS = {
	'waiting-for-seller-confirmation': 'Aguardando confirmação do Seller',
	'order-created': 'Processando',
	'order-completed': 'Processando',
	'on-order-completed': 'Processando',
	'payment-pending': 'Pagamento Pendente',
	'waiting-for-order-authorization': 'Aguardando Autorização do Pedido',
	'approve-payment': 'Preparando entrega',
	'payment-approved': 'Pagamento Aprovado',
	'payment-denied': 'Pagamento Negado',
	'request-cancel': 'Solicitar cancelamento',
	'waiting-for-seller-decision': 'Aguardando decisão do Seller',
	'authorize-fulfillment': 'Aguardando autorização para despachar',
	'order-create-error': 'Erro na criação do pedido',
	'order-creation-error': 'Erro na criação do pedido',
	'window-to-cancel': 'Carência para Cancelamento',
	'ready-for-handling': 'Pronto para o Manuseio',
	'start-handling': 'Iniciar Manuseio',
	'handling': 'Preparando Entrega',
	'invoice-after-cancellation-deny': 'Fatura pós-cancelamento negado',
	'order-accepted': 'Verificando Envio',
	'invoice': 'Enviando',
	'invoiced': 'Faturado',
	'replaced': 'Substituído',
	'cancellation-requested': 'Cancelamento solicitado',
	'cancel': 'Cancelar',
	'canceled': 'Cancelado',
}

api_connection_file = open("api_connection.json", 'rb')
api_connection_config = json.load(api_connection_file)

def get_stock_fix_from_linx(x):
	sku = x[0] 
	fixes_dict = x[1]

	print(sku)
	if sku['sku_id']:
		sku_id = sku['sku_id']
	else:
		get_sku_id_url = 'http://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/%s' % sku['ean']
		response = try_to_request("GET", get_sku_id_url, headers=api_connection_config)
		if not response:
			return 'Sku not found'

		json_response = json.loads(response.text)
		sku_id = json_response['Id']

	fixes_dict[sku_id] = sku['stock_linx']

	# set_stock_url = 'http://logistics.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/%s/warehouses/1_1?an=marciamello' % sku_id
	# response = try_to_request("PUT", set_stock_url, headers=api_connection_config, data='{"quantity": %s}' % sku['stock_linx'])
	# print(sku)

def get_stock_fix_from_orders(x):
	sku = x[0] 
	fixes_dict = x[1]
	
	available_quantity = None
	try:
		available_quantity = fixes_dict[sku['sku_id']]
	except Exception as e:
		return

		get_stock_url = "http://logistics.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/%s?an=marciamello" % sku['sku_id']
		response = try_to_request("GET", get_stock_url, headers=api_connection_config)
		stock_info = json.loads(response.text)
		available_quantity = stock_info['balance'][0]['totalQuantity']

	true_quantity = available_quantity - sku['ordered_quantity']
	print('%s: %s -> %s' % (sku['sku_id'], available_quantity, true_quantity))

	fixes_dict[sku['sku_id']] = true_quantity

	# print('%s: %s' % (sku['sku_id'], stock_info['balance'][0]['totalQuantity']))

	# stock_infos.append([sku['sku_id'], available_quantity])
	# stock_info['balance'][0]['reservedQuantity']

def fix_stock(sku):
	sku_id = sku['sku_id']
	true_quantity = sku['true_quantity']

	if true_quantity < 0:
		print('Estoque negativo: %s (%s)' % (sku_id, true_quantity))
		true_quantity = 0
	else:
		print('%s: %s' % (sku_id, true_quantity))

	set_stock_url = 'http://logistics.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/%s/warehouses/1_1?an=marciamello' % sku_id
	response = try_to_request("PUT", set_stock_url, headers=api_connection_config, data='{"quantity": %s}' % true_quantity)

	if not response:
		print('ERROR: %s' % sku_id)
	# else:
	# 	print(response.text)

if __name__ == '__main__':
	fixes_dict = Manager().dict()
	import time
	start_time = time.time()

	# linx_stock_query = """
	# 	SELECT 
	# 		ps.codigo_barra as ean,
	# 		vpi.item_id as sku_id,
	# 		case  
	# 			when COALESCE(e.estoque_disponivel, 0) - 2 <= 0 then 0 
	# 			else COALESCE(e.estoque_disponivel, 0) - 2
	# 		end	as stock_linx
	# 	from produtos_barra ps
	# 	LEFT JOIN w_estoque_disponivel_sku e on ps.codigo_barra = e.codigo_barra and e.filial = 'e-commerce'
	# 	LEFT JOIN bi_vtex_product_items vpi on ps.codigo_barra = vpi.ean
	# 	where 1=1 
	# 		-- and ps.codigo_barra in ('081000140238','081000140239','22010007216P','2205054102PP','22050545221G','2204015604G','2204015604P','22050577252P','081000140235','0810001426335','2205054101G','22050577507G','081000140237','0810001426339','2205054104M','22050577252G','0810001426337','2205054102P','22050577244P','081000140234','2205054102M','2205054104PP','22050577198P')
	# 		and ps.codigo_barra in (
	# 			SELECT distinct
	# 				ps.codigo_barra	
	# 			FROM LOJA_SAIDAS_PRODUTO lsp
	# 			inner join produtos_barra ps on ps.produto = lsp.produto and ps.cor_produto = lsp.COR_PRODUTO
	# 			where 1=1
	# 				and filial = 'e-commerce'
	# 				and lsp.DATA_PARA_TRANSFERENCIA >= DATEADD(day, -7, GETDATE())
	# 				-- and ps.CODIGO_BARRA = '2207028407P'
	# 			UNION
	# 			SELECT distinct
	# 				vp.CODIGO_BARRA
	# 			FROM loja_venda_produto vp
	# 			INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
	# 			where 1=1
	# 				and f.filial = 'e-commerce'
	# 				and vp.DATA_PARA_TRANSFERENCIA >= DATEADD(day, -7, GETDATE())
	# 				-- and vp.CODIGO_BARRA = '2207028407P'
	# 			UNION
	# 			SELECT distinct
	# 				lvp.CODIGO_BARRA
	# 			FROM dbo.LOJA_VENDA_PRODUTO lvp
	# 			inner join dbo.FILIAIS fil on fil.cod_filial = lvp.codigo_filial
	# 			INNER JOIN produtos p on lvp.produto = p.produto
	# 			where 1=1
	# 				and fil.filial = 'e-commerce' 
	# 				and data_venda >= DATEADD(day, -7, GETDATE())
	# 				and p.grupo_produto != 'GIFTCARD'
	# 				and lvp.qtde > 0
	# 			UNION
	# 			SELECT
	# 				voi.ean
	# 			FROM dbo.bi_vtex_order_items voi 
	# 			WHERE status not in ('Faturado', 'Cancelado')
	# 		)
	# 	;
	# """

	linx_stock_query = """
		SELECT 
			ps.codigo_barra as ean,
			vpi.item_id as sku_id,
			case  
				when COALESCE(e.estoque_disponivel, 0)-COALESCE(fix.sold_qnt,0) - 2 <= 0 then 0 
				else COALESCE(e.estoque_disponivel, 0)-COALESCE(fix.sold_qnt,0) - 2
			end	as stock_linx
		from produtos_barra ps
		LEFT JOIN w_estoque_disponivel_sku e on ps.codigo_barra = e.codigo_barra and e.filial = 'e-commerce'
		LEFT JOIN bi_vtex_product_items vpi on ps.codigo_barra = vpi.ean
		LEFT JOIN (
			SELECT
				vp.CODIGO_BARRA,
				sum(vp.QTDE) as sold_qnt
			FROM loja_venda_produto vp
			INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
			where 1=1
				and f.filial = 'MM DOM PEDRO'
				and vp.DATA_VENDA between '2018-12-23' and '2018-12-26'
			group by vp.CODIGO_BARRA
		) fix on ps.codigo_barra = fix.CODIGO_BARRA
		where 1=1 
			-- and ps.codigo_barra in ('081000140238','081000140239','22010007216P','2205054102PP','22050545221G','2204015604G','2204015604P','22050577252P','081000140235','0810001426335','2205054101G','22050577507G','081000140237','0810001426339','2205054104M','22050577252G','0810001426337','2205054102P','22050577244P','081000140234','2205054102M','2205054104PP','22050577198P')
			and ps.codigo_barra in (
				SELECT distinct
					ps.codigo_barra	
				FROM LOJA_SAIDAS_PRODUTO lsp
				inner join produtos_barra ps on ps.produto = lsp.produto and ps.cor_produto = lsp.COR_PRODUTO
				where 1=1
					and filial = 'e-commerce'
					and lsp.DATA_PARA_TRANSFERENCIA >= DATEADD(day, -7, GETDATE())
					-- and ps.CODIGO_BARRA = '2207028407P'
				UNION
				SELECT distinct
					vp.CODIGO_BARRA
				FROM loja_venda_produto vp
				INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
				where 1=1
					and f.filial = 'e-commerce'
					and vp.DATA_PARA_TRANSFERENCIA >= DATEADD(day, -7, GETDATE())
					-- and vp.CODIGO_BARRA = '2207028407P'
				UNION
				SELECT distinct
					lvp.CODIGO_BARRA
				FROM dbo.LOJA_VENDA_PRODUTO lvp
				inner join dbo.FILIAIS fil on fil.cod_filial = lvp.codigo_filial
				INNER JOIN produtos p on lvp.produto = p.produto
				where 1=1
					and fil.filial = 'e-commerce' 
					and data_venda >= DATEADD(day, -7, GETDATE())
					and p.grupo_produto != 'GIFTCARD'
					and lvp.qtde > 0
				UNION
				SELECT
					voi.ean
				FROM dbo.bi_vtex_order_items voi 
				WHERE status not in ('Faturado', 'Cancelado')
				-- UNION
				-- SELECT distinct
				-- 	vp.CODIGO_BARRA,
				-- FROM loja_venda_produto vp
				-- INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
				-- where 1=1
				-- 	and f.filial = 'MM DOM PEDRO'
				-- 	and vp.DATA_VENDA between '2018-12-23' and '2018-12-26'
			)
		;
	"""


	dc = DatabaseConnection()
	skus_with_different_stock = dc.select(linx_stock_query, strip=True, dict_format=True)

	# skus_with_different_stock = skus_with_different_stock[0:10]
	with Pool(20) as p:
		errors = p.map(get_stock_fix_from_linx, [(x, fixes_dict) for x in skus_with_different_stock])

	dc = DatabaseConnection()
	skus_to_reduce = dc.select("""
		SELECT 
			voi.vtex_sku as sku_id,
			voi.quantity as ordered_quantity,
			voi.status as status
		from dbo.bi_vtex_order_items voi 
		where status not in ('Faturado', 'Cancelado')
			and created_at > '2018-11-21'
			-- and ean like '24040521%'
		order by order_sequence
		;
	""", strip=True, dict_format=True)

	# skus_to_reduce = skus_to_reduce[0:10]

	skus_to_reduce = make_dict(skus_to_reduce, 'ordered_quantity', ['sku_id'], repeated_key='sum')
	skus_to_reduce = [{'sku_id': sku_id, 'ordered_quantity': qnt} for sku_id, qnt in skus_to_reduce.items()]

	with Pool(20) as p:
		errors = p.map(get_stock_fix_from_orders, [(x, fixes_dict) for x in skus_to_reduce])

	with Pool(20) as p:
		errors = p.map(fix_stock, [{'sku_id': sku_id, 'true_quantity': qnt} for sku_id, qnt in fixes_dict.items()])

	# for x in [{'sku_id': sku_id, 'true_quantity': qnt} for sku_id, qnt in fixes_dict.items()]:
	# 	errors = fix_stock(x)

	end_time = time.time()
	print(str(datetime.now()))
	print(end_time-start_time)


	# print('	vtex_stock_backup2')
	# dc.execute('TRUNCATE TABLE vtex_stock_backup2;')
	# dc.insert('vtex_stock_backup2', stock_infos, print_only=False)