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

	# print(sku)
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
	dc = DatabaseConnection()

	divergence_query = """
		SELECT
			count(*) as divergences,
			sum(mudanca_detectada) as detected,
			count(*) - sum(mudanca_detectada) as undetected
		from (
			select 
				vpi.ean, 
				vpi.item_id,
				COALESCE(e.estoque_disponivel, 0) as linx, 
				vpi.stock_quantity as vtex,
				case when vpi.ean in (
					SELECT distinct
						ps.codigo_barra
					FROM loja_saidas ls
					INNER JOIN loja_saidas_produto lsp on lsp.ROMANEIO_PRODUTO = ls.ROMANEIO_PRODUTO
					inner join produtos_barra ps on ps.produto = lsp.produto and ps.COR_PRODUTO = lsp.COR_PRODUTO
					where 1=1
						and ls.filial = 'e-commerce'
						and case 
							when lsp.DATA_PARA_TRANSFERENCIA > ls.DATA_PARA_TRANSFERENCIA then lsp.DATA_PARA_TRANSFERENCIA
							else ls.DATA_PARA_TRANSFERENCIA 
						end >= DATEADD(day, -7, GETDATE())
					UNION
					SELECT distinct
						ps.codigo_barra
					FROM loja_entradas le
					INNER JOIN loja_entradas_produto lep on lep.ROMANEIO_PRODUTO = le.ROMANEIO_PRODUTO
					inner join produtos_barra ps on ps.produto = lep.produto and ps.COR_PRODUTO = lep.COR_PRODUTO
					where 1=1
						and le.filial = 'e-commerce'
						and case 
							when lep.DATA_PARA_TRANSFERENCIA > le.DATA_PARA_TRANSFERENCIA then lep.DATA_PARA_TRANSFERENCIA
							else le.DATA_PARA_TRANSFERENCIA 
						end >= DATEADD(day, -7, GETDATE())
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
					-- 	vp.CODIGO_BARRA
					-- FROM loja_venda_produto vp
					-- INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
					-- where 1=1
					-- 	and f.filial = 'MM DOM PEDRO'
					-- 	and vp.DATA_VENDA >= '2018-12-21'
				) then 1 else 0 end as mudanca_detectada
			from (
				SELECT * 
				FROM w_estoque_disponivel_sku
				where filial = 'e-commerce'
			) e
			full outer join bi_vtex_product_items vpi on vpi.ean = e.codigo_barra
			where 1=1
				and vpi.stock_quantity != COALESCE(e.estoque_disponivel,0)
				-- and vpi.ean = '0810001026339'
		) t
	;
	"""

	divergence_skus = dc.select(divergence_query, strip=True, dict_format=True)

	print(divergence_skus)

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
			COALESCE(e.estoque_disponivel, 0)  as stock_linx
		from produtos_barra ps
		LEFT JOIN w_estoque_disponivel_sku e on ps.codigo_barra = e.codigo_barra and e.filial = 'e-commerce'
		LEFT JOIN bi_vtex_product_items vpi on ps.codigo_barra = vpi.ean
		where 1=1 
			and ps.codigo_barra in (
				-- produtos com mudanca de estoque em X dias
				SELECT distinct
					ps.codigo_barra
				FROM loja_saidas ls
				INNER JOIN loja_saidas_produto lsp on lsp.ROMANEIO_PRODUTO = ls.ROMANEIO_PRODUTO
				inner join produtos_barra ps on ps.produto = lsp.produto and ps.COR_PRODUTO = lsp.COR_PRODUTO
				where 1=1
					and ls.filial = 'e-commerce'
					and case 
						when lsp.DATA_PARA_TRANSFERENCIA > ls.DATA_PARA_TRANSFERENCIA then lsp.DATA_PARA_TRANSFERENCIA
						else ls.DATA_PARA_TRANSFERENCIA 
					end >= DATEADD(day, -7, GETDATE())
				UNION
				SELECT distinct
					ps.codigo_barra
				FROM loja_entradas le
				INNER JOIN loja_entradas_produto lep on lep.ROMANEIO_PRODUTO = le.ROMANEIO_PRODUTO
				inner join produtos_barra ps on ps.produto = lep.produto and ps.COR_PRODUTO = lep.COR_PRODUTO
				where 1=1
					and le.filial = 'e-commerce'
					and case 
						when lep.DATA_PARA_TRANSFERENCIA > le.DATA_PARA_TRANSFERENCIA then lep.DATA_PARA_TRANSFERENCIA
						else le.DATA_PARA_TRANSFERENCIA 
					end >= DATEADD(day, -7, GETDATE())
				UNION
				SELECT distinct
					lvp.CODIGO_BARRA
				FROM dbo.LOJA_VENDA_PRODUTO lvp
				inner join dbo.FILIAIS fil on fil.cod_filial = lvp.codigo_filial
				INNER JOIN produtos p on lvp.produto = p.produto
				where 1=1
					and fil.filial = 'e-commerce' 
					and lvp.data_venda >= DATEADD(day, -7, GETDATE())
					and p.grupo_produto != 'GIFTCARD'
					and lvp.qtde > 0
				UNION
				SELECT
					voi.ean
				FROM dbo.bi_vtex_order_items voi 
				WHERE status not in ('Faturado', 'Cancelado')
				-- UNION
				-- SELECT distinct
				-- 	vp.CODIGO_BARRA
				-- FROM loja_venda_produto vp
				-- INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
				-- where 1=1
				-- 	and f.filial = 'MM DOM PEDRO'
				-- 	and vp.DATA_VENDA >= '2018-12-21'
				-- produtos com estoque divergente
				UNION
				select distinct
					vpi.ean
				from (
					SELECT * 
					FROM w_estoque_disponivel_sku
					where filial = 'e-commerce'
				) e
				full outer join bi_vtex_product_items vpi on vpi.ean = e.codigo_barra
				where 1=1
					and vpi.stock_quantity != COALESCE(e.estoque_disponivel,0)
				-- manual
				-- UNION
				-- ps.codigo_barra in ('08011940238','080119426334','080119426338','080119426339','081000100234','081000100239','2205047202P','2205047204M','22050472105GG','2205051702P','22050537244M','22050577244P','22070215218G','2207026033GG','2212061502P','2212061602G','2212061602PP','2212061604M','22120616105GG','22120616105M','22120616155P','23110186461XGG','23110206453GG','3102012908M','33020232105GG','33020232216M','3302023232GG','3302023232P','3501049202PP','35010845217P','35091084135P','4101010301G','08011940234','08011940237','080119426335','081000120234','081000120237','081000120238','0810001226334','0810001226337','2205047206GG','22050472105G','2205052304M','2205052304P','22050523176GG','22050537229P','22050545221G','22070215218XPP','22120576467M','22120615176P','22120616105P','22120616155GG','22120616155M','23110186461G','24040535122M','31010140105P','31010140105PP','35040041105P','3509120101M','3509120101P','4101011802G','0810001026334','0810001026336','2205047202GG','2205047206G','2205051104M','2205052302GG','22050537229M','22050537244P','22050573217P','2207026025P','2212057802P','2212061507G','2212061604G','2212061604P','22120616176G','22120616176M','3101014001PP','31010140176G','31010140176GG','31010140176P','3207006310M','3302021204G','3302023202GG','3302023202P','33020232259G','33020232457P','35091084135M','4101010801G','4101011864G','081000100236','081000100237','0810001026338','22030248105M','22050472105P','2205051104G','2205052302M','22050537229G','2205057502P','22050577252P','2212061502G','2212061507M','2212061604PP','22120616105G','2305010102P','2308023801M','23110186461M','23110206453P','31010140176PP','3302021207M','33020232216G','33020232259M','3302023232M','3501049202P','35020750105P','35040041105GG','35040041176GG','35040041176P','4101011802P','08011940235','0810001026335','081000120235','2205047202G','22050537252P','22050577252G','2207026025M','2207026033P','22120576467G','2212061507GG','22120615176GG','22120615176M','22120616155G','2404053503XGG','3101014001G','3302023202G','33020232259GG','35010845217M','3509120104P','35091201155P','4101010801GG','4101010801M','08011940236','08011940239','080119426336','080119426337','081000120236','081000120239','0810001226335','0810001226336','2205047204GG','2205047207M','22050472105M','22050517176P','22050523176M','22050537244G','22050537252M','2205057464P','2207026025G','2212061602P','22120616176P','2305010102G','2308023801G','23110186461GG','24040535122GG','2404053568M','3202021602P','33020232105P','3302023232G','35040041105G','35040041176G','3509120104M','35091201155M','4101010301GG','081000100238','2205047202M','2205047204G','2205047207G','2205047207GG','2205051702G','2205052302G','2205052302P','2205052304G','22050523176P','22050537252G','22050575176P','22050577507G','2212057805GG','2212061602M','22120616155PP','22120616176GG','2305010102M','2308023801P','31010140105G','35040041105M','35040041176M','4101011802M','4101011864M','081000100235','0810001026337','0810001026339','2205051702M','22050523176G','2212057805M','2212061502GG','22120615176G','2212061602GG','22120616176PP','2404053503GG','2404053568XGG','3101014001GG','3101014001P','31010140105M','3302023202M','3501049202M','35020750105GG','35091201155G','4101010808GG','3501049204P','31010140176M','2212061502M','3101014001M')
			)
		;
	"""

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
		where voi.status not in ('Faturado', 'Cancelado')
			and voi.created_at > '2018-12-26 10:03:33'
			-- and ean like '24040521%'
		order by voi.order_sequence
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
	print(end_time-start_time)


	# print('	vtex_stock_backup2')
	# dc.execute('TRUNCATE TABLE vtex_stock_backup2;')
	# dc.insert('vtex_stock_backup2', stock_infos, print_only=False)