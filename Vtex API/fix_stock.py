import requests, json, dateutil.relativedelta
from datetime import datetime, timedelta
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import make_dict
from shadow_vtex.vtex import try_to_request
from multiprocessing import Pool, Manager


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

# api_connection_file = open("api_connection.json", 'rb')
# api_connection_config = json.load(api_connection_file)

api_connection_config = {
	"Content-Type": "application/json",
	"X-VTEX-API-AppKey": "vtexappkey-marciamello-XNZFUX",
	"X-VTEX-API-AppToken": "HJGVGUPUSMZSFYIHVPLJPFBZPYBNLCFHRYTTUTPZSYTYCHTIOPTJKAABHHFHTCIPGSAHFOMBZLRRMCXHFSYWJVWRXRLNOIGPPDSJHLDZCRKZJIPFKYBBDMFLVIKODZNQ"
}
# list_orders_url = 'http://marciamello.vtexcommercestable.com.br/api/oms/pvt/orders'

# end_date = datetime.now()
# start_date = datetime(year=end_date.year, month=end_date.month, day=end_date.day, hour=2, minute=0, second=0) - timedelta(days=8)

# print('start_date: %s' % start_date)
# print('end_date: %s' % end_date)

# formatted_end_date = (end_date + timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
# formatted_start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
# params = {
# 	'orderBy': 'creationDate,desc',
# 	'per_page': '15',
# 	'f_creationDate': 'creationDate:[%sZ TO %sZ]' % (formatted_start_date, formatted_end_date)
# }


# print('creationDate:[¨%sZ TO %sZ]' % (formatted_start_date, formatted_end_date))

# with open('order_output.csv', 'wb') as f:
# order_items = []
# total_items = 1
# skus_to_reduce_bypass = []

# c=0
# while (c*15) < total_items:
# 	c+=1
# 	print(u"Page: %d" % c, end='')
# 	params["page"] = c
# 	response = requests.request("GET", list_orders_url, headers=api_connection_config, params=params)

# 	response_status_code = response.status_code

# 	print(u"   status code: %d" % response_status_code)
# 	list_json_response = json.loads(response.text)

# 	for order_json in list_json_response['list']:
# 		order_id = order_json['orderId']

# 		for i in range(0,10):
# 			order_response = requests.request("GET", list_orders_url + '/' + order_id, headers=api_connection_config)
# 			order_json_response = json.loads(order_response.text)

# 			if order_json_response.get('error'):
# 				continue

# 			break

# 		order_info = []
# 		order_info.append(order_id) # order_id
# 		order_info.append(int(order_json_response['sequence'])) # sequence
# 		created_at = datetime.strptime(order_json_response['creationDate'][:-14], '%Y-%m-%dT%H:%M:%S')
# 		created_at = created_at - timedelta(hours=2)
# 		order_info.append(created_at) # created_at
# 		order_info.append(order_json_response['clientProfileData']['firstName'] + ' ' + order_json_response['clientProfileData']['lastName']) # client_name
# 		order_info.append(ORDER_STATUS[order_json_response['status']]) # status

# 		total_product_price = format_int_to_float(order_json_response['totals'][0]['value'] + order_json_response['totals'][1]['value'])
# 		total_shipping_price = format_int_to_float(order_json_response['totals'][2]['value'])

# 		order_info.append(total_product_price) # total_product_price
# 		order_info.append(total_shipping_price) # total_shipping_price
# 		order_info.append(total_product_price + total_shipping_price) # total_order_price

# 		for item in order_json_response['items']:
# 			order_item = order_info.copy()
# 			order_item.append(item['ean']) # ean
# 			order_item.append(int(item['id'])) # vtex_sku
# 			order_item.append(int(item['productId'])) # vtex_product_id
# 			order_item.append(item['quantity']) # quantity
# 			order_item.append(item['name']) # name
# 			order_item.append(format_int_to_float(item['price'])) # tb tem sellingPrice # price

# 			order_items.append(order_item)

# 			skus_to_reduce_bypass.append({
# 				'sku_id': int(item['id']),
# 				'ordered_quantity': item['quantity'],
# 				'status': ORDER_STATUS[order_json_response['status']],
# 			})

# 	total_items = list_json_response['stats']['stats']['totalValue']['Count']


# print('Connecting to database...',end='')
# dc = DatabaseConnection()
# print('Done!')

# print('Inserting into tables...')

# print('	vtex_order_items')
# dc.execute('TRUNCATE TABLE vtex_order_items;')
# dc.insert('vtex_order_items', order_items, print_only=False)


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


	
if __name__ == '__main__':
	fixes_dict = Manager().dict()

	different_stock_query = """
		SELECT 
			e.codigo_barra as ean,
			vpi.item_id as sku_id,
			e.estoque_disponivel as stock_linx,
			vpi.stock_quantity as stock_vtex
		from w_estoque_disponivel_sku e
		LEFT JOIN bi_vtex_product_items vpi on e.codigo_barra = vpi.ean and e.filial = 'e-commerce'
		where vpi.stock_quantity is null or vpi.stock_quantity != e.estoque_disponivel
		;
	"""
	dc = DatabaseConnection()
	skus_with_different_stock = dc.select(different_stock_query, strip=True, dict_format=True)

	# skus_with_different_stock = skus_with_different_stock[0:10]
	with Pool(10) as p:
		errors = p.map(get_stock_fix_from_linx, [(x, fixes_dict) for x in skus_with_different_stock])

	skus_to_reduce = dc.select("""
		SELECT 
			voi.vtex_sku as sku_id,
			voi.quantity as ordered_quantity,
			voi.status as status
		from dbo.bi_vtex_order_items voi 
		where status not in ('Faturado', 'Cancelado')
			and created_at > '2018-11-21'
		order by order_sequence
		;
	""", strip=True, dict_format=True)

	# skus_to_reduce = skus_to_reduce[0:10]

	skus_to_reduce = make_dict(skus_to_reduce, 'ordered_quantity', ['sku_id'], repeated_key='sum')
	skus_to_reduce = [{'sku_id': sku_id, 'ordered_quantity': qnt} for sku_id, qnt in skus_to_reduce.items()]

	with Pool(10) as p:
		errors = p.map(get_stock_fix_from_orders, [(x, fixes_dict) for x in skus_to_reduce])

	with Pool(10) as p:
		errors = p.map(fix_stock, [{'sku_id': sku_id, 'true_quantity': qnt} for sku_id, qnt in fixes_dict.items()])

	print(str(datetime.now()))


	# print('	vtex_stock_backup2')
	# dc.execute('TRUNCATE TABLE vtex_stock_backup2;')
	# dc.insert('vtex_stock_backup2', stock_infos, print_only=False)