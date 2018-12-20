import requests, json, dateutil.relativedelta
from shadow_vtex.vtex import try_to_request
from datetime import datetime, timedelta
from shadow_database import DatabaseConnection
from multiprocessing import Pool, Manager

# ssh -i kibe.pem ec2-user@ec2-18-228-150-241.sa-east-1.compute.amazonaws.com

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

list_orders_url = 'http://marciamello.vtexcommercestable.com.br/api/oms/pvt/orders'

def get_orders_by_date_range(date_range):
	start_date = date_range[0]
	end_date = date_range[1]

	print('start_date: %s' % start_date)
	print('end_date: %s' % end_date)

	date_range_order_items = []
	formatted_end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
	formatted_start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
	params = {
		'orderBy': 'orderId,desc',
		'per_page': '15',
		'f_creationDate': 'creationDate:[%sZ TO %sZ]' % (formatted_start_date, formatted_end_date)
	}

	# print('creationDate:[¨%sZ TO %sZ]' % (formatted_start_date, formatted_end_date))
	# with open('order_output.csv', 'wb') as f:
	
	total_items = 1
	c=0
	while (c*15) < total_items:
		c+=1
		params["page"] = c
		response = try_to_request("GET", list_orders_url, headers=api_connection_config, params=params)

		response_status_code = response.status_code

		print(u"Page: %d - %s" % (c, response_status_code))
		list_json_response = json.loads(response.text)

		for order_json in list_json_response['list']:
			try:
				order_id = order_json['orderId']

				for i in range(0,20):
					try:
						order_response = requests.request("GET", list_orders_url + '/' + order_id, headers=api_connection_config)
						order_json_response = json.loads(order_response.text)

						if order_json_response.get('error'):
							continue
						break
					except Exception as e:
						pass

				order_info = []
				order_info.append(order_id) # order_id
				order_info.append(int(order_json_response['sequence'])) # sequence
				created_at = datetime.strptime(order_json_response['creationDate'][:-14], '%Y-%m-%dT%H:%M:%S')
				created_at = created_at - timedelta(hours=2)
				order_info.append(created_at) # created_at
				order_info.append(order_json_response['clientProfileData']['firstName'] + ' ' + order_json_response['clientProfileData']['lastName']) # client_name

				order_info.append(order_json_response['clientProfileData']['phone']) # client_phone_number
				order_info.append(order_json_response['clientProfileData']['email']) # client_email
				order_info.append(order_json_response['clientProfileData']['document']) # cpf
				order_info.append(order_json_response['shippingData']['logisticsInfo'][0]['deliveryCompany']) # courier
				order_info.append(order_json_response['shippingData']['address']['city']) # city
				order_info.append(order_json_response['shippingData']['address']['neighborhood']) # neighborhood
				order_info.append(order_json_response['shippingData']['address']['state']) # state
				order_info.append(order_json_response['shippingData']['address']['postalCode']) # postal_code
				order_info.append(order_json_response['shippingData']['address']['street']) # street_number
				order_info.append(order_json_response['shippingData']['address']['number']) # number
				order_info.append(order_json_response['shippingData']['address']['complement']) # complement

				order_info.append(order_json_response['paymentData']['transactions'][0]['payments'][0]['group']) # payment_method_group
				order_info.append(order_json_response['paymentData']['transactions'][0]['payments'][0]['tid']) # tid


				# order_info.append(ORDER_STATUS[order_json_response['status']]) # status
				order_info.append(order_json_response['statusDescription']) # status
				total_product_price = format_int_to_float(order_json_response['totals'][0]['value'] + order_json_response['totals'][1]['value'])
				total_shipping_price = format_int_to_float(order_json_response['totals'][2]['value'])

				order_info.append(total_product_price) # total_product_price
				order_info.append(total_shipping_price) # total_shipping_price
				order_info.append(total_product_price + total_shipping_price) # total_order_price

				this_order_items = []
				for item in order_json_response['items']:
					try:
						order_item = order_info.copy()
						order_item.append(item['ean']) # ean
						order_item.append(int(item['id'])) # vtex_sku
						order_item.append(int(item['productId'])) # vtex_product_id
						order_item.append(item['quantity']) # quantity
						order_item.append(item['name']) # name
						order_item.append(format_int_to_float(item['price'])) # tb tem sellingPrice # price

						this_order_items.append(order_item)
					except Exception as e:
						print('item error')
						continue

				invoice_info_by_index = {}
				packages = order_json_response['packageAttachment']['packages']
				for package in packages:
					# invoice_number = package['invoiceNumber']
					# invoice_key = package['invoiceKey']

					for package_item in package['items']:
						invoice_info = {
							'invoiced_quantity': package_item['quantity'],
							'invoiced_price': format_int_to_float(package_item['price']),
						}

						invoice_info_by_index[package_item['itemIndex']] = invoice_info

				for i, order_item in enumerate(this_order_items):
					invoiced_quantity = 0
					invoiced_price = 0

					if i in invoice_info_by_index:
						invoiced_quantity = invoice_info_by_index[i]['invoiced_quantity']
						invoiced_price = invoice_info_by_index[i]['invoiced_price']

					order_item.append(invoiced_quantity)
					order_item.append(invoiced_price)

				# order_item.append(invoiced_quantity)
				# order_item.append(invoiced_price)

				date_range_order_items.extend(this_order_items)



			except Exception as e:
				print('order error')
				print(str(e))
				continue

		total_items = list_json_response['stats']['stats']['totalValue']['Count']

	return date_range_order_items

if __name__ == '__main__':
	DAYS_TO_FETCH = 33
	# DAYS_TO_FETCH = 1

	tomorrow = datetime.now() + timedelta(days=1)
	days_delta = timedelta(days=1)
	end_date = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=2, minute=0, second=0)
	start_date = datetime(year=end_date.year, month=end_date.month, day=end_date.day, hour=2, minute=0, second=0) - days_delta

	date_ranges = []
	for i in range(0, DAYS_TO_FETCH):
		date_ranges.append([start_date, end_date])

		end_date -= days_delta
		start_date -= days_delta
	# date_ranges = [
	# 	[datetime(year=2018, month=11, day=1, hour=2, minute=0, second=0), datetime(year=2018, month=11, day=2, hour=2, minute=0, second=0),]
	# ]

	with Pool(len(date_ranges)) as p:
		order_items_lists = p.map(get_orders_by_date_range, date_ranges)

	# order_items_lists = []
	# for date_range in date_ranges:
	# 	order_items_list = get_orders_by_date_range(date_range)

	# 	order_items_lists.append(order_items_list)

	order_items = []
	for x in order_items_lists:
		order_items.extend(x)

	print('Connecting to database...', end='')
	dc = DatabaseConnection()
	print('Done!')

	print('Inserting into tables...')

	print('	bi_vtex_order_items')
	dc.execute('TRUNCATE TABLE bi_vtex_order_items;')
	dc.insert('bi_vtex_order_items', order_items, print_only=False)

	# skus_to_reduce = dc.select("""
	# 	SELECT 
	# 		voi.vtex_sku as sku_id,
	# 		voi.quantity as ordered_quantity,
	# 		voi.status as status
	# 	from dbo.vtex_order_items voi 
	# 	where status not in ('Faturado', 'Cancelado')
	# 	order by order_sequence
	# 	;
	# """, strip=True, dict_format=True)