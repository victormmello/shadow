from shadow_database import DatabaseConnection
from shadow_vtex.vtex import try_to_request
from shadow_helpers.helpers import make_dict

import json, requests


# Set the request parameters
url = 'https://marciamello.zendesk.com/api/v2/tickets.json'
user = 'leif.ecomm@marciamello.com.br'
pwd = 'marciamello2017'
headers = {'content-type': 'application/json'}

body_template = """
Olá %(client_name)s, tudo bem?

Infelizmente por conta da alta demanda tivemos problemas em nosso estoque e os itens do seu pedido #%(order_sequence)s

%(missing_products_text)s

Estão indisponíveis em nosso estoque. E dessa forma, não poderemos realizar o envio. 

Pedimos desculpas pelo ocorrido e resolver isso da melhor maneira possível, temos duas opções para lhe oferecer:

1-) Podemos efetuar o estorno dos valores referente os itens acima; %(ask_for_bank_info_str)s

2-) Você pode ir até qualquer loja Marcia Mello e escolher qualquer outra peça do mesmo valor e mais 50%% de desconto na próxima compra.

Estamos aqui para resolver da melhor maneira possível, fico a disposição e aguardamos seu retorno para conclusão desta pendência.

Aguardamos seu retorno.

Equipe MM
"""

query = """
	SELECT distinct
		voi.order_sequence, 
		voi.client_email, 
		voi.client_name, 
		voi.payment_method_group,

		voi.ean,
		voi.name,
		voi.vtex_sku
	from bi_vtex_order_items voi
	where 
		voi.order_sequence in ('561973','561857','561766','561549','561528','561304','561194','560970','560865','560840','560018','562757','562703','562633','562551','562301')
		-- voi.order_sequence in ('561268')
	;
"""

dc = DatabaseConnection()
orders_to_cancel = dc.select(query, strip=True, dict_format=True)

orders_to_cancel = make_dict(orders_to_cancel, None, ['order_sequence'], repeated_key='append')

for missing_products in orders_to_cancel.values():
	order = missing_products[0]
	subject = 'Informações do Pedido #%s' % order['order_sequence']

	missing_products_text = []
	for product in missing_products:
		missing_products_text.append('%s\n%s (ref %s)' % (product['name'], product['vtex_sku'], product['ean'])
		)

	missing_products_text = '\n\n'.join(missing_products_text)

	# missing_products_text = """
	# BLUSA MANGA CURTA-MESCLA - PP
	# 583656 (ref 2205056464PP)

	# BLUSA DECOTE V PREGAS PAOLA-ESTAMPA AZUL - PP
	# 582840 (ref 22120516217PP)
	# """

	ask_for_bank_info_str = ''
	if order['payment_method_group'] == 'bankInvoice':
		ask_for_bank_info_str = 'Caso escolha o estorno, por favor nos envie suas informações bancárias.'

	body_params = {
		'ask_for_bank_info_str': ask_for_bank_info_str,
		'missing_products_text': missing_products_text,
	}
	body_params.update(order)
	body_params['client_name'] = order['client_name'].title()
	body = body_template % body_params

	user_response = try_to_request('GET', 'https://marciamello.zendesk.com/api/v2/users/search.json?query=email:%s' % order['client_email'], auth=(user, pwd))
	user_json = json.loads(user_response.text)

	if user_json['count']:
		requester_id = user_json['users'][0]['id']
	else:
		print('no user: %s' % order['client_email'])

		user_data = {
			"user": {
				"name": order['client_name'].title(), 
				"email": order['client_email'], 
				"verified": True
			}
		}
		user_payload = json.dumps(user_data)

		user_response = requests.request('POST', 'https://marciamello.zendesk.com/api/v2/users.json', data=user_payload, auth=(user, pwd), headers=headers)
		user_json = json.loads(user_response.text)
		requester_id = user_json['user']['id']
		
	# Package the data in a dictionary matching the expected JSON
	ticket_data = {
		'ticket': {
			'subject': subject,
			'comment': {
				'body': body
			},
			'priority': 'normal',
			# 'requester_id': requester_id,
			'requester_id': requester_id,
			"submitter_id": 115531826414,
			"assignee_id": 115531826414,
			"recipient": "sac@marciamello.com.br",
			"group_id": 114095382893,
			'status': 'pending',
			"type": "incident",
		}
	}

	# Encode the data to create a JSON payload
	ticket_payload = json.dumps(ticket_data)

	# Do the HTTP post request
	response = requests.request('POST', url, data=ticket_payload, auth=(user, pwd), headers=headers)

	# Check for HTTP codes other than 201 (Created)
	if response.status_code != 201:
		print(response.text)
		print('Status:', response.status_code, 'Problem with the request. Exiting.')
		continue

	# Report success
	print('Successfully created the ticket. %s' % order['order_sequence'])

	import time
	time.sleep(60)