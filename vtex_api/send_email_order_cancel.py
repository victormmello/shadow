import os, shutil, csv
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

gmail_user = 'ftoyoda.mmello.ecom@gmail.com'  
gmail_password = '@Alterar123'

server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
server.ehlo()
server.login(gmail_user, gmail_password)

def send_email(to, body,subject):
	sent_from = gmail_user  

	msg = MIMEMultipart()
	msg['From'] = gmail_user
	msg['To'] = to
	msg['Subject'] = subject
	msg.attach(MIMEText(body, 'html'))

	try:
		server.sendmail(gmail_user, to, msg.as_string())
		print('Email sent! %s' % to)
	except:
		print('Something went wrong...')

query = """
	SELECT distinct
		voi.order_sequence, 
		voi.client_name, 
		voi.total_order_price,
		tid,
		payment_method_group
	from bi_vtex_order_items voi
	where voi.order_sequence in ('562345','560994','562457','561581','562575','561777','561144','561349','560945','560974','561882','562092','559499','559700','559951','560053','560253','560260','560354','560907','561966','562000','562162')
		and payment_method_group = 'creditCard' 
	;
"""

dc = DatabaseConnection()
orders_to_cancel = dc.select(query, strip=True, dict_format=True)

for order in orders_to_cancel:
	subject = 'Estorno de pedido do e-commerce.'

	body = """
		<p>Boa tarde, Talissa.</p>
		<br>
		<p>
			Por favor, estornar o pedido %(order_sequence)s.<br>
			<br>
			Nome: %(client_name)s<br>
			Valor pedido: %(total_order_price)s<br>
			Valor estorno: %(total_order_price)s<br>
			TID: %(tid)s<br>
		</p>
		<br>

		<p>
			Att, <br>
			Felipe Toyoda
		</p>
	""" % order

	send_email('talissa.medina@marciamello.com.br', body, subject)
	# time.sleep(50)

server.close()
