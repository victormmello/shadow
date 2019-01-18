import os, shutil, csv
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

gmail_user = 'gsato.mmello.ecom@gmail.com'  
gmail_password = '#googlemm'

email_dict = {}

emails_with_wrong_price = (
	'viviane.quiessi@terra.com.br',
	'cristina.miatto@br.luxottica.com',
	'denisereato@yahoo.com.br',
	'julianagluciano@gmail.com',
	'moniqsz@hotmail.com',
	'robertanbarros@hotmail.com',
	'apcm2000@yahoo.com.br',
	'alessandra.b.verde@gmail.com',
	'janaina_giraldi@yahoo.com.br',
	'cannonieri@hotmail.com',
	'adrianamariadepaula@hotmail.com',
	'amandayb2525@gmail.com',
	'elvia.carvalho@yahoo.com.br',
	'meire.leite@bol.com.br',
	'rosy_xz@yahoo.com.br',
	'mterhorst1@gmail.com',
	'gabisouza_romano@yahoo.com.br',
	'carolpasetti@gmail.com',
	'dllshs@terra.com.br',
	'deniseveronez@hotmail.com',
	'mica.bianchini@gmail.com',
	'ajsnc@uol.com.br',
	'elaine.sbravatti@hotmail.com',
	'vivianuromero@yahoo.com.br',
	'neysevieira@yahoo.com.br',
	'gerencia@empreiteiraschinalle.com.br',
	'engfabiana@ig.com.br',
	'marlimichelon@hotmail.com',
	'carolinaraquel@yahoo.com.br',
	'kellyn_foscaldi@uol.com.br',
	'mary_m_lene@hotmail.com',
	'isadoracgomes@hotmail.com',
	'kelpim@yahoo.com.br',
	'elisabeteoliveiragarciabol@hotmail.com',
	'mrc.carrion@gmail.com',
	'anac-lima@uol.com.br',
	'juli.moraes2010@gmail.com',
	'cristthiane.de.souza@gmail.com',
	'iviepizolli@gmail.com',
	'ingrid.mian@gmail.com',
	'rodrigodeperon@uol.com.br',
	'fabinha_bruno@yahoo.com.br',
	'renatar_andrade@yahoo.com',
	'christiana_souza@yahoo.com.br',
	'rosana-costa@hotmail.com',
	'brenda.100@hotmail.com',
	'dricanicoba@gmail.com',
	'claudiaspicoli@hotmail.com',
	'v_viane@yahoo.com',
	'helena.ky@hotmail.com',
	'marines.s3208@gmail.com.br',
	'solange2003usa@hotmail.com',
	'reginacaxefo@yahoo.com.br',
	'alice_chagas@yahoo.com.br',
	'roura.04@gmail.com',
	'silviahelenasperetta@gmail.com',
	'jessica_v_c@hotmail.com',
	'jussaravieirabranco@yahoo.com.br',
	'aldicler@gmail.com',
	'marisetto@uol.com.br',
	'kellycris.silva1112@gmail.com',
	'carinadibbern@hotmail.com',
	'heloysa_sr@hotmail.com',
	'zizispinelli@yahoo.com.br',
	'andreaspereiira@gmail.com',
	'alinetakamiya@gmail.com',
	'tamires.m.serafim@bol.com.br',
	'luci.scarlet@hotmail.com',
	'kcbaquete@yahoo.com.br',
	'robertacalcada8159@gmail.com',
	'marianasfernandes@hotmail.com',
	'rosangelaschiavinato@outlook.com',
	'biaarantez@hotmail.com',
)

emails_sent = [
	'denisereato@yahoo.com.br',
	'juliane.azarias@gmail.com',
	'viviane.quiessi@terra.com.br',

	'cristina.miatto@br.luxottica.com',
	'deniseveronez@hotmail.com',
	'sirlene.furian@hotmail.com',
	'mefrare1@hotmail.com',
	'anac-lima@uol.com.br',
	'giovanacriteixeira@gmail.com',
	'mrc.carrion@gmail.com',
	'elisabeteoliveiragarciabol@hotmail.com',
	'kelpim@yahoo.com.br',
	'michelysayumi@hotmail.com',
	'isadoracgomes@hotmail.com',
	'mary_m_lene@hotmail.com',
	'kellyn_foscaldi@uol.com.br',
	'carolinaraquel@yahoo.com.br',
	'marlimichelon@hotmail.com',
	'engfabiana@ig.com.br',
	'gerencia@empreiteiraschinalle.com.br',
	'neysevieira@yahoo.com.br',
	'vivianuromero@yahoo.com.br',
	'andreia.frangogel@yahoo.com.br',
	'elaine.sbravatti@hotmail.com',
	'ajsnc@uol.com.br',
	'contato@consultoriasegura.com.br',
	'robertacalcada8159@gmail.com',
	'mica.bianchini@gmail.com',
	'josy.ricoldi@gmail.com',
	'pri.cardoso.br@gmail.com',
	'dllshs@terra.com.br',
	'carolpasetti@gmail.com',
	'gabisouza_romano@yahoo.com.br',
	'silviaest.fer@gmail.com',
	'mterhorst1@gmail.com',
	'afurlan2004@yahoo.com.br',
	'rosy_xz@yahoo.com.br',
	'valdineiapita@yahoo.com.br',
	'meire.leite@bol.com.br',
	'elvia.carvalho@yahoo.com.br',
	'amandayb2525@gmail.com',
	'adrianamariadepaula@hotmail.com',
	'cannonieri@hotmail.com',
	'janaina_giraldi@yahoo.com.br',
	'alessandra.b.verde@gmail.com',
	'apcm2000@yahoo.com.br',
	'ksibinelli@uol.com.br',
	'ma-maurin@bol.com.br',
	'sirleigomes.as@gmail.com',
	'elianechief@gmail.com',
	'robertanbarros@hotmail.com',
	'moniqsz@hotmail.com',
	'julianagluciano@gmail.com',
]





def send_email(to, body,subject):
	to = 'felipe.toyoda.mmello@gmail.com'

	sent_from = gmail_user  

	msg = MIMEMultipart()
	msg['From'] = gmail_user
	msg['To'] = to
	msg['Subject'] = subject
	msg.attach(MIMEText(body,'html'))

	try:
		server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
		server.ehlo()
		server.login(gmail_user, gmail_password)
		server.sendmail(gmail_user, to, msg.as_string())
		server.close()
		print('Email sent! %s' % to)
	except:
		print('Something went wrong...')


with open('email_base.csv', encoding='latin-1') as csvfile:
	reader = list(csv.DictReader(csvfile, delimiter=';'))
	for row in reader[:4]:
		email = row['email'].strip()
		desc = row['desc'].strip() + ' ' + row['cor'].strip()
		set_in_dict(email_dict, float(row['price_2'].replace(',', '.')), [email, desc])

for email in email_dict:
	produtos = ""
	value = 0
	for desc in  email_dict[email]:

		produto = "<p><b>- %s</b></p>" % (desc)
		produtos += produto
		value += email_dict[email][desc]

	parsed_value = str(round(value, 2)).replace(".",",")
	parts = parsed_value.split(',')
	if len(parts) == 1:
		parsed_value = parts[0] + ',00'
	if len(parts) == 2:
		if len(parts[1]) == 1:
			parsed_value = parts[0] + ',' + parts[1] + '0'
		elif len(parts[1]) == 2:
			parsed_value = parts[0] + ',' + parts[1]

	# nao mandar novamente
	if email in emails_sent and email not in emails_with_wrong_price:
		print('email jah tinha sido enviado: %s' % email)
		continue

	ignore_previous_email = ''
	subject = 
	if email in emails_sent and email in emails_with_wrong_price:
		ignore_previous_email = '<p style="color: red;">Por favor, desconsidere o email enviado previamente. As informações corretas estão a seguir.</p>'



	body = """
<div id=":p4" class="a3s aXjCH "><div style="font-size:10pt;font-family:Verdana,Geneva,sans-serif"><div class="adM">
</div><p><img src="http://blog.marciamello.com.br/Ecommerce/mail-mkt/11122018/1.png" width="1147" height="191" data-image-whitelisted="" class="CToWUd a6T" tabindex="0"><div class="a6S" dir="ltr" style="opacity: 0.01; left: 1059px; top: 167.333px;"><div id=":pn" class="T-I J-J5-Ji aQv T-I-ax7 L3 a5q" role="button" tabindex="0" aria-label="Fazer o download do anexo 28ca821b.png" data-tooltip-class="a1V" data-tooltip="Fazer o download"><div class="aSK J-J5-Ji aYr"></div></div><div id=":po" class="T-I J-J5-Ji aQv T-I-ax7 L3 a5q" role="button" tabindex="0" aria-label="Salvar o anexo 28ca821b.png no Google Drive" data-tooltip-class="a1V" data-tooltip="Salvar no Google Drive"><div class="wtScjd J-J5-Ji aYr aQu"><div class="T-aT4" style="display: none;"><div></div><div class="T-aT4-JX"></div></div></div></div></div></p>
%(ignore_previous_email)s
<p>Querida(o) cliente,</p>
<p>infelizmente não localizamos em nosso estoque <span class="m_4059923059014164740x_markolpp8yojr" style="color:black">o(s)</span> <span class="m_4059923059014164740x_mark4evryy5le" style="color:black">item (itens)</span> abaix<span class="m_4059923059014164740x_markolpp8yojr" style="color:black">o</span>:</p>
%(produtos)s
<p>Caso haja algum item restante em seu pedido, ele deve estar a caminho. Se quiser confirmar fique à vontade para entrar em contato conosco pelo telefone <strong>(19) 3252-3374.</strong></p>
<p>&nbsp;</p>
<p>Devido a essa falha nossa, seu crédito total soma o montante de <b>R$ %(parsed_value)s</b>. Fique à vontade para escolher uma das 2 opções abaixo:</p>
<p><strong>1. Crédito de R$ %(parsed_value)s em nossas lojas físicas + voucher de 50%% de desconto em qualquer produto da loja para qualquer valor de compra. Apresente esse e-mail e informe seu CPF para a vendedora.</strong//></span></p>
<p><strong>2. Devolução do crédito de R$ %(parsed_value)s, em conta bancária. Se você tiver extrema urgência, ligue para o nosso telefone (19) 3252-3374. Favor nos informar seus dados bancários para depósito.</p>
<p>&nbsp;</p>
<p>Mais uma vez, pedimos desculpas pelo inconveniente. Estamos trabalhando 24h por dia para que isso não aconteça mais.</p>
<p>&nbsp;</p>
<p>Nos colocamos à sua disposição para tirar qualquer dúvida.</p>
<p>&nbsp;</p>
<p>Atenciosamente,</p>
<p><strong>Grupo Marcia Mello</strong></p>
<p>&nbsp;</p>
<div>
<p>&nbsp;</p>
</div>
<p>&nbsp;</p><div class="yj6qo"></div><div class="adL">
</div></div><div class="adL">
</div></div>
		""" % {
			'ignore_previous_email': ignore_previous_email,
			'produtos': produtos,
			'parsed_value': parsed_value,
		}

	send_email(email, body)
	time.sleep(50)