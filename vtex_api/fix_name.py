from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
from shadow_vtex.vtex import update_vtex_product

import requests, re
from multiprocessing import Pool

# --------------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
	query = """
		SELECT distinct
			vpi.product_id
		from dbo.bi_vtex_products vp
		inner join bi_vtex_product_items vpi on vpi.product_id = vp.product_id
		where 1=1
			-- and vp.produto = '35.02.0766'
			-- and vpi.item_id in ('570164')
			-- and vpi.product_id = 530739
		;
	"""
	dc = DatabaseConnection()
	products_to_fix = dc.select(query, strip=True, dict_format=True)
	# print(products_to_fix)

	errors = []
	def fix_name(product_info):
		name = product_info['Name'].title()

		name = re.sub(r'\bCalca\b', 'Calça', name)
		name = re.sub(r'\bMacacao\b', 'Macacão', name)
		name = re.sub(r'\bLaco\b', 'Laço', name)
		name = re.sub(r'\bLacos\b', 'Laços', name)
		name = re.sub(r'\bBasica\b', 'Básica', name)
		name = re.sub(r'\bBasico\b', 'Básico', name)
		name = re.sub(r'\bLenco\b', 'Lenço', name)
		name = re.sub(r'\bIlhos\b', 'Ilhós', name)
		name = re.sub(r'\bBotoes\b', 'Botões', name)
		name = re.sub(r'\bBotao\b', 'Botão', name)
		name = re.sub(r'\bAmarracao\b', 'Amarração', name)
		name = re.sub(r'\bVitoria\b', 'Vitória', name)
		name = re.sub(r'\bAndreia\b', 'Andréia', name)
		name = re.sub(r'\bAlca\b', 'Alça', name)
		name = re.sub(r'\bAlcas\b', 'Alças', name)
		name = re.sub(r'\bMedia\b', 'Média', name)
		name = re.sub(r'\bMedias\b', 'Médias', name)
		name = re.sub(r'\bAplicacao\b', 'Aplicação', name)
		name = re.sub(r'\bAplicacoes\b', 'Aplicações', name)
		name = re.sub(r'\bCom No\b', 'Com Nó', name)
		name = re.sub(r'\bOmbro So\b', 'Ombro Só', name)
		name = re.sub(r'\bPescoco\b', 'Pescoço', name)
		name = re.sub(r'\bUnica\b', 'Única', name)

		print('%s -> %s' % (product_info['Name'], name))
		return name


	description_fields = {
		'Name': fix_name,
	}
	# Rodar sem thread:

	errors = []
	for product_to_fix in products_to_fix:
		error = update_vtex_product(product_to_fix['product_id'], description_fields)
		errors.append(error)
		# errors.append(f(product_to_fix))

	errors = [x for x in errors if x]
	print(errors)