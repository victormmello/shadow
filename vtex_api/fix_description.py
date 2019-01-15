from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
from shadow_vtex.vtex import update_vtex_product

import os, fnmatch, shutil, requests, csv
import requests
from bs4 import BeautifulSoup as Soup
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
			and vpi.item_id in ('530762')
			-- and vpi.product_id = 530739
		;
	"""
	dc = DatabaseConnection()
	products_to_fix = dc.select(query, strip=True, dict_format=True)
	# print(products_to_fix)

	errors = []
	description_fields = {
		'Description': '%(Name)s',
		'DescriptionShort': '%(Name)s',
	}
	# Rodar sem thread:
	for product_to_fix in products_to_fix:
		update_vtex_product(product_to_fix['product_id'], description_fields)
		# errors.append(f(product_to_fix))

	errors = [x for x in errors if x]
	print(errors)