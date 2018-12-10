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
			and vpi.item_id in ('565561','576484','579452','576199','576185','576196','576204','576189','576203','576190','576205','576200','576191','576193','576197','576198','576202','576186','576201','576192','563753','563755','577973','577981','577979','577976','577977','577974','577978','581775','571642','571648','571645','571644','571646','571647','571643','571732','571729','571734','571731','571730','571727','571728','581727','581724','581725','581728','581774','581777','581772','581776','581771','571001','581722','572140','572144','572143','572145','572141','572139','581726','581723','583254','571009','571007','581384','571006','571003','571002','571005','571004','581387','581377','581370','581373','565384','565382','565386','581388','565381','565379','581379','581369','581383','581386','581371','581380','581375','565380','581382','565377','581385','581389','565376','581374','581376','581381','581378','581372')
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