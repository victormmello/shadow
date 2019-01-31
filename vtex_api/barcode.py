import sys
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()
yay = dc.select('select top 1 * from produtos;', strip=True)
if yay:
	print('database connection OK')


fp = open('/dev/hidraw0', 'rb')

def ean_to_product_color(ean):
	sizes = ['XXGG','XGG','XPP','10','12','14','15','16','17','18','19','20','21','22','32','33','34','35','36','37','38','39','40','42','44','45','46','48','50','60','70','GG','PP','1','2','3','4','5','6','7','8','G','M','P','U']

	for size in sizes:
		if barcode[-len(size):] == size:
			produto_cor = barcode[:-len(size)]
			return produto_cor

def barcode_reader():
	hid = {4: 'a', 5: 'b', 6: 'c', 7: 'd', 8: 'e', 9: 'f', 10: 'g', 11: 'h', 12: 'i', 13: 'j', 14: 'k', 15: 'l', 16: 'm',
		   17: 'n', 18: 'o', 19: 'p', 20: 'q', 21: 'r', 22: 's', 23: 't', 24: 'u', 25: 'v', 26: 'w', 27: 'x', 28: 'y',
		   29: 'z', 30: '1', 31: '2', 32: '3', 33: '4', 34: '5', 35: '6', 36: '7', 37: '8', 38: '9', 39: '0', 44: ' ',
		   45: '-', 46: '=', 47: '[', 48: ']', 49: '\\', 51: ';', 52: '\'', 53: '~', 54: ',', 55: '.', 56: '/'}

	hid2 = {4: 'A', 5: 'B', 6: 'C', 7: 'D', 8: 'E', 9: 'F', 10: 'G', 11: 'H', 12: 'I', 13: 'J', 14: 'K', 15: 'L', 16: 'M',
			17: 'N', 18: 'O', 19: 'P', 20: 'Q', 21: 'R', 22: 'S', 23: 'T', 24: 'U', 25: 'V', 26: 'W', 27: 'X', 28: 'Y',
			29: 'Z', 30: '!', 31: '@', 32: '#', 33: '$', 34: '%', 35: '^', 36: '&', 37: '*', 38: '(', 39: ')', 44: ' ',
			45: '_', 46: '+', 47: '{', 48: '}', 49: '|', 51: ':', 52: '"', 53: '~', 54: '<', 55: '>', 56: '?'}

	

	ss = ""
	shift = False
	done = False

	while not done:

		buffer = fp.read(8)
		for c in buffer:
			int_value = c
			if not isinstance(c, int):
				int_value = ord(c)
			if int_value > 0:

				if int_value == 40:
					done = True
					break;

				if shift:

					if int_value == 2:
						shift = True

					else:
						ss += hid2[int_value]
						shift = False

				else:
					if int_value == 2:
						shift = True

					else:
						ss += hid[int_value]
	return ss

if __name__ == '__main__':
	flag_position = False
	flag_ean = False
	try:
		while True:
			barcode = barcode_reader()
			print(barcode)
			if (len(barcode)==4):
				position = barcode
				flag_position = True
			elif barcode:
				ean = barcode
				flag_ean = True
				
			if flag_ean and flag_position:
				product_color = ean_to_product_color(ean)
				flag_position = False
				flag_ean = False
				query = """
					SELECT *
					FROM bi_estoque_localizacao bel 
					where bel.produto_cor = %(produto_cor)s
					""" % {
						"produto_cor": product_color
					}
				product_search = dc.select(query, dict_format = True, strip = True)
				if product_search:
					if product_search['posicao'] = position:
						print("Position ok")
					else:
						print("Wrong position")
				else:
					print("Adicionando posicao")
					query = """
						INSERT INTO bi_estoque_localizacao (produto_cor, filial, posicao)
						VALUES ('%(produto_cor)s','E-COMMERCE', '%(posicao)s') 
						""" % {
							"produto_cor": product_color,
							"posicao": position
						}
					dc.execute(query)
	except KeyboardInterrupt:
		pass



		
