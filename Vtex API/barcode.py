import sys
from shadow_database import DatabaseConnection
from shadow_helpers.helpers import set_in_dict
dc = DatabaseConnection()

fp = open('/dev/hidraw0', 'rb')

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
            if ord(c) > 0:

                if int(ord(c)) == 40:
                    done = True
                    break;

                if shift:

                    if int(ord(c)) == 2:
                        shift = True

                    else:
                        ss += hid2[int(ord(c))]
                        shift = False

                else:
                    if int(ord(c)) == 2:
                        shift = True

                    else:
                        ss += hid[int(ord(c))]
    return ss

if __name__ == '__main__':
    flag_position = False
    flag_ean = False
	try:
		while True:
			barcode = barcode_reader()
            if (len(barcode)==4):
                position = barcode
                flag_position = True
            elif barcode:
                ean = barcode
                flag_ean = True
                
            if flag_ean and flag_position:

                flag_position = False
                flag_ean = False

                query = """
                    INSERT INTO bi_estoque_localizacao (filial, codigo_barra, posicao)
                    VALUES ('E-COMMERCE', %(codigo_barra)s, %(posicao)s) 
                    """ % {
                        "codigo_barra": ean,
                        "posicao": position
                    }
    except KeyboardInterrupt:
        pass



		
