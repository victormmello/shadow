import csv

photo_file_names = []
with open('lista_renomear_foto.csv', encoding='latin-1') as csvfile:
	reader = csv.DictReader(csvfile, delimiter=';')
	for row in reader:
		prod_code = row['code'].strip().replace('.', '')
		color_code = row['color'].split('-')[0].strip().zfill(2)
		photo_qnt = int(row['photo_qnt'].strip())

		params = {
			'ref_sem_ponto': prod_code,
			'cod_cor': color_code,
		}
		for pose_num in range(1, photo_qnt+1):
			params['pose_num'] = pose_num

			file_name = '%(ref_sem_ponto)s.%(cod_cor)s_0%(pose_num)s.jpg' % params

			# print(file_name)
			photo_file_names.append(file_name)

import os
path = os.getcwd()
path += '/fotos_para_renomear/renomear'
images = list(os.listdir(path))

images = sorted(images, key=(lambda x: x.split('-')[1].zfill(8)))

if len(images) != len(photo_file_names):
	raise Exception("%s - %s" % (len(images), len(photo_file_names)))

for i, old_filename in enumerate(images):
	old_file_path = path + '/' + old_filename

	new_filename = photo_file_names[i]
	new_file_path = path + '/' + new_filename

	# print(old_file_path)
	# print(new_file_path)
	os.rename(old_file_path, new_file_path)

	pass