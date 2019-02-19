from shadow_vtex.vtex import update_vtex_product, post_to_webservice, authenticated_request
from shadow_database import DatabaseConnection
import json

sku_id = 2000110

# product_id = 530935
# sku_id = 572827

url = "https://marciamello.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/%s" % sku_id
response = authenticated_request("GET", url)
sku_info = json.loads(response.text)
sku_name = sku_info['NameComplete']
ean = sku_info['AlternateIds']['Ean']
product_id = sku_info['ProductId']

soap_productget = """
	<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
		<soapenv:Header/>
		<soapenv:Body>
			<tem:ProductGet>
				<tem:idProduct>%(product_id)s</tem:idProduct>
			</tem:ProductGet>
		</soapenv:Body>
	</soapenv:Envelope>
""" % {'product_id': product_id}

soup = post_to_webservice("http://tempuri.org/IService/ProductGet", soap_productget)
if not soup:
	raise Exception('error: soap_productget %s' % product_id)

product_code = soup.find('a:RefId').text

dc = DatabaseConnection()
query = """
	SELECT
		IdItem
	from IT4Produto 
	where RefProduto='%s'
""" % product_code
it4_item_id = dc.select(query, strip=True)

if it4_item_id:
	it4_item_id = it4_item_id[0][0]

	if int(product_id) != int(it4_item_id):
		query = """
			UPDATE dbo.IT4Produto
			SET IdProduto='%s'
			WHERE IdItem='%s'
		;
		""" % (product_id, it4_item_id)

		print(query)
		dc.execute(query)

	
else:
	product_params = {
		'product_id': product_id,
		'product_code': product_code,
		'description': soup.find('a:Name').text,
		'key_words': soup.find('a:KeyWords').text,
	}

	query = """
		INSERT INTO dbo.IT4Produto
		(IdProduto, Descricao, DescricaoLink, DescricaoDetalhada, DescricaoMetaTag, PalavrasSubstitutivas, Categorias, IdMarca, IdDepartamento, RefProduto, DataCadastro, DataLancamento)
		VALUES('%(product_id)s', '%(description)s', '%(key_words)s', '%(description)s', '%(description)s', '%(key_words)s', '2', 1, 1, '%(product_code)s', null, null)
	;
	""" % product_params

	print(query)
	dc.execute(query)


	query = """
		SELECT
			IdItem
		from IT4Produto 
		where RefProduto='%(product_code)s'
	""" % product_params

	it4_item_id = dc.select(query, strip=True)
	it4_item_id = it4_item_id[0][0]

# max_it4_sku_id = dc.select("SELECT MAX(IdSku) as max_id FROM IT4Sku;", strip=True)[0][0]


query = """
	SELECT
		IdSku
	from IT4Sku 
	where RefSku='%s'
	;
""" % ean
it4_sku_id = dc.select(query, strip=True)
if it4_sku_id:
	it4_sku_id = it4_sku_id[0][0]

	if int(sku_id) != int(it4_sku_id):
		try:
			dc.execute('ALTER TABLE dbo.IT4Preco NOCHECK CONSTRAINT all;')

			query = """
				UPDATE dbo.IT4Preco
				SET IdSku='%s'
				WHERE IdSku='%s'
			;
			""" % (sku_id, it4_sku_id)

			print(query)
			dc.execute(query)	
			
			query = """
				UPDATE dbo.IT4Sku
				SET IdSku='%s'
				WHERE IdSku='%s'
			;
			""" % (sku_id, it4_sku_id)

			print(query)
			dc.execute(query)
		except Exception as e:
			raise e
		finally:
			dc.execute('ALTER TABLE dbo.IT4Preco WITH CHECK CHECK CONSTRAINT all')

else:
	sku_params = {
		'it4_sku_id': sku_id,
		'it4_item_id': it4_item_id,
		'ean': ean,
		'sku_name': sku_name,
	}

	query = """
		INSERT INTO Marcia_Mello.dbo.IT4Sku
		(IdSku, IdItem, RefSku, Descricao, Peso, Altura, Largura, Comprimento, PesoReal, AlturaReal, LarguraReal, ComprimentoReal, PesoCubico, Estoque, CodigoAlternativo, EstoqueMinimo)
		VALUES ('%(it4_sku_id)s', '%(it4_item_id)s', '%(ean)s', '%(sku_name)s', 1000, 1, 1, 1, 0, 0, 0, 0, 0, 0, null, null);
	""" % sku_params

	print(query)
	dc.execute(query)


	for spec in sku_info['SkuSpecifications']:
		if spec['FieldName'] == 'Cor':
			query = """
				INSERT INTO Marcia_Mello.dbo.IT4SkuAtributo
					(IdSku, IdAtributo, Valor)
				VALUES('%s', 1, '%s');
			""" % (sku_id, spec['FieldValues'][0])
			print(query)
			dc.execute(query)
		elif spec['FieldName'] == 'Tamanho':
			query = """
				INSERT INTO Marcia_Mello.dbo.IT4SkuAtributo
					(IdSku, IdAtributo, Valor)
				VALUES('%s', 2, '%s');
			""" % (sku_id, spec['FieldValues'][0])
			print(query)
			dc.execute(query)
