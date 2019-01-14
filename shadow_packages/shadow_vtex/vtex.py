from shadow_vtex import vtex_config
from bs4 import BeautifulSoup as Soup
import time, requests
from datetime import datetime

def try_to_request(*args, **kwargs):
	retry = 3
	for i in range(0, retry):
		response = None
		try:
			response = requests.request(*args, **kwargs)

			# print(response.status_code)
			if response.status_code in (200, 201):
				break
			elif response.status_code == 404:
				return None
			elif response.status_code == 429:
				time.sleep(10)
			else:
				raise Exception()

		except Exception as e:
			if i == retry-1:
				print(response.text)
				# import pdb; pdb.set_trace()
				if response:
					print('desistindo')

				return None
	
	return response

def authenticated_request(*args, **kwargs):
	base_headers = vtex_config.api_connection_header
	if kwargs.get('headers'):
		headers = base_headers.copy()
		headers.update(kwargs['headers'])
	else:
		headers = base_headers

	return try_to_request(headers=headers, *args, **kwargs)


def post_to_webservice(soap_action, soap_message, retry=3):
	vtex_config.params["SOAPAction"] = soap_action
	request_type = soap_action.split('/')[-1]

	print(request_type)
	response = try_to_request('POST', vtex_config.webserviceURL, auth=vtex_config.auth, headers=vtex_config.params, data=soap_message.encode(), timeout=10)

	if response:
		return Soup(response.text, "xml")

	print(soap_message)

# def post_to_webservice(soap_action, soap_message, retry=3):
# 	params["SOAPAction"] = soap_action
# 	request_type = soap_action.split('/')[-1]

# 	for i in range(0, retry):
# 		try:
# 			response = requests.post("http://webservice-marciamello.vtexcommerce.com.br/Service.svc?singleWsdl", auth=auth, headers=params, data=soap_message.encode(), timeout=10)

# 			print("%s %s" % (request_type, response.status_code))
# 			if response.status_code == 200:
# 				break
# 			elif response.status_code == 429:
# 				import time
# 				time.sleep(10)
# 				raise Exception()
# 			else:
# 				raise Exception()

# 		except Exception as e:
# 			if i == retry-1:
# 				print('desistindo')
# 				return None

# 	return Soup(response.text, "xml")



def update_vtex_product(product_id, update_dict, dafiti_store=False):
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
		return 'error: soap_productget %s' % product_id


	
	product_dict = {
		'AdWordsRemarketingCode': soup.find('a:AdWordsRemarketingCode').text,
		'BrandId': soup.find('a:BrandId').text,
		'CategoryId': soup.find('a:CategoryId').text,
		'DepartmentId': soup.find('a:DepartmentId').text,
		'Description': soup.find('a:Description').text,
		'DescriptionShort': soup.find('a:DescriptionShort').text,
		'Id': soup.find('a:Id').text,
		'IsActive': soup.find('a:IsActive').text,
		'IsVisible': soup.find('a:IsVisible').text,
		'KeyWords': soup.find('a:KeyWords').text,
		'LinkId': soup.find('a:LinkId').text,
		# 'ListStoreId': soup.find('a:ListStoreId').text,
		'LomadeeCampaignCode': soup.find('a:LomadeeCampaignCode').text,
		'MetaTagDescription': soup.find('a:MetaTagDescription').text,
		'Name': soup.find('a:Name').text,
		'RefId': soup.find('a:RefId').text,
		'ReleaseDate': soup.find('a:ReleaseDate').text,
		'Score': soup.find('a:Score').text or 0,
		'ShowWithoutStock': soup.find('a:ShowWithoutStock').text,
		'SupplierId': soup.find('a:SupplierId').text,
		'TaxCode': soup.find('a:TaxCode').text,
		'Title': soup.find('a:Title').text,
	}

	for k, v in update_dict.items():
		if k not in product_dict:
			raise Exception('setando atributo inexistente %s' % k)

		if isinstance(v, str):
			product_dict[k] = (v % product_dict)

	dafiti_store_str = ''
	if dafiti_store:
		dafiti_store_str = '<arr:int>3</arr:int>'
	product_dict['dafiti_store'] = dafiti_store_str


	release_date_str = datetime.today().strftime('%Y-%m-%dT00:00:00')
	if product_dict['ReleaseDate']:
		release_date_str = '<vtex:ReleaseDate>%(ReleaseDate)s</vtex:ReleaseDate>' % product_dict
	product_dict['release_date_str'] = release_date_str


	# --------------------------------------------------------------------------------------------------------------------------------

	soap_productupdate = """
		<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/" xmlns:vtex="http://schemas.datacontract.org/2004/07/Vtex.Commerce.WebApps.AdminWcfService.Contracts" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
			<soapenv:Header/>
			<soapenv:Body>
				<tem:ProductInsertUpdate>
					<tem:productVO>
						<vtex:AdWordsRemarketingCode>%(AdWordsRemarketingCode)s</vtex:AdWordsRemarketingCode>
						<vtex:BrandId>%(BrandId)s</vtex:BrandId>
						<vtex:CategoryId>%(CategoryId)s</vtex:CategoryId>
						<vtex:DepartmentId>%(DepartmentId)s</vtex:DepartmentId>
						<vtex:Description>%(Description)s</vtex:Description>
						<vtex:DescriptionShort>%(DescriptionShort)s</vtex:DescriptionShort>
						<vtex:Id>%(Id)s</vtex:Id>
						<vtex:IsActive>%(IsActive)s</vtex:IsActive>
						<vtex:IsVisible>%(IsVisible)s</vtex:IsVisible>
						<vtex:KeyWords>%(KeyWords)s</vtex:KeyWords>
						<vtex:LinkId>%(LinkId)s</vtex:LinkId>
						<vtex:ListStoreId>
							<arr:int>1</arr:int>
							%(dafiti_store)s
						</vtex:ListStoreId>
						<!--<vtex:LomadeeCampaignCode>%(LomadeeCampaignCode)s</vtex:LomadeeCampaignCode>-->
						<!--<vtex:MetaTagDescription>%(MetaTagDescription)s</vtex:MetaTagDescription>-->
						<vtex:Name>%(Name)s</vtex:Name>
						<vtex:RefId>%(RefId)s</vtex:RefId>
						%(release_date_str)s
						<vtex:Score>%(Score)s</vtex:Score>
						<vtex:ShowWithoutStock>%(ShowWithoutStock)s</vtex:ShowWithoutStock>
						<!--<vtex:SupplierId>%(SupplierId)s</vtex:SupplierId>-->
						<vtex:TaxCode>%(TaxCode)s</vtex:TaxCode>
						<vtex:Title>%(Title)s</vtex:Title>
					</tem:productVO>
				</tem:ProductInsertUpdate>
			</soapenv:Body>
		</soapenv:Envelope>""" % product_dict


	soup = post_to_webservice("http://tempuri.org/IService/ProductInsertUpdate", soap_productupdate)
	if not soup:
		print(soap_productupdate)
		return 'error: soap_productupdate %s' % product_id