from collections import OrderedDict
from datetime import date
from dateutil.relativedelta import relativedelta

####################################################################################### 
###################################### YearMonth ######################################
####################################################################################### 
def add_months(year_month,months_to_add):
	year = int(str(year_month)[:4])
	month = int(str(year_month)[-2:])
	d = date(year,month,1)
	new_d  = d + relativedelta(months=+months_to_add)
	return new_d.year*100 + new_d.month

def get_next_year_month(year_month):
	return int(year_month) + 1 if str(year_month)[-2:] != '12' else int(year_month) + 100 - 11

def get_previous_year_month(year_month):
	return int(year_month) - 1 if str(year_month)[-2:] != '01' else int(year_month) - 100 + 11

####################################################################################### 
######################################### Dict ########################################
####################################################################################### 
def normalize_string(string):
	return string.lower().strip()

def normalize_key(key):
	if isinstance(key, str):
		return normalize_string(key)

	return key

def set_in_dict(dic, value, keys=[], normalize_keys=False, repeated_key='replace', ordered=False):
	original_dict = dic
	length = len(keys)
	for i, key in enumerate(keys, 1):
		if normalize_keys:
			key = normalize_key(key)

		if i == length:
			if repeated_key == 'replace':
				dic[key] = value
			else:
				existing = dic.get(key)

				if repeated_key in ('append', 'list'):
					if existing is None:
						dic[key] = [value]
					else:
						existing.append(value)

				if repeated_key == 'set':
					if existing is None:
						dic[key] = set()
						dic[key].add(value)
					else:
						existing.add(value)

				elif repeated_key == 'sum':
					if existing is None:
						existing = 0

					dic[key] = existing + value
				elif repeated_key == 'and':
					if existing is None:
						existing = True

					dic[key] = existing and value
				elif repeated_key == 'or':
					if existing is None:
						existing = False

					dic[key] = existing or value
				elif repeated_key == 'error':
					if existing is None:
						dic[key] = value
					else:
						raise Exception(u'Chave repetida "%s"' % (to_unicode(key)))
				elif hasattr(repeated_key, '__call__'):
					dic[key] = repeated_key(existing, value)

			return original_dict

		if key not in dic:
			dic[key] = {}
			if ordered:
				dic[key] = OrderedDict()

		dic = dic[key]

def get_from_dict(dic, keys=[], normalize_keys=False, default=None):
	for key in keys:
		if normalize_keys:
			key = normalize_key(key)
		try:
			dic = dic[key]
		except KeyError:
			return default

	return dic

def get_attr(obj, attr_string):
	attrs = attr_string.split('__')
	value = obj
	for attr in attrs:
		value = getattr(value, attr)

	return value

def get_value(obj, attr_string, default=None, split_keys=True):
	if split_keys:
		attrs = attr_string.split('__')
	else:
		attrs = [attr_string]

	value = obj
	for attr in attrs:
		if value == None:
			return None

		if hasattr(value, attr):
			value = getattr(value, attr)
		elif isinstance(value, dict):
			value = value.get(attr, default)
		else:
			raise Exception('"get_value()" error: var has no attr "%s" or isnt a dict' % attr)

	return value

def make_dict(queryset, dict_value_attr, dict_keys=[], normalize_keys=False, repeated_key='replace', ordered=False, split_keys=True):
	dictionary = {}
	if ordered:
		dictionary = OrderedDict()

	for obj in queryset:
		attrs = []
		for key in dict_keys:
			if hasattr(key, '__call__'):
				attrs.append(key(obj))

			else:
				attrs.append(get_value(obj, key, split_keys=split_keys))

		value_to_add = None
		if not dict_value_attr:
			value_to_add = obj

		elif hasattr(dict_value_attr, '__call__'):
			value_to_add = dict_value_attr(obj)

		elif type(dict_value_attr) == list:
			value_to_add = {}
			for dict_val in dict_value_attr:
				value_to_add[dict_val] = get_value(obj, dict_val)	

		else:
			value_to_add = get_value(obj, dict_value_attr)
			if hasattr(value_to_add, '__call__'):
				value_to_add=value_to_add()

		set_in_dict(dictionary, value_to_add, keys=attrs, normalize_keys=normalize_keys, repeated_key=repeated_key, ordered=ordered)

	return dictionary

def for_each_leaf_aux_depth(dic, keys, depth):
	if depth == 0:
		yield keys, dic
		return
	for key in dic:
		value = dic[key]
		new_keys = keys + [key]
		for y_keys, x in for_each_leaf_aux_depth(value, new_keys, depth-1):
			yield y_keys, x

def for_each_leaf_aux(dic, keys):
	for key in dic:
		value = dic[key]
		new_keys = keys + [key]
		if isinstance(value, dict):
			for y_keys, x in for_each_leaf_aux(value,new_keys ):
				yield y_keys, x
		else:
			yield new_keys, value

def for_each_leaf(dic, depth=None, return_keys=True):
	if depth is not None:
		for keys, x in for_each_leaf_aux_depth(dic, [], depth):
			if return_keys:
				yield keys, x
			else:
				yield x
	else:
		for keys, x in for_each_leaf_aux(dic, []):
			if return_keys:
				yield keys, x
			else:
				yield x