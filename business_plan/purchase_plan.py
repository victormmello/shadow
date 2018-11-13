from shadow_database import DatabaseConnection
from shadow_helpers import make_dict, add_months
from collections import OrderedDict
import calendar
from math import pow

# row[0]:
year_month_index = 0
year_month_dimension = 'anomes'
# row[1] to row[len(dimensions)]:
dimensions = [
	'faixa_de_custo',
	'categoria',
	'liso_estampado'
]
# row[len(dimensions)+1] to row[len(dimensions)+len(metrics)+1]
metrics = [
	{'name':'quantidade_estoque','type':'sum','format':'int'},
	{'name':'custo_estoque','type':'sum','format':'numeric(38,2)'},
	{'name':'itens','type':'sum','format':'int'},
	{'name':'receita_total','type':'sum','format':'numeric(38,2)'},
	{'name':'custo_total','type':'sum','format':'numeric(38,2)'},
	{'name':'quantidade_recebida','type':'sum','format':'int'},
	{'name':'custo_recebido','type':'sum','format':'numeric(38,2)'},
	{'name':'quantidade_a_receber','type':'sum','format':'int'},
	{'name':'custo_a_receber','type':'sum','format':'numeric(38,2)'},
	{'name':'avg_ticket','type':'calculated','format':'numeric(38,2)'},
	{'name':'avg_cogs','type':'calculated','format':'numeric(38,2)'}
]
last_sale_index = 1 + len(dimensions)
for metric in metrics:
	if metric['type'] == 'sum':
		last_sale_index += 1
last_sale = None

MULTIPLIERS = {
	'avg_cogs':{
		'inferior':0.9,
		'superior':1.1,
		'multiplier':0.97
	},
	'avg_ticket':{
		'inferior':0.9,
		'superior':1.1,
		'multiplier':1
	},
	'itens':{
		'inferior':0.7,
		'superior':1.8,
		'min_relevant':20,
		'multiplier':1.03,
		'factor':2.5
	},
	'pc1p':{
		'inferior':0.9,
		'superior':1.1,
	},
	'pc1p_check':{
		'inferior':-0.1,
		'superior':0.1,
	},
}
# mudar!!!!
FATOR_DE_COBERTURA_MESES = 3

output_schema = "dbo"
output_table = "bi_purchase_plan"
update_results = True

projection_months_number = 12

query = open("category_sales_agrupado.sql").read()

metrics_list_for_query = []
for metric in metrics:
	if metric['type'] == 'sum':
		metrics_list_for_query.append(metric['name'])

def create_year_month_dict():
	result = {}
	for metric_dict in metrics:
		metric = metric_dict['name']
		result[metric] = 0
	return result

def get_parent_dimension(dimension):
	return '__'.join(dimension.split('__')[:-1])

def get_metric_list(dict,year_month,metric,year_month_from=1,year_month_to=6,exclude_zeros=True):
	result = []
	for i in range(year_month_from,year_month_to+1):
		val = dict[add_months(year_month,-i)][metric]
		if val > 0 or not exclude_zeros:
			result.append(val)
	return result

def get_trustable_parent(dict, initial_loop_level,dimension,metric,year_month_from,year_month_to,min_len=0,min_value=0):
	metric_list = []
	loop_level = initial_loop_level
	dimension_level = dimension
	for i in range(0,initial_loop_level):
		dimension_level = get_parent_dimension(dimension_level)
	while loop_level <= len(dimensions) and (metric_list == [] or max(metric_list) <= min_value or len(metric_list) < min_len):
		metric_list = get_metric_list(result_dict[loop_level][dimension_level],year_month,metric,year_month_from=year_month_from,year_month_to=year_month_to)
		if loop_level <= len(dimensions) and (metric_list == [] or max(metric_list) <= min_value or len(metric_list) < min_len):
			loop_level += 1
			dimension_level = get_parent_dimension(dimension_level)
	return loop_level, dimension_level

print('Connecting to database...',end='')
dc = DatabaseConnection()
print('Done!')

print('Getting database information...',end='')
result = dc.select(query % {
	'dimensions':',\n'.join(['cs.%s' % dimension for dimension in dimensions]),
	'metrics':',\n'.join(['SUM(cs.%s) as %s' % (metric,metric) for metric in metrics_list_for_query]),
})
print('Done!')

if result:
	print('Creating result dict structure...',end='')
	# Creates Result dict structure:
	result_dict = {}
	for level in range(0,len(dimensions)+1):
		result_dict[level] = {}

	historical_year_months = []
	for row in result:
		# set last sale:
		last_sale = max(last_sale,row[last_sale_index]) if last_sale else row[last_sale_index]
		historical_year_month = row[year_month_index]
		if historical_year_month not in historical_year_months:
			historical_year_months.append(historical_year_month)

		# creates the result_dict with the structure:
		# result_dict[level]['a__b__c'][year_month][metric] = value
		for level in range(0,len(dimensions)+1):
			concatenated_dimensions = ''
			if level < len(dimensions):
				concatenated_dimensions = '__'.join([row[item] for item in range(1,len(dimensions[:len(dimensions)-level])+1)])
			if not concatenated_dimensions in result_dict[level]:
				result_dict[level][concatenated_dimensions] = {}
			if not row[year_month_index] in result_dict[level][concatenated_dimensions]:
				result_dict[level][concatenated_dimensions][row[year_month_index]] = create_year_month_dict()
			for metric_row_index in range(len(dimensions)+1,len(dimensions)+len(metrics)+1):
				metric = metrics[metric_row_index - len(dimensions) - 1]
				metric_name = metric['name']
				if metric['type'] == 'sum':
					result_dict[level][concatenated_dimensions][row[year_month_index]][metric_name] += float(row[metric_row_index])

	print('Done!')
	print('Filling aditional metrics and current month data...',end='')
	# Fill aditional metrics and current month data:
	historical_year_months.sort(reverse=True)
	current_year_month = historical_year_months[0]
	current_month_ratio = calendar.monthrange(last_sale.year,last_sale.month)[1]/last_sale.day
	for level in result_dict:
		for dimension in result_dict[level]:
			for historical_year_month in historical_year_months:
				dimension_dict = result_dict[level][dimension]
				# Creates metric dict if not exists year month data
				if historical_year_month not in dimension_dict:
					dimension_dict[historical_year_month] = create_year_month_dict()

				row = dimension_dict[historical_year_month]
				row['avg_ticket'] = row['receita_total']/row['itens'] if row['itens'] > 0 else 0
				row['avg_cogs'] = row['custo_total']/row['itens'] if row['itens'] > 0 else 0

				# Fill current month data:
				if historical_year_month == current_year_month:
					itens_a_vender = int(row['itens']*(current_month_ratio-1))
					receita_total_a_vender = itens_a_vender * row['avg_ticket']
					custo_total_a_vender = itens_a_vender * row['avg_cogs']
					row['quantidade_estoque'] = max(0,row['quantidade_estoque'] + row['quantidade_a_receber'] - itens_a_vender)
					row['custo_estoque'] = max(0,row['custo_estoque'] + row['custo_a_receber'] - custo_total_a_vender)
					row['itens'] += itens_a_vender
					row['receita_total'] += receita_total_a_vender
					row['custo_total'] += custo_total_a_vender
				
				# Fill last months stock information:
				else:
					next_year_month = add_months(historical_year_month,1)
					next_year_month_row = dimension_dict[next_year_month]
					quantidade_recebida = next_year_month_row['quantidade_recebida'] + next_year_month_row['quantidade_a_receber'] if next_year_month == current_year_month else next_year_month_row['quantidade_recebida']
					custo_recebido = next_year_month_row['custo_recebido'] + next_year_month_row['custo_a_receber'] if next_year_month == current_year_month else next_year_month_row['custo_recebido']

					row['quantidade_estoque'] = max(0,next_year_month_row['quantidade_estoque'] + next_year_month_row['itens'] - quantidade_recebida)
					row['custo_estoque'] = max(0,next_year_month_row['custo_estoque'] + next_year_month_row['custo_total'] - custo_recebido)

	if last_sale.day <= 15:
		print('\n\tRemoving current month...',end='')
		current_year_month = historical_year_months[1]
	
	print('Done!')
	print('Filling sales projections...',end='')
	# Fill Sales Projections:
	for i in range(1,projection_months_number+1):
		year_month = add_months(current_year_month,i)
		for level in result_dict:
			for dimension in result_dict[level]:
				dimension_dict = result_dict[level][dimension]
				if year_month not in dimension_dict:
					dimension_dict[year_month] = create_year_month_dict()
				row = dimension_dict[year_month]
				
				if level == 0:
					# print(dimension)
					for metric in ('avg_cogs','avg_ticket','itens'):
						# print(metric)
						# AVG COGS:
						metric_value = 0
						# 1) M-1 to M-6 is trustable?
						# Check: if M-1 to M-6 the value was 0
						# if M-1 to M-6 the value was 0, go 1 level up:
						loop_level, dimension_level = get_trustable_parent(result_dict,0,dimension,metric,1,6)
						# 1) ==> M-1 to M-6 is not trustable:
						# if it went all levels up:
						if loop_level > len(dimensions):
							# print('it went all levels up')
							pass
						# if it went 1 or more levels up:
						elif loop_level > 0:
							# print('it went 1 or more levels up')
							metric_value = result_dict[loop_level][dimension_level][add_months(year_month,-1)][metric] if metric != 'itens' else 0
						# 1) ==> M-1 to M-6 is trustable:
						else:
							# 2) M-1 is trustable?
							# Check: M-1 < @MULTIPLIERS[metric]['inferior'] * MIN(M-2 to M-6)
							# Check: M-1 > @MULTIPLIERS[metric]['superior'] * MAX(M-2 to M-6)
							# Check: itens[M-1] < MULTIPLIERS['itens']['min_relevant']
							metric_m2_to_m6 = get_metric_list(result_dict[level][dimension],year_month,metric,year_month_from=2,year_month_to=6)
							metric_m1 = result_dict[level][dimension][add_months(year_month,-1)][metric]
							itens_m1 = result_dict[level][dimension][add_months(year_month,-1)]['itens']
							# 2) ==> M-1 is not trustable:
							if metric_m2_to_m6 == [] or metric_m1 < MULTIPLIERS[metric]['inferior'] * min(metric_m2_to_m6) or metric_m1 > MULTIPLIERS[metric]['superior'] * max(metric_m2_to_m6) or itens_m1 < MULTIPLIERS['itens']['min_relevant']:
								# print('m-1 is not trustable')
								loop_level = 0
								dimension_level = None
								while loop_level < len(dimensions) and (loop_level == 0 or result_dict[loop_level][dimension_level][add_months(year_month,-12)][metric] == 0):
									loop_level += 1
									loop_level, dimension_level = get_trustable_parent(result_dict,loop_level,dimension,metric,12+2,12+6,min_len=3)
								# if it went all levels up:
								if loop_level > len(dimensions):
									# print('it went all levels up')
									pass
								elif len(metric_m2_to_m6) == 0:
									metric_value = metric_m1
								else:
									# print('it went 1 or more levels up')
									avg_metric_m2_to_m6 = sum(metric_m2_to_m6)/float(len(metric_m2_to_m6))
									parent_metrics_m14_to_m18 = get_metric_list(result_dict[loop_level][dimension_level],year_month,metric,year_month_from=12+2,year_month_to=12+6)
									avg_parent_metric_m14_to_m18 = sum(parent_metrics_m14_to_m18)/float(len(parent_metrics_m14_to_m18))
									parent_metric_m12 = result_dict[loop_level][dimension_level][add_months(year_month,-12)][metric]
									metric_value = avg_metric_m2_to_m6 * parent_metric_m12 / avg_parent_metric_m14_to_m18
							# 2) ==> M-1 is trustable:
							else:
								# print('m-1 is trustable')
								# 3) Final relevance checks:
								# Check1: MAX(M-12,M-13) > @MULTIPLIERS[metric]['superior'] * MAX(M-6 to M-1)
								# Check2: MIN(M-12,M-13) < @MULTIPLIERS[metric]['inferior'] * MIN(M-6 to M-1)
								# Check3: MIN(itens[M-12],itens[M-13]) < @MULTIPLIERS['itens']['min_relevant']
								# Check4: Check: M-12 < @MULTIPLIERS[metric]['inferior'] * M-13
								# Check5: Check: M-12 > @MULTIPLIERS[metric]['superior'] * M-13
								# Check6: Check: M-1 < @MULTIPLIERS[metric]['inferior'] * M-13
								# Check7: Check: M-1 > @MULTIPLIERS[metric]['superior'] * M-13
								# Check8: Check: itens[M-1] < @MULTIPLIERS['itens']['inferior'] * itens[M-13]
								# Check9: Check: itens[M-1] > @MULTIPLIERS['itens']['superior'] * itens[M-13]
								metric_m12_to_m13 = get_metric_list(result_dict[level][dimension],year_month,metric,year_month_from=12,year_month_to=13,exclude_zeros=False)
								metric_m1_to_m6 = get_metric_list(result_dict[level][dimension],year_month,metric,year_month_from=1,year_month_to=6)
								itens_m12_to_m13 = get_metric_list(result_dict[level][dimension],year_month,'itens',year_month_from=12,year_month_to=13,exclude_zeros=False)
								checks = []
								checks.append(max(metric_m12_to_m13) > MULTIPLIERS[metric]['superior'] * max(metric_m1_to_m6))
								checks.append(min(metric_m12_to_m13) < MULTIPLIERS[metric]['inferior'] * min(metric_m1_to_m6))
								checks.append(min(itens_m12_to_m13) < MULTIPLIERS['itens']['min_relevant'])
								checks.append(metric_m12_to_m13[0] < MULTIPLIERS[metric]['inferior'] * metric_m12_to_m13[1])
								checks.append(metric_m12_to_m13[0] > MULTIPLIERS[metric]['superior'] * metric_m12_to_m13[1])
								checks.append(metric_m1 < MULTIPLIERS[metric]['inferior'] * metric_m12_to_m13[1])
								checks.append(metric_m1 > MULTIPLIERS[metric]['superior'] * metric_m12_to_m13[1])
								if metric != 'itens':
									checks.append(itens_m1 < MULTIPLIERS['itens']['inferior'] * itens_m12_to_m13[1])
									checks.append(itens_m1 > MULTIPLIERS['itens']['superior'] * itens_m12_to_m13[1])
								# 3) ==> Fail in final relevance checks:
								if True in checks:
									# print('fail in final relevance checks')
									min_value = MULTIPLIERS['itens']['min_relevant'] if metric == 'itens' else 0
									loop_level, dimension_level = get_trustable_parent(result_dict,1,dimension,metric,12,13,min_value=min_value,min_len=2)
									# if it went all levels up:
									if loop_level > len(dimensions):
										# print('it went all levels up')
										pass
									else:
										# print('it went 1 or more levels up')
										parent_metrics_m12_to_m13 = get_metric_list(result_dict[loop_level][dimension_level],year_month,metric,year_month_from=12,year_month_to=13,exclude_zeros=False)
										if metric == 'itens':
											parent_avg_cogs_m12_to_m13 = get_metric_list(result_dict[loop_level][dimension_level],year_month,'avg_cogs',year_month_from=12,year_month_to=13,exclude_zeros=False)
											parent_avg_ticket_m12_to_m13 = get_metric_list(result_dict[loop_level][dimension_level],year_month,'avg_ticket',year_month_from=12,year_month_to=13,exclude_zeros=False)
											parent_pc1p_m12 = 1-parent_avg_cogs_m12_to_m13[0]/parent_avg_ticket_m12_to_m13[0]
											parent_pc1p_m13 = 1-parent_avg_cogs_m12_to_m13[1]/parent_avg_ticket_m12_to_m13[1]
											pc1p_m = 1-row['avg_cogs']/row['avg_ticket']
											pc1p_m1 = 1-dimension_dict[add_months(year_month,-1)]['avg_cogs']/dimension_dict[add_months(year_month,-1)]['avg_ticket']
											parent_pc1p_m13 = 1-parent_avg_cogs_m12_to_m13[1]/parent_avg_ticket_m12_to_m13[1]
											mult_1 = (parent_metrics_m12_to_m13[0]/parent_metrics_m12_to_m13[1])/pow(min(max(MULTIPLIERS['pc1p']['inferior'],(1-parent_pc1p_m12)/(1-parent_pc1p_m13)),MULTIPLIERS['pc1p']['superior']),MULTIPLIERS[metric]['factor'])
											mult_2 = pow(min(max(MULTIPLIERS['pc1p']['inferior'],(1-pc1p_m)/(1-pc1p_m1)),MULTIPLIERS['pc1p']['superior']),MULTIPLIERS[metric]['factor'])
											metric_value = metric_m1 * mult_1 * mult_2
										else:
											metric_value = metric_m1 * parent_metrics_m12_to_m13[0]/parent_metrics_m12_to_m13[1]

								# 3) ==> Pass in final relevance checks:
								else:
									# print('pass in final relevance checks')
									# M-1/M-12*M-13 limited by:
									# 	@MULTIPLIERS[metric]['inferior'] * MIN(M-6 a M-1)
									# 	@MULTIPLIERS[metric]['superior'] * MAX(M-6 a M-1)
									value = metric_m1*metric_m12_to_m13[0]/metric_m12_to_m13[1]
									if metric == 'itens':
										pc1p_m = 1-row['avg_cogs']/row['avg_ticket']
										pc1p_m1 = 1-dimension_dict[add_months(year_month,-1)]['avg_cogs']/dimension_dict[add_months(year_month,-1)]['avg_ticket']
										pc1p_m12 = 1-dimension_dict[add_months(year_month,-12)]['avg_cogs']/dimension_dict[add_months(year_month,-12)]['avg_ticket']
										pc1p_m13 = 1-dimension_dict[add_months(year_month,-13)]['avg_cogs']/dimension_dict[add_months(year_month,-13)]['avg_ticket']
										check1 = pc1p_m/pc1p_m12 if pc1p_m12 > 0 else 0
										check2 = pc1p_m1/pc1p_m13 if pc1p_m13 > 0 else 0
										check3 = pc1p_m/pc1p_m1 if pc1p_m1 > 0 else 0
										check4 = pc1p_m12/pc1p_m13 if pc1p_m13 > 0 else 0
										
										pc1p_check_m = ((check1-check2) + (check3-check4))/2
										value *= pow(1-min(max(MULTIPLIERS['pc1p_check']['inferior'],pc1p_check_m),MULTIPLIERS['pc1p_check']['superior']),MULTIPLIERS[metric]['factor'])
									metric_value = min(max(value,MULTIPLIERS[metric]['inferior']*min(metric_m1_to_m6)),MULTIPLIERS[metric]['superior'] * max(metric_m1_to_m6))
						# preço de venda nao pode ser menor que custo
						if metric == 'avg_ticket':
							metric_value = max(row['avg_cogs'],metric_value)
						last_metric_value = dimension_dict[add_months(year_month,-1)][metric]
						row[metric] = min(max(metric_value,last_metric_value*MULTIPLIERS[metric]['inferior']),last_metric_value*MULTIPLIERS[metric]['superior']) * MULTIPLIERS[metric]['multiplier']
						
						if metric == 'itens':
							row[metric] = int(row[metric])
						# print(row[metric])
					row['receita_total'] = row['avg_ticket'] * row['itens']
					row['custo_total'] = row['avg_cogs'] * row['itens']
				# Level 1 or above:
				else:
					for child_dimension in result_dict[level-1]:
						if child_dimension.startswith(dimension):
							for metric in metrics:
								if metric['type'] == 'sum':
									row[metric['name']] += result_dict[level-1][child_dimension][year_month][metric['name']]

					row['avg_cogs'] = row['custo_total'] / row['itens'] if row['itens'] > 0 else 0
					row['avg_ticket'] = row['receita_total'] / row['itens'] if row['itens'] > 0 else 0
	print('Done!')			
	print('Filling stock projections...',end='')
	# Fill Stock Projections:
	for i in range(1,projection_months_number+1):
		year_month = add_months(current_year_month,i)
		for level in result_dict:
			for dimension in result_dict[level]:
				dimension_dict = result_dict[level][dimension]
				coverage_previous_itens = dimension_dict[year_month]['itens']
				coverage_previous_cogs = dimension_dict[year_month]['custo_total']
				ideal_stock_quantity = 0
				ideal_stock_cost = 0
				for i in range(1,FATOR_DE_COBERTURA_MESES+1):
					if add_months(year_month,i) in dimension_dict:
						coverage_previous_itens = dimension_dict[add_months(year_month,i)]['itens']
						coverage_previous_cogs = dimension_dict[add_months(year_month,i)]['custo_total']
					ideal_stock_quantity += coverage_previous_itens
					ideal_stock_cost += coverage_previous_cogs
				row = dimension_dict[year_month]
				prev_row = dimension_dict[add_months(year_month,-1)]
				row['quantidade_estoque'] = max(ideal_stock_quantity,prev_row['quantidade_estoque']-row['itens'])
				row['custo_estoque'] = max(ideal_stock_cost,prev_row['custo_estoque']-row['custo_total'])
				row['quantidade_recebida'] = max(0,row['quantidade_estoque'] - prev_row['quantidade_estoque'] + row['itens'])
				row['custo_recebido'] = max(0,row['custo_estoque'] - prev_row['custo_estoque'] + row['custo_total'])
	print('Done!')
	print('Updating database...',end='')
	if update_results:
		# Drop current Table:
		drop_query = """
			IF OBJECT_ID('%s') IS NOT NULL
				DROP TABLE %s;
		""" % (output_table,output_table)
		dc.execute(drop_query)

		# Create new Table:
		create_table_dimensions = ''
		for dimension in dimensions:
			create_table_dimensions += '%s VARCHAR(50),\n' % dimension
		create_table_metrics = ''
		for metric in metrics:
			create_table_metrics += '%s %s,\n' % (metric['name'],metric['format'])	
		create_table_query = """
		CREATE TABLE %(output_schema)s.%(output_table)s (
		anomes INT,
		%(create_table_dimensions)s
		%(create_table_metrics)s
		PRIMARY KEY (%(primary_keys)s)
		);""" % {
			'output_schema':output_schema,
			'output_table':output_table,
			'create_table_dimensions':create_table_dimensions,
			'create_table_metrics':create_table_metrics,
			'primary_keys':','.join(['anomes'] + dimensions)
		}
		dc.execute(create_table_query)

		# Insert values into new table
		insert_query_header = 'INSERT INTO %s VALUES ' % output_table
		insert_block_max_size = 50
		insert_query = insert_query_header
		insert_block_size = 0
		insert_block = []
		for dimension in result_dict[0]:
			dimension_values = ["'%s'" % dimension_value for dimension_value in dimension.split('__')]
			for year_month in result_dict[0][dimension]:
				row = result_dict[0][dimension][year_month]
				list_values = [str(year_month)] + dimension_values
				for metric in metrics:
					metric_value = str(int(row[metric['name']])) if metric['format'] == 'int' else str(round(row[metric['name']],2))
					list_values.append(metric_value)
				list_values_string = '\n(%s),' % ','.join(list_values)
				if insert_block_size >= insert_block_max_size:
					insert_block_size = 0
					insert_block.append(insert_query[:-1])
					insert_query = insert_query_header
				insert_block_size += 1
				insert_query += list_values_string

		insert_block.append(insert_query[:-1])
		for insert_query in insert_block:
			dc.execute(insert_query)
	print('Done!')
else:
	print('No Query Results.')