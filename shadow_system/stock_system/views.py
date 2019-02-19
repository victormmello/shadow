from stock_system.models import OrderItem, Order

from django.views.generic import ListView, TemplateView
from django.db.models import Sum, Case, Value as V, When, IntegerField, Q
from django.db import connection

from shadow_database import DatabaseConnection

import datetime, copy


def execute_query(query):
	cursor = connection.cursor()
	cursor.execute(query)
	columns = [col[0] for col in cursor.description]
	return [
		dict(zip(columns, row))
		for row in cursor.fetchall()
	]

class OrderList(ListView):
	template_name = 'stock_system/order_list2.html'
	filter_fields = [
		{
			'name': 'Pedido',
			'field': 'sequence',
		},
		{
			'name': 'EAN',
			'field': 'order_items__ean',
		},
		{
			'name': 'Cliente',
			'field': 'client_name',
		},
		{
			'name': 'Status',
			'field': 'status',
			'choices': ['Tudo', 'Preparando Entrega', 'Pagamento Pendente', 'Faturado', 'Cancelado', 'Pronto para o manuseio'],
		},
	]

	def get_queryset(self):
		orders = Order.objects.order_by('-vtex_created_at').prefetch_related('order_items')

		get_params = dict(self.request.GET)
		if get_params:
			for field, values in get_params.items():
				values = [x for x in values if x]

				if len(values) > 1:
					field = field + '__in'
					orders = orders.filter(**{field: [x.upper() for x in values]})

				elif len(values) == 1:
					if field == 'status' and values[0] == 'Tudo':
						continue
						
					if field == 'order_items__ean':
						field = field + '__iexact'
					else:
						field = field + '__icontains'

					orders = orders.filter(**{field: values[0]})

				print({field: values})

		for order in orders:
			order.status_color = 'black'
			if order.status == 'Cancelado':
				order.status_color = 'red'
			elif order.status == 'Pagamento Pendente':
				order.status_color = 'orange'
			elif order.status == 'Faturado':
				order.status_color = 'green'
				

		return orders

	def get_context_data(self, **kwargs):
		context = super(OrderList, self).get_context_data(**kwargs)

		filter_fields = copy.deepcopy(self.filter_fields)
		for filter_field in filter_fields:
			value = self.request.GET.get(filter_field['field'])
			if value:
				filter_field['value'] = value

		context['filter_fields'] = filter_fields

		return context

class OrderDashboard(TemplateView):
	template_name = 'stock_system/order_dashboard.html'

	def get_context_data(self, **kwargs):
		context = super(OrderDashboard, self).get_context_data(**kwargs)

		today = datetime.datetime.today().date()
		yesterday = today - datetime.timedelta(days=1)
		last_week = today - datetime.timedelta(days=7)

		filter_1d = Q(vtex_invoiced_at__gte=yesterday) & Q(vtex_invoiced_at__lt=today)
		filter_7d = Q(vtex_invoiced_at__gte=last_week) & Q(vtex_invoiced_at__lt=today)

		query = """
			SELECT
				SUM(
					CASE
						WHEN o.status = 'Preparando Entrega' and f.invoiced_quantity == 0 then 1 else 0
					end
				) as paid,
				SUM(
					CASE
						WHEN o.status = 'Pagamento Pendente' then 1 else 0
					end
				) as not_paid,
				0 as invoiced_1d,
				0 as invoiced_7d
			FROM stock_system_order o
			INNER JOIN (
				select order_id, sum(invoiced_quantity) as invoiced_quantity
				from stock_system_orderitem
				group by order_id
			) f on f.order_id = o.id
		"""

		order_current_summary = execute_query(query)[0]

		# order_current_summary = Order.objects.all().aggregate(
		# 	paid=Sum(Case(When(status="Preparando Entrega", then=V(1)), default=0, output_field=IntegerField())),
		# 	not_paid=Sum(Case(When(status="Pagamento Pendente", then=V(1)), default=0, output_field=IntegerField())),

		# 	invoiced_1d=Sum(Case(When(filter_1d, then=V(1)), default=0, output_field=IntegerField())),
		# 	invoiced_7d=Sum(Case(When(filter_7d, then=V(1)), default=0, output_field=IntegerField())),
		# )

		order_current_summary['total'] = order_current_summary['paid'] + order_current_summary['not_paid']

		context['order_current_summary'] = order_current_summary



		# order_current_summary = Order.objects.all().aggregate(
		# 	paid=Sum(Case(When(status="Preparando Entrega", then=V(1)), default=0, output_field=IntegerField())),
		# 	not_paid=Sum(Case(When(status="Pagamento Pendente", then=V(1)), default=0, output_field=IntegerField())),

		# 	invoiced_1d=Sum(Case(When(filter_1d, then=V(1)), default=0, output_field=IntegerField())),
		# 	invoiced_7d=Sum(Case(When(filter_7d, then=V(1)), default=0, output_field=IntegerField())),
		# )		


		return context

class StockPosition(TemplateView):
	template_name = 'stock_system/stock_position.html'

	def get_context_data(self, **kwargs):
		context = super(StockPosition, self).get_context_data(**kwargs)

		ean = self.request.GET.get('ean')
		context['ean'] = ean

		if ean:
			dc = DatabaseConnection()

			ean_position = dc.select("""
				SELECT
					ps.codigo_barra as ean,
					dsp.position as position,
					LEFT(ps.codigo_barra, LEN(ps.codigo_barra) - LEN(ps.grade))
				from dbo.produtos_barra ps
				left join dbo.bi_django_stock_position dsp on dsp.product_color = LEFT(ps.codigo_barra, LEN(ps.codigo_barra) - LEN(ps.grade))
				where ps.codigo_barra = '%s'
				;
			""" % ean, strip=True, dict_format=True)

			if not ean_position:
				context['position'] = 'Produto não existe'

			else:
				ean_position = ean_position[0]
				context['position'] = ean_position['position']

		return context

class SkuHistory(ListView):
	template_name = 'stock_system/sku_history.html'

	def get_queryset(self):
		ean = self.request.GET.get('ean')

		if not ean:
			return []
		else:
			dc = DatabaseConnection()

			sku_history = dc.select("""
				SELECT 
					t.*
				from (
					SELECT distinct
						ps.codigo_barra as ean,
						'saida' as action,
						case 
							when lsp.DATA_PARA_TRANSFERENCIA > ls.DATA_PARA_TRANSFERENCIA then lsp.DATA_PARA_TRANSFERENCIA
							else ls.DATA_PARA_TRANSFERENCIA 
						end as transf_date,
						lsp.QTDE_SAIDA as qnt,
						ls.FILIAL_DESTINO as filial,
						ls.NUMERO_NF_TRANSFERENCIA as nf
					FROM loja_saidas ls
					INNER JOIN loja_saidas_produto lsp on lsp.ROMANEIO_PRODUTO = ls.ROMANEIO_PRODUTO
					inner join produtos_barra ps on ps.produto = lsp.produto and ps.COR_PRODUTO = lsp.COR_PRODUTO
					where 1=1
						and ls.filial = 'e-commerce'
						and case 
							when lsp.DATA_PARA_TRANSFERENCIA > ls.DATA_PARA_TRANSFERENCIA then lsp.DATA_PARA_TRANSFERENCIA
							else ls.DATA_PARA_TRANSFERENCIA 
						end >= DATEADD(day, -30, GETDATE())
					UNION
					SELECT distinct
						ps.codigo_barra,
						'entrada',
						case 
							when lep.DATA_PARA_TRANSFERENCIA > le.DATA_PARA_TRANSFERENCIA then lep.DATA_PARA_TRANSFERENCIA
							else le.DATA_PARA_TRANSFERENCIA 
						end,
						lep.QTDE_ENTRADA,
						le.FILIAL_ORIGEM,
						le.NUMERO_NF_TRANSFERENCIA
					FROM loja_entradas le
					INNER JOIN loja_entradas_produto lep on lep.ROMANEIO_PRODUTO = le.ROMANEIO_PRODUTO
					inner join produtos_barra ps on ps.produto = lep.produto and ps.COR_PRODUTO = lep.COR_PRODUTO
					where 1=1
						and le.filial = 'e-commerce'
						and case 
							when lep.DATA_PARA_TRANSFERENCIA > le.DATA_PARA_TRANSFERENCIA then lep.DATA_PARA_TRANSFERENCIA
							else le.DATA_PARA_TRANSFERENCIA 
						end >= DATEADD(day, -30, GETDATE())
					UNION
					SELECT
						lvp.CODIGO_BARRA,
						'venda',
						data_venda,
						lvp.qtde,
						'venda',
						'venda'
					FROM dbo.LOJA_VENDA_PRODUTO lvp
					inner join dbo.FILIAIS fil on fil.cod_filial = lvp.codigo_filial
					INNER JOIN produtos p on lvp.produto = p.produto
					where 1=1
						and fil.filial = 'e-commerce' 
						and data_venda >= DATEADD(day, -30, GETDATE())
						and p.grupo_produto != 'GIFTCARD'
						and lvp.qtde > 0
				) t
				where t.ean = '%s'
				order by t.transf_date
				;
			""" % ean, strip=True, dict_format=True)

			return sku_history

	def get_context_data(self, **kwargs):
		context = super(SkuHistory, self).get_context_data(**kwargs)

		ean = self.request.GET.get('ean')
		context['ean'] = ean

		return context