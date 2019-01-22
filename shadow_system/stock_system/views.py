from django.views.generic import ListView, TemplateView
from stock_system.models import OrderItem, Order

from django.db.models import Sum, Case, Value as V, When, IntegerField, Q
import datetime, copy

class OrderList(ListView):
	template_name = 'stock_system/order_list.html'
	filter_fields = [
		{
			'name': 'Pedido',
			'field': 'sequence',
		},
		{
			'name': 'EAN',
			'field': 'order_items__ean',
		},
	]

	def get_queryset(self):
		orders = Order.objects.order_by('-vtex_created_at').prefetch_related('order_items')

		get_params = dict(self.request.GET)
		if get_params:
			for field, values in get_params.items():
				if any(values):
					field = field + '__in'
					orders = orders.filter(**{field: [x.upper() for x in values]})

					print({field: values})

		return orders

	def get_context_data(self, **kwargs):
		context = super(OrderList, self).get_context_data(**kwargs)

		filter_fields = copy.deepcopy(self.filter_fields)
		for filter_field in filter_fields:
			value = self.request.GET.get(filter_field['field'])
			if value:
				filter_field['value'] = value.upper()

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

		order_current_summary = Order.objects.all().aggregate(
			paid=Sum(Case(When(status="Preparando Entrega", then=V(1)), default=0, output_field=IntegerField())),
			not_paid=Sum(Case(When(status="Pagamento Pendente", then=V(1)), default=0, output_field=IntegerField())),

			invoiced_1d=Sum(Case(When(filter_1d, then=V(1)), default=0, output_field=IntegerField())),
			invoiced_7d=Sum(Case(When(filter_7d, then=V(1)), default=0, output_field=IntegerField())),
		)

		order_current_summary['total'] = order_current_summary['paid'] + order_current_summary['not_paid']

		context['order_current_summary'] = order_current_summary



		# order_current_summary = Order.objects.all().aggregate(
		# 	paid=Sum(Case(When(status="Preparando Entrega", then=V(1)), default=0, output_field=IntegerField())),
		# 	not_paid=Sum(Case(When(status="Pagamento Pendente", then=V(1)), default=0, output_field=IntegerField())),

		# 	invoiced_1d=Sum(Case(When(filter_1d, then=V(1)), default=0, output_field=IntegerField())),
		# 	invoiced_7d=Sum(Case(When(filter_7d, then=V(1)), default=0, output_field=IntegerField())),
		# )		


		return context