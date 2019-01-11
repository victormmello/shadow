from django.views.generic import ListView, TemplateView
from stock_system.models import OrderItem, Order

from django.db.models import Sum, Case, Value as V, When, IntegerField, Q
import datetime

class OrderList(ListView):
	template_name = 'stock_system/order_list.html'

	def get_queryset(self):
		return Order.objects.all().order_by('-vtex_created_at').prefetch_related('order_items')

	# def get_context_data(self, **kwargs):
	# 	context = super(OrderList, self).get_context_data(**kwargs)

	# 	return context

class OrderDashboard(TemplateView):
	template_name = 'stock_system/order_dashboard.html'

	def get_context_data(self, **kwargs):
		context = super(OrderDashboard, self).get_context_data(**kwargs)

		today = datetime.datetime.today().date()
		yesterday = today - datetime.timedelta(days=1)
		last_week = today - datetime.timedelta(days=7)

		filter_1d = Q(vtex_invoiced_at__gte=yesterday) & Q(vtex_invoiced_at__lt=today)
		filter_7d = Q(vtex_invoiced_at__gte=last_week) & Q(vtex_invoiced_at__lt=today)

		order_info = Order.objects.all().aggregate(
			paid=Sum(Case(When(status="Preparando Entrega", then=V(1)), default=0, output_field=IntegerField())),
			not_paid=Sum(Case(When(status="Pagamento Pendente", then=V(1)), default=0, output_field=IntegerField())),

			invoiced_1d=Sum(Case(When(filter_1d, then=V(1)), default=0, output_field=IntegerField())),
			invoiced_7d=Sum(Case(When(filter_7d, then=V(1)), default=0, output_field=IntegerField())),
		)

		order_info['total'] = order_info['paid'] + order_info['not_paid']

		context['total'] = order_info['total']
		context['paid'] = order_info['paid']
		context['not_paid'] = order_info['not_paid']
		context['invoiced_1d'] = order_info['invoiced_1d']
		context['invoiced_7d'] = order_info['invoiced_7d']

		return context