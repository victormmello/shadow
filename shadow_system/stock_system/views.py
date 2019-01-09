from django.views.generic import ListView, TemplateView
from stock_system.models import OrderItem, Order

from django.db.models import Sum, Case, Value as V, When, IntegerField

class OrderList(ListView):
	template_name = 'stock_system/order_list.html'

	def get_queryset(self):
		return Order.objects.all().prefetch_related('order_items')

	# def get_context_data(self, **kwargs):
	# 	context = super(OrderList, self).get_context_data(**kwargs)

	# 	return context

class OrderDashboard(TemplateView):
	template_name = 'stock_system/order_dashboard.html'

	def get_context_data(self, **kwargs):
		context = super(OrderDashboard, self).get_context_data(**kwargs)

		order_info = Order.objects.all().aggregate(
			paid=Sum(Case(When(status="Preparando Entrega", then=V(1)), default=0, output_field=IntegerField())),
			not_paid=Sum(Case(When(status="Pagamento Pendente", then=V(1)), default=0, output_field=IntegerField())),
			invoiced=Sum(Case(When(status="Faturado", then=V(1)), default=0, output_field=IntegerField())),
		)

		order_info['total'] = order_info['paid'] + order_info['not_paid']

		context['total'] = order_info['total']
		context['paid'] = order_info['paid']
		context['not_paid'] = order_info['not_paid']
		context['invoiced'] = order_info['invoiced']

		return context