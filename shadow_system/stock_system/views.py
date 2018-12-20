from django.views.generic import ListView
from stock_system.models import OrderItem

class OrderList(ListView):
    def get_queryset(self):
    	return OrderItem.objects.all().select_related('order')

    def get_context_data(self, **kwargs):
    	context = super(OrderList, self).get_context_data(**kwargs)

    	return context