from django.core.management.base import BaseCommand
from django.db.transaction import atomic

from shadow_database import DatabaseConnection
from shadow_helpers.helpers import make_dict

from stock_system.models import Order, OrderItem


class Command(BaseCommand):
	def handle(self, *args, **kwargs):
		dc = DatabaseConnection()

		order_item_infos = dc.select("""
			SELECT 
				*
			from dbo.bi_vtex_order_items voi
			;
		""", strip=True, dict_format=True)

		order_infos = make_dict(order_item_infos, None, ['order_id'])
		order_dict = {}
		for order_info in order_infos.values():
			order = Order()

			order.vtex_id = order_info['order_id']
			order.vtex_created_at = order_info['created_at']
			order.sequence = order_info['order_sequence']
			order.client_name = order_info['client_name']
			order.cpf = order_info['cpf']
			order.status = order_info['status']
			order.total_product_price = order_info['total_product_price']
			order.total_shipping_price = order_info['total_shipping_price']
			order.courier = order_info['courier']
			order.city = order_info['city']
			order.neighborhood = order_info['neighborhood']
			order.state = order_info['state']
			order.postal_code = order_info['postal_code']
			order.street_number = order_info['street_number']
			order.number = order_info['street_number']

			order_dict[order.vtex_id] = order

		with atomic():
			OrderItem.objects.all().delete()
			Order.objects.all().delete()

			for order in order_dict.values():
				order.save()

			order_items = []
			for order_item_info in order_item_infos:
				order_item = OrderItem()

				order = order_dict[order_item_info['order_id']]

				order_item.order_id = order.id
				order_item.ean = order_item_info['ean']
				order_item.vtex_sku = order_item_info['vtex_sku']
				order_item.vtex_product_id = order_item_info['vtex_product_id']
				order_item.quantity = order_item_info['quantity']
				order_item.product_name = order_item_info['name']
				order_item.unit_sale_price = order_item_info['price']

				order_items.append(order_item)

			OrderItem.objects.bulk_create(order_items, batch_size=99)