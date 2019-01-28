from django.core.management.base import BaseCommand
from django.db.transaction import atomic

from shadow_database import DatabaseConnection
from shadow_helpers.helpers import make_dict, get_from_dict, set_in_dict

from stock_system.models import Order, OrderItem
from django.utils import timezone

class Command(BaseCommand):
	def handle(self, *args, **kwargs):
		current_tz = timezone.get_current_timezone()

		db_order_items = OrderItem.objects.all().prefetch_related('order')
		db_orders = Order.objects.all()

		existing_order_items_dict = make_dict(db_order_items, None, ['order__vtex_id', 'ean'])
		existing_orders_dict = make_dict(db_orders, None, ['vtex_id'])

		dc = DatabaseConnection()

		order_item_infos = dc.select("""
			SELECT
				voi.*,
				coalesce(e.estoque_disponivel, 0) as stock_warehouse
			from dbo.bi_vtex_order_items voi
			left join w_estoque_disponivel_sku e on e.codigo_barra = voi.ean and e.filial = 'e-commerce'
			where voi.created_at > '2018-12-26 10:03:33'
			;
		""", strip=True, dict_format=True)

		order_infos = make_dict(order_item_infos, None, ['order_id'])
		order_dict = {}
		for order_info in order_infos.values():
			order = get_from_dict(existing_orders_dict, [order_info['order_id']])
			if not order:
				order = Order()
				order.vtex_id = order_info['order_id']
			
			order.vtex_created_at = current_tz.normalize(order_info['created_at'].astimezone(current_tz))
			if order_info['invoiced_at']:
				order.vtex_invoiced_at = current_tz.normalize(order_info['invoiced_at'].astimezone(current_tz))
			if order_info['paid_at']:
				order.vtex_paid_at = current_tz.normalize(order_info['paid_at'].astimezone(current_tz))

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

			order.payment_method_group = order_info['payment_method_group']

			order_dict[order.vtex_id] = order

		with atomic():
			# OrderItem.objects.all().delete()
			# Order.objects.all().delete()

			for order in order_dict.values():
				order.save()

			order_items_to_create = []
			order_items_to_update = []
			for order_item_info in order_item_infos:
				order_item = get_from_dict(existing_order_items_dict, [order_item_info['order_id'], order_item_info['ean']])
				if order_item:
					order_items_to_update.append(order_item)
				else:
					order_item = OrderItem()
					order = order_dict[order_item_info['order_id']]
					order_item.order_id = order.id

					order_items_to_create.append(order_item)

				order_item.ean = order_item_info['ean']
				order_item.vtex_sku = order_item_info['vtex_sku']
				order_item.vtex_product_id = order_item_info['vtex_product_id']
				order_item.quantity = order_item_info['quantity']
				order_item.product_name = order_item_info['name']
				order_item.unit_sale_price = order_item_info['price']
				order_item.image_link = order_item_info['image_link']
				order_item.stock_warehouse = order_item_info['stock_warehouse']
				order_item.invoiced_quantity = order_item_info['invoiced_quantity']

			OrderItem.objects.bulk_create(order_items_to_create, batch_size=99)

			for order_item in order_items_to_update:
				order_item.save()