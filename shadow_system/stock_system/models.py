from django.db import models

class Order(models.Model):
	vtex_id = models.CharField(max_length=30)
	vtex_created_at = models.DateTimeField()
	vtex_invoiced_at = models.DateTimeField(null=True)
	vtex_paid_at = models.DateTimeField(null=True)
	sequence = models.CharField(max_length=20)
	client_name = models.TextField()
	cpf = models.CharField(max_length=20)
	status = models.CharField(max_length=50)
	total_product_price = models.FloatField()
	total_shipping_price = models.FloatField()

	# delivery info
	courier = models.TextField()
	city = models.TextField()
	neighborhood = models.TextField()
	state = models.TextField()
	postal_code = models.TextField()
	street_number = models.TextField()
	payment_method_group = models.TextField()

	def invoiced_quantity(self):
		return sum([x.invoiced_quantity for x in self.order_items.all()])

	class Meta:
		app_label = 'stock_system'


class OrderItem(models.Model):
	ean = models.CharField(max_length=20)
	vtex_sku =  models.CharField(max_length=20)
	vtex_product_id = models.CharField(max_length=20)
	quantity = models.IntegerField()
	product_name = models.TextField()
	unit_sale_price = models.FloatField()
	image_link = models.TextField()
	stock_warehouse = models.IntegerField()
	invoiced_quantity = models.IntegerField()
	warehouse_position = models.CharField(max_length=5, null=True)

	order = models.ForeignKey(Order, on_delete=models.PROTECT, related_name='order_items')

	class Meta:
		app_label = 'stock_system'

class StockItem(models.Model):
	store = models.TextField()
	positions = models.TextField(null=True)

	quantity = models.IntegerField()

	class Meta:
		app_label = 'stock_system'