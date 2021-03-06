# Generated by Django 2.1.3 on 2018-12-03 19:53

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Order',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('vtex_id', models.CharField(max_length=30)),
                ('vtex_created_at', models.DateTimeField()),
                ('sequence', models.CharField(max_length=20)),
                ('client_name', models.TextField()),
                ('cpf', models.CharField(max_length=20)),
                ('status', models.CharField(max_length=50)),
                ('total_product_price', models.FloatField()),
                ('total_shipping_price', models.FloatField()),
                ('courier', models.TextField()),
                ('city', models.TextField()),
                ('neighborhood', models.TextField()),
                ('state', models.TextField()),
                ('postal_code', models.TextField()),
                ('street_number', models.TextField()),
                ('number', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='OrderItem',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ean', models.CharField(max_length=20)),
                ('vtex_sku', models.CharField(max_length=20)),
                ('vtex_product_id', models.CharField(max_length=20)),
                ('quantity', models.IntegerField()),
                ('product_name', models.TextField()),
                ('unit_sale_price', models.FloatField()),
                ('order', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='stock_system.Order')),
            ],
        ),
        migrations.CreateModel(
            name='StockItem',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('store', models.TextField()),
                ('positions', models.TextField(null=True)),
                ('quantity', models.IntegerField()),
            ],
        ),
    ]
