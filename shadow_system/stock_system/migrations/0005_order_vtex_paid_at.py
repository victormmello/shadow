# Generated by Django 2.1.3 on 2019-01-28 13:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stock_system', '0004_orderitem_invoiced_quantity'),
    ]

    operations = [
        migrations.AddField(
            model_name='order',
            name='vtex_paid_at',
            field=models.DateTimeField(null=True),
        ),
    ]
