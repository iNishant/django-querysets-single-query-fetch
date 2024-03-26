from django.db import models


class OnlineStore(models.Model):
    name = models.CharField(max_length=30, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expired_on = models.DateField(null=True, blank=True)


class StoreProductCategory(models.Model):
    store = models.ForeignKey(OnlineStore, on_delete=models.CASCADE)
    name = models.CharField(max_length=30)


class StoreProduct(models.Model):
    store = models.ForeignKey(OnlineStore, on_delete=models.CASCADE)
    name = models.CharField(max_length=30)
    category = models.ForeignKey(
        StoreProductCategory, on_delete=models.CASCADE, null=True
    )
    selling_price = models.DecimalField(max_digits=10, decimal_places=2)
