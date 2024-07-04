from datetime import datetime, timezone
from django_querysets_single_query_fetch.service import (
    QuerysetsSingleQueryFetch,
    QuerysetGetOrNoneWrapper,
)
from django.test import TransactionTestCase
from model_bakery import baker

from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


class QuerysetGetOrNoneWrapperPostgresTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.today = datetime.now(tz=timezone.utc)
        self.store = baker.make(OnlineStore, expired_on=self.today)
        self.store = OnlineStore.objects.get(
            id=self.store.id
        )  # force refresh from db so that types are the default
        # types
        self.category = baker.make(StoreProductCategory, store=self.store)
        self.product_1 = baker.make(StoreProduct, store=self.store, selling_price=50.22)
        self.product_2 = baker.make(
            StoreProduct, store=self.store, category=self.category, selling_price=100.33
        )

    def test_get_or_none_wrapper_with_single_row_matching(self):
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetGetOrNoneWrapper(
                        StoreProduct.objects.filter(id=self.product_1.id)
                    ),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        product = results[0]
        self.assertEqual(product.id, self.product_1.id)

    def test_get_or_none_wrapper_with_no_row_matching(self):
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetGetOrNoneWrapper(StoreProduct.objects.filter(id=-1)),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        product = results[0]
        self.assertIsNone(product)

    def test_get_or_none_wrapper_with_multiple_rows_matching(self):
        with self.assertNumQueries(1):
            # get in this case can return either product 1 or product 2
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetGetOrNoneWrapper(StoreProduct.objects.all()),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        product = results[0]
        self.assertTrue(
            (product.id == self.product_1.id) or (product.id == self.product_2.id)
        )
