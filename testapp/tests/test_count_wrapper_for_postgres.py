from datetime import datetime, timezone

from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import (
    QuerysetsSingleQueryFetch,
    QuerysetCountWrapper,
)
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


class QuerysetCountWrapperPostgresTestCase(TransactionTestCase):
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

    def test_fetch_count(self):
        """
        - test fetch count works in single query
        _ test fetch count works with filter querysets
        _ test fetch count works with other querysets
        """
        # test fetch count works in single query
        count_queryset = StoreProduct.objects.filter()
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(queryset=count_queryset),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], count_queryset.count())

        # test fetch count works with filter querysets
        count_filter_queryset = StoreProduct.objects.filter(id=self.product_1.id)
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(queryset=count_filter_queryset),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], count_filter_queryset.count())

        # test fetch count works with other querysets
        count_queryset = StoreProduct.objects.filter()
        count_filter_queryset = StoreProduct.objects.filter(id=self.product_1.id)
        queryset = StoreProduct.objects.filter()
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(queryset=count_queryset),
                    QuerysetCountWrapper(queryset=count_filter_queryset),
                    queryset,
                ]
            ).execute()
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], count_queryset.count())
        self.assertEqual(results[1], count_filter_queryset.count())
        self.assertEqual(results[2], list(queryset))
