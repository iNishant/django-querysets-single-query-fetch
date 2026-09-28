from datetime import datetime, timezone

from django.db.models import Count
from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import (
    QuerysetsSingleQueryFetch,
    QuerysetCountWrapper,
)
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


class QuerysetCountWrapperTestCase(TransactionTestCase):
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

    def test_works_in_simple_case(self):
        count_queryset = StoreProduct.objects.filter()
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(queryset=count_queryset),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], count_queryset.count())

    def test_works_with_filtered_queryset(self):
        count_filter_queryset = StoreProduct.objects.filter(id=self.product_1.id)
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(queryset=count_filter_queryset),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], count_filter_queryset.count())

    def test_works_with_other_querysets(self):
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

    def test_works_with_distinct_sliced_annotated_and_combined_querysets(self):
        other_store = baker.make(OnlineStore)
        baker.make(StoreProduct, store=other_store, selling_price=1, _quantity=3)
        querysets = [
            StoreProduct.objects.values("store_id").distinct(),
            StoreProduct.objects.order_by("id")[:2],
            StoreProduct.objects.order_by("id")[1:],
            StoreProduct.objects.annotate(
                products_in_store=Count("store__storeproduct")
            ),
            StoreProduct.objects.values("store_id").annotate(products=Count("id")),
            StoreProduct.objects.filter(id=self.product_1.id).union(
                StoreProduct.objects.filter(id=self.product_2.id)
            ),
            # ordering by a related field would change the rows of a distinct
            # query, count() ignores it
            OnlineStore.objects.order_by("storeproduct__name").distinct(),
        ]
        expected = [queryset.count() for queryset in querysets]
        self.assertEqual(expected, [2, 2, 4, 5, 2, 2, 2])

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[QuerysetCountWrapper(queryset) for queryset in querysets]
            ).execute()

        self.assertEqual(results, expected)

    def test_count_is_returned_as_zero_for_empty_queryset(self):
        with self.assertNumQueries(0):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(StoreProduct.objects.none()),
                ]
            ).execute()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], 0)
