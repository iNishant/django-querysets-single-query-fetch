from decimal import Decimal

from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import QuerysetsSingleQueryFetch
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


class QuerysetsSingleQueryFetchPostgresTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.store = baker.make(OnlineStore)
        self.category = baker.make(StoreProductCategory, store=self.store)
        self.product_1 = baker.make(
            StoreProduct, store=self.store, category=self.category, selling_price=50.22
        )
        self.product_2 = baker.make(
            StoreProduct, store=self.store, category=self.category, selling_price=100.33
        )

    def test_multiple_querysets_are_fetched_in_a_single_query(self):
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    StoreProduct.objects.filter(id=self.product_1.id),
                    StoreProductCategory.objects.filter(id=self.category.id),
                ]
            ).execute()

            self.assertEqual(len(results), 2)
            fetched_product_instance = results[0][0]
            fetched_category_instance = results[1][0]
            self.assertIsInstance(fetched_product_instance, StoreProduct)
            self.assertEqual(fetched_product_instance.id, self.product_1.id)
            self.assertIsInstance(fetched_category_instance, StoreProductCategory)
            self.assertEqual(fetched_category_instance.id, self.category.id)

    def test_select_related_in_querysets_work(self):
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    StoreProduct.objects.filter(id=self.product_1.id).select_related(
                        "store"
                    ),
                    StoreProductCategory.objects.filter(id=self.category.id),
                ]
            ).execute()

            self.assertEqual(len(results), 2)
            fetched_product_instance = results[0][0]
            fetched_category_instance = results[1][0]

            self.assertIsInstance(fetched_product_instance, StoreProduct)
            self.assertEqual(fetched_product_instance.id, self.product_1.id)

            self.assertIsInstance(fetched_category_instance, StoreProductCategory)
            self.assertEqual(fetched_category_instance.id, self.category.id)

            store = (
                fetched_product_instance.store
            )  # this should not make a new db query
            self.assertEqual(store.id, self.store.id)

    def test_single_query_result_is_of_proper_types(self):
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[StoreProduct.objects.filter(id=self.product_1.id)]
            ).execute()

            self.assertEqual(len(results), 1)
            fetched_product_instance = results[0][0]
            self.assertIsInstance(fetched_product_instance, StoreProduct)
            self.assertEqual(fetched_product_instance.id, self.product_1.id)
            self.assertEqual(fetched_product_instance.selling_price, Decimal("50.22"))

    def test_executing_single_queryset_which_is_always_empty_is_handled(self):
        """
        if there is only one queryset and it is always empty, the result should be an empty list,
        there should not be any db query made
        """
        with self.assertNumQueries(0):
            results = QuerysetsSingleQueryFetch(
                querysets=[StoreProduct.objects.filter(id__in=[])]
            ).execute()

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0], [])

    def test_executing_multiple_querysets_which_are_always_empty_is_handled(self):
        """
        if there are multiple querysets which are always empty, the result should be an empty list
        for each queryset, there should not be any db query made as well
        """
        with self.assertNumQueries(0):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    StoreProduct.objects.filter(id__in=[]),
                    OnlineStore.objects.filter(id__in=[]),
                ]
            ).execute()

            self.assertEqual(len(results), 2)
            self.assertEqual(results[0], [])
            self.assertEqual(results[1], [])
