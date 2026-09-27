from unittest import skipIf

import django
from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import (
    QuerysetGetOrNoneWrapper,
    QuerysetsSingleQueryFetch,
)
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


@skipIf(django.VERSION < (6, 1), "fetch modes were added in django 6.1")
class FetchModesTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.store_1 = baker.make(OnlineStore)
        self.store_2 = baker.make(OnlineStore)
        self.category = baker.make(StoreProductCategory, store=self.store_1)
        self.product_1 = baker.make(
            StoreProduct, store=self.store_1, category=self.category, selling_price=10
        )
        self.product_2 = baker.make(StoreProduct, store=self.store_2, selling_price=20)

    def test_fetch_one_is_the_default_fetch_mode(self):
        from django.db.models import FETCH_ONE

        with self.assertNumQueries(1):
            products = QuerysetsSingleQueryFetch(
                querysets=[StoreProduct.objects.order_by("id")]
            ).execute()[0]

        self.assertEqual(len(products), 2)
        for product in products:
            self.assertIs(product._state.fetch_mode, FETCH_ONE)
        with self.assertNumQueries(2):
            self.assertEqual(products[0].store, self.store_1)
            self.assertEqual(products[1].store, self.store_2)

    def test_fetch_peers_fetches_related_objects_for_all_peers_together(self):
        from django.db.models import FETCH_PEERS

        with self.assertNumQueries(1):
            products = QuerysetsSingleQueryFetch(
                querysets=[StoreProduct.objects.fetch_mode(FETCH_PEERS).order_by("id")]
            ).execute()[0]

        self.assertEqual(len(products), 2)
        with self.assertNumQueries(1):
            self.assertEqual(products[0].store, self.store_1)
            self.assertEqual(products[1].store, self.store_2)

    def test_fetch_raise_blocks_fetching_on_fetched_and_select_related_objects(self):
        from django.core.exceptions import FieldFetchBlocked
        from django.db.models import FETCH_RAISE

        with self.assertNumQueries(1):
            products, product = QuerysetsSingleQueryFetch(
                querysets=[
                    StoreProduct.objects.fetch_mode(FETCH_RAISE)
                    .select_related("category")
                    .only("name", "category__name")
                    .order_by("id"),
                    QuerysetGetOrNoneWrapper(
                        StoreProduct.objects.fetch_mode(FETCH_RAISE).filter(
                            id=self.product_2.id
                        )
                    ),
                ]
            ).execute()

        self.assertEqual(len(products), 2)
        self.assertEqual(products[0].name, self.product_1.name)
        self.assertEqual(products[0].category, self.category)
        self.assertIsNone(products[1].category)
        with self.assertRaises(FieldFetchBlocked):
            products[0].selling_price  # deferred field
        with self.assertRaises(FieldFetchBlocked):
            products[0].category.store  # select_related object
        self.assertEqual(product.id, self.product_2.id)
        with self.assertRaises(FieldFetchBlocked):
            product.store
