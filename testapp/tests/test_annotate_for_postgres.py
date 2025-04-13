from datetime import datetime, timezone
from decimal import Decimal

from django.db.models import Count, F, Value, IntegerField, Sum
from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import (
    QuerysetsSingleQueryFetch,
    QuerysetCountWrapper,
)
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


class QuerysetAnnotatePostgresTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.today = datetime.now(tz=timezone.utc)
        self.store = baker.make(OnlineStore, expired_on=self.today)
        self.store = OnlineStore.objects.get(
            id=self.store.id
        )  # force refresh from db so that types are the default
        self.category1 = baker.make(StoreProductCategory, store=self.store)
        self.category2 = baker.make(StoreProductCategory, store=self.store)
        self.product_1 = baker.make(StoreProduct, store=self.store, selling_price=50.22)
        self.product_2 = baker.make(
            StoreProduct,
            store=self.store,
            category=self.category1,
            selling_price=100.33,
        )
        self.product_3 = baker.make(
            StoreProduct, store=self.store, category=self.category1, selling_price=75.50
        )
        self.product_4 = baker.make(
            StoreProduct,
            store=self.store,
            category=self.category2,
            selling_price=120.75,
        )

    def test_simple_annotate_with_constant(self):
        """Test annotating with a constant value"""
        queryset = StoreProduct.objects.filter().annotate(test_val=Value(10))

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        products = results[0]
        regular_products = list(queryset)

        self.assertEqual(len(products), len(regular_products))
        for product, regular_product in zip(products, regular_products):
            self.assertEqual(product.test_val, regular_product.test_val)
            self.assertEqual(product.test_val, 10)

    def test_annotate_with_count(self):
        """Test annotating with Count"""
        queryset = StoreProductCategory.objects.filter().annotate(
            product_count=Count("storeproduct")
        )

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        categories = results[0]
        regular_categories = list(queryset)

        self.assertEqual(len(categories), len(regular_categories))
        for category, regular_category in zip(categories, regular_categories):
            self.assertEqual(category.product_count, regular_category.product_count)

        category_counts = {
            category.id: category.product_count for category in categories
        }
        self.assertEqual(category_counts[self.category1.id], 2)  # Two products
        self.assertEqual(category_counts[self.category2.id], 1)  # One product

    def test_annotate_with_f_expression(self):
        """Test annotating with F expression"""
        queryset = StoreProduct.objects.filter().annotate(
            doubled_price=F("selling_price") * 2
        )

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        products = results[0]
        regular_products = list(queryset)

        self.assertEqual(len(products), len(regular_products))
        for product, regular_product in zip(products, regular_products):
            self.assertAlmostEqual(
                float(product.doubled_price),
                float(regular_product.doubled_price),
                places=2,
            )
            self.assertAlmostEqual(
                float(product.doubled_price), float(product.selling_price * 2), places=2
            )

    def test_multiple_annotations(self):
        """Test multiple annotations in a single queryset"""
        queryset = StoreProductCategory.objects.filter().annotate(
            product_count=Count("storeproduct"),
            test_val=Value(5, output_field=IntegerField()),
        )

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        categories = results[0]
        regular_categories = list(queryset)

        self.assertEqual(len(categories), len(regular_categories))
        for category, regular_category in zip(categories, regular_categories):
            self.assertEqual(category.product_count, regular_category.product_count)
            self.assertEqual(category.test_val, regular_category.test_val)
            self.assertEqual(category.test_val, 5)

    def test_empty_queryset_with_annotation(self):
        """Test annotating an empty queryset"""
        queryset = StoreProduct.objects.none().annotate(test_val=Value(10))

        with self.assertNumQueries(0):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], [])

    def test_aggregate_query_with_count_inside_annotate(self):
        """Test aggregate query with Count inside annotate"""
        queryset = (
            StoreProductCategory.objects.filter()
            .annotate(product_count=Count("storeproduct"))
            .filter(product_count__gt=0)
        )

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        categories = results[0]
        regular_categories = list(queryset)

        self.assertEqual(len(categories), len(regular_categories))
        for category, regular_category in zip(categories, regular_categories):
            self.assertEqual(category.product_count, regular_category.product_count)

        for category in categories:
            self.assertGreater(category.product_count, 0)

    def test_mix_of_annotated_and_regular_querysets(self):
        """Test mixture of annotated and regular querysets"""
        annotated_queryset = StoreProductCategory.objects.filter().annotate(
            product_count=Count("storeproduct")
        )
        regular_queryset = StoreProduct.objects.filter()

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[annotated_queryset, regular_queryset]
            ).execute()

        self.assertEqual(len(results), 2)
        categories = results[0]
        products = results[1]

        regular_categories = list(annotated_queryset)
        regular_products = list(regular_queryset)

        self.assertEqual(len(categories), len(regular_categories))
        self.assertEqual(len(products), len(regular_products))

        for category, regular_category in zip(categories, regular_categories):
            self.assertEqual(category.product_count, regular_category.product_count)

    def test_mix_with_count_wrapper_and_annotated_queryset(self):
        """Test mixture of count wrapper and annotated queryset"""
        count_queryset = StoreProduct.objects.filter()
        annotated_queryset = StoreProductCategory.objects.filter().annotate(
            product_count=Count("storeproduct")
        )

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetCountWrapper(queryset=count_queryset),
                    annotated_queryset,
                ]
            ).execute()

        self.assertEqual(len(results), 2)
        product_count = results[0]
        categories = results[1]

        self.assertEqual(product_count, count_queryset.count())

        regular_categories = list(annotated_queryset)
        self.assertEqual(len(categories), len(regular_categories))

        for category, regular_category in zip(categories, regular_categories):
            self.assertEqual(category.product_count, regular_category.product_count)

    def test_complex_annotate_with_aggregation(self):
        """Test complex annotation with aggregation"""
        queryset = StoreProduct.objects.values("store").annotate(
            total_price=Sum("selling_price"), product_count=Count("id")
        )

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(querysets=[queryset]).execute()

        self.assertEqual(len(results), 1)
        aggregated_data = results[0]
        regular_aggregated_data = list(queryset)

        self.assertEqual(len(aggregated_data), len(regular_aggregated_data))
        for item, regular_item in zip(aggregated_data, regular_aggregated_data):
            self.assertAlmostEqual(
                float(item["total_price"]), float(regular_item["total_price"]), places=2
            )
            self.assertEqual(item["product_count"], regular_item["product_count"])

        self.assertEqual(len(aggregated_data), 1)  # Only one store
        self.assertEqual(aggregated_data[0]["product_count"], 4)  # Four products
        expected_total = (
            Decimal("50.22") + Decimal("100.33") + Decimal("75.50") + Decimal("120.75")
        )
        self.assertAlmostEqual(
            float(aggregated_data[0]["total_price"]), float(expected_total), places=2
        )
