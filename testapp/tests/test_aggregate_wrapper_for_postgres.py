from decimal import Decimal

from django.db.models import Sum, Count, Avg, Max, Min
from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import (
    QuerysetsSingleQueryFetch,
    QuerysetAggregateWrapper,
)
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


class QuerysetAggregateWrapperPostgresTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.store = baker.make(OnlineStore)
        self.category1 = baker.make(StoreProductCategory, store=self.store)
        self.category2 = baker.make(StoreProductCategory, store=self.store)
        self.product_1 = baker.make(StoreProduct, store=self.store, selling_price=50.22)
        self.product_2 = baker.make(
            StoreProduct, store=self.store, category=self.category1, selling_price=100.33
        )
        self.product_3 = baker.make(
            StoreProduct, store=self.store, category=self.category1, selling_price=75.50
        )
        self.product_4 = baker.make(
            StoreProduct, store=self.store, category=self.category2, selling_price=120.75
        )

    def test_simple_aggregate(self):
        """Test simple aggregate with Sum"""
        queryset = StoreProduct.objects.filter()
        aggregate_queryset = queryset.aggregate(total_price=Sum("selling_price"))
        
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[QuerysetAggregateWrapper(queryset=queryset, **aggregate_queryset)]
            ).execute()
        
        self.assertEqual(len(results), 1)
        aggregate_result = results[0]
        
        self.assertEqual(len(aggregate_result), len(aggregate_queryset))
        self.assertIn('total_price', aggregate_result)
        self.assertAlmostEqual(
            float(aggregate_result['total_price']), 
            float(aggregate_queryset['total_price']), 
            places=2
        )

    def test_multiple_aggregates(self):
        """Test multiple aggregates in a single query"""
        queryset = StoreProduct.objects.filter()
        aggregate_queryset = queryset.aggregate(
            total_price=Sum("selling_price"),
            count=Count("id"),
            avg_price=Avg("selling_price"),
            max_price=Max("selling_price"),
            min_price=Min("selling_price"),
        )
        
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[QuerysetAggregateWrapper(queryset=queryset, **aggregate_queryset)]
            ).execute()
        
        self.assertEqual(len(results), 1)
        aggregate_result = results[0]
        
        self.assertEqual(len(aggregate_result), len(aggregate_queryset))
        
        for key in aggregate_queryset.keys():
            self.assertIn(key, aggregate_result)
            if isinstance(aggregate_queryset[key], Decimal) or isinstance(aggregate_result[key], (int, float, Decimal)):
                self.assertAlmostEqual(
                    float(aggregate_result[key]), 
                    float(aggregate_queryset[key]), 
                    places=2
                )
            else:
                self.assertEqual(aggregate_result[key], aggregate_queryset[key])
        
        self.assertEqual(aggregate_result['count'], 4)
        self.assertAlmostEqual(
            float(aggregate_result['total_price']), 
            float(Decimal('50.22') + Decimal('100.33') + Decimal('75.50') + Decimal('120.75')), 
            places=2
        )

    def test_filtered_aggregate(self):
        """Test aggregate with filter"""
        queryset = StoreProduct.objects.filter(category=self.category1)
        aggregate_queryset = queryset.aggregate(
            total_price=Sum("selling_price"),
            count=Count("id"),
        )
        
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[QuerysetAggregateWrapper(queryset=queryset, **aggregate_queryset)]
            ).execute()
        
        self.assertEqual(len(results), 1)
        aggregate_result = results[0]
        
        self.assertEqual(len(aggregate_result), len(aggregate_queryset))
        
        for key in aggregate_queryset.keys():
            self.assertIn(key, aggregate_result)
            if isinstance(aggregate_queryset[key], Decimal) or isinstance(aggregate_result[key], (int, float, Decimal)):
                self.assertAlmostEqual(
                    float(aggregate_result[key]), 
                    float(aggregate_queryset[key]), 
                    places=2
                )
            else:
                self.assertEqual(aggregate_result[key], aggregate_queryset[key])
        
        self.assertEqual(aggregate_result['count'], 2)  # Only products in category1
        self.assertAlmostEqual(
            float(aggregate_result['total_price']), 
            float(Decimal('100.33') + Decimal('75.50')), 
            places=2
        )

    def test_empty_aggregate(self):
        """Test aggregate on empty queryset"""
        queryset = StoreProduct.objects.filter(id=-1)  # No matches
        aggregate_queryset = queryset.aggregate(
            total_price=Sum("selling_price"),
            count=Count("id"),
        )
        
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[QuerysetAggregateWrapper(queryset=queryset, **aggregate_queryset)]
            ).execute()
        
        self.assertEqual(len(results), 1)
        aggregate_result = results[0]
        
        self.assertEqual(len(aggregate_result), len(aggregate_queryset))
        
        for key in aggregate_queryset.keys():
            self.assertIn(key, aggregate_result)
            self.assertEqual(aggregate_result[key], aggregate_queryset[key])
        
        self.assertEqual(aggregate_result['count'], 0)
        self.assertIsNone(aggregate_result['total_price'])

    def test_mix_with_other_querysets(self):
        """Test mixture of aggregate wrapper and other querysets"""
        aggregate_queryset = StoreProduct.objects.filter().aggregate(
            total_price=Sum("selling_price"),
            count=Count("id"),
        )
        regular_queryset = StoreProductCategory.objects.filter()
        
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    QuerysetAggregateWrapper(queryset=StoreProduct.objects.filter(), **aggregate_queryset),
                    regular_queryset
                ]
            ).execute()
        
        self.assertEqual(len(results), 2)
        aggregate_result = results[0]
        categories = results[1]
        
        self.assertEqual(len(aggregate_result), len(aggregate_queryset))
        
        for key in aggregate_queryset.keys():
            self.assertIn(key, aggregate_result)
            if isinstance(aggregate_queryset[key], Decimal) or isinstance(aggregate_result[key], (int, float, Decimal)):
                self.assertAlmostEqual(
                    float(aggregate_result[key]), 
                    float(aggregate_queryset[key]), 
                    places=2
                )
            else:
                self.assertEqual(aggregate_result[key], aggregate_queryset[key])
        
        regular_categories = list(regular_queryset)
        self.assertEqual(len(categories), len(regular_categories))
