from datetime import date
from decimal import Decimal
from unittest import mock, skipUnless

from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError, connection
from django.db.models import Count, F, Model
from django.test import TransactionTestCase
from model_bakery import baker

from django_querysets_single_query_fetch.service import (
    QuerysetCountWrapper,
    QuerysetGetOrNoneWrapper,
    QuerysetsSingleQueryFetch,
)
from testapp.models import OnlineStore, StoreProduct, StoreProductCategory


def _typed(value):
    """
    value with the types of everything in it (and loaded fields of model instances) for comparison
    """
    if isinstance(value, Model):
        return (
            type(value),
            {k: _typed(v) for k, v in value.__dict__.items() if k != "_state"},
            {k: _typed(v) for k, v in value._state.fields_cache.items()},
        )
    if isinstance(value, dict):
        return (type(value), {k: _typed(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return (type(value), [_typed(v) for v in value])
    return (type(value), value)


@skipUnless(connection.vendor == "mysql", "mysql only")
class MySQLMultiStatementTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.store = baker.make(OnlineStore, expired_on=date(2030, 1, 2))
        self.category = baker.make(StoreProductCategory, store=self.store)
        for i in range(5):
            self.product = baker.make(
                StoreProduct,
                store=self.store,
                category=self.category if i % 2 else None,
                selling_price=Decimal(f"{i}.25"),
                meta={"i": i} if i % 2 else [i],
            )

    def test_results_are_same_as_normal_evaluation_in_a_single_query(self):
        querysets = [
            StoreProduct.objects.select_related("store", "category").order_by("-id")[
                :3
            ],
            StoreProduct.objects.filter(
                selling_price__gt=Decimal("1.5"),
                store__expired_on__gte=date(2030, 1, 1),
                name__startswith=self.product.name[:1],
            ).order_by("selling_price"),
            StoreProduct.objects.annotate(price=F("selling_price"))
            .values("price", "name", "meta", "store__name")
            .order_by("id"),
            StoreProduct.objects.annotate(price=F("selling_price"))
            .values_list("price", "name")
            .order_by("id"),
            StoreProduct.objects.values_list("id", flat=True).order_by("-id"),
            StoreProduct.objects.values_list("id", "name", named=True).order_by("id"),
            StoreProduct.objects.values("category_id")
            .annotate(products=Count("id"))
            .order_by("category_id"),
        ]
        expected = [list(queryset) for queryset in querysets] + [
            StoreProduct.objects.filter(category__isnull=False).count(),
            None,
            StoreProduct.objects.order_by("id").first(),
            [],
        ]

        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=querysets
                + [
                    QuerysetCountWrapper(
                        StoreProduct.objects.filter(category__isnull=False)
                    ),
                    QuerysetGetOrNoneWrapper(StoreProduct.objects.filter(name="-")),
                    QuerysetGetOrNoneWrapper(StoreProduct.objects.order_by("id")),
                    StoreProduct.objects.none(),
                ]
            ).execute()

        self.assertEqual(_typed(results), _typed(expected))
        self.assertIsInstance(results[1][0].selling_price, Decimal)
        self.assertEqual(results[5][0].name, expected[5][0].name)  # named tuple

    def test_params_cannot_inject_extra_statements(self):
        name = "x'; DELETE FROM testapp_storeproduct; SELECT '"
        with self.assertNumQueries(1):
            results = QuerysetsSingleQueryFetch(
                querysets=[
                    StoreProduct.objects.filter(name=name),
                    OnlineStore.objects.filter(name__contains=";"),
                ]
            ).execute()

        self.assertEqual(results, [[], []])
        self.assertEqual(StoreProduct.objects.count(), 5)

    def test_disabled_multi_statements_raises_improperly_configured(self):
        with mock.patch.dict(
            connection.settings_dict["OPTIONS"], {"multi_statements": False}
        ):
            with self.assertNumQueries(0):
                with self.assertRaises(ImproperlyConfigured):
                    QuerysetsSingleQueryFetch(
                        querysets=[OnlineStore.objects.all()]
                    ).execute()

    def test_error_in_a_later_statement_is_raised_and_connection_is_usable(self):
        with self.assertRaises(DatabaseError):
            QuerysetsSingleQueryFetch(
                querysets=[
                    OnlineStore.objects.all(),
                    OnlineStore.objects.extra(where=["no_such_column = 1"]),
                ]
            ).execute()

        self.assertEqual(list(OnlineStore.objects.all()), [self.store])
