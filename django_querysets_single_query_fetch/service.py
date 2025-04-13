import datetime
import json
import logging
import operator
from decimal import Decimal
from typing import Any, List, Tuple, Union
from uuid import UUID

from django.core.exceptions import EmptyResultSet
from django.db import connections
from django.db.models import (
    DecimalField,
    QuerySet,
    UUIDField,
    Count,
    DateTimeField,
    DateField,
)
from django.db.models.query import (
    FlatValuesListIterable,
    ModelIterable,
    ValuesIterable,
    ValuesListIterable,
    get_related_populators,
)
from django.utils.dateparse import parse_datetime

logger = logging.getLogger(__name__)


class QuerysetCountWrapper:
    """
    Wrapper around queryset to indicate that we want to fetch count of the queryset
    """

    def __init__(self, queryset: QuerySet) -> None:
        self.queryset = queryset


class QuerysetGetOrNoneWrapper:
    """
    Wrapper around queryset to indicate that we want to fetch .get() or None
    NOTE: this uses LIMIT 1 query so does not raise MultipleObjectsReturned
    only returns the actual row or None (in case of no match)
    """

    def __init__(self, queryset: QuerySet) -> None:
        self.queryset = queryset[:1]  # force limit 1


QuerysetWrapperType = Union[QuerySet, QuerysetCountWrapper, QuerysetGetOrNoneWrapper]

RESULT_PLACEHOLDER = object()


class QuerysetsSingleQueryFetch:
    """
    Executes multiple querysets in a single db query using json_build_object and returns results which
    would have been returned in normal evaluation of querysets. This can help in critical paths
    to avoid network/connection/connection-pooler latency if db cpu/mem are nowhere
    near exhaustion. Note parallelisation by postgres is not guaranteed, as it depends on lot of config params
    (max_parallel_workers_per_gather, min_parallel_table_scan_size, max_parallel_workers etc). Even without
    parallelisation, this can be faster than normal evaluation of querysets due to reduced no of network trips.

    Ideal use case is fetching multiple small/optimised independent querysets where above mentioned
    latencies can dominate total execution time.

    Usage:

    querysets = [queryset1, queryset2, ...]
    results = QuerysetsSingleQueryFetch(querysets=querysets).execute()
    assert results == [list(queryset) for queryset in querysets]
    """

    def __init__(self, querysets: list[QuerysetWrapperType]) -> None:
        self.querysets = querysets

    def _get_fetch_count_compiler_from_queryset(
        self, queryset: QuerySet, using: str
    ) -> Any:
        """
        slightly modified copy paste from get_count and get_aggregation in django.db.models.sql.compiler
        """
        obj = queryset.query.clone()
        obj.add_annotation(Count("*"), alias="__count")
        added_aggregate_names = ["__count"]
        existing_annotations = [
            annotation
            for alias, annotation in obj.annotations.items()
            if alias not in added_aggregate_names
        ]
        if (
            isinstance(obj.group_by, tuple)
            or obj.is_sliced
            or existing_annotations
            or obj.distinct
            or obj.combinator
        ):
            from django.db.models.sql.subqueries import AggregateQuery

            inner_query = obj.clone()
            inner_query.subquery = True
            outer_query = AggregateQuery(obj.model, inner_query)
            inner_query.select_for_update = False
            inner_query.select_related = False
            inner_query.set_annotation_mask(obj.annotation_select)
            # Queries with distinct_fields need ordering and when a limit is
            # applied we must take the slice from the ordered query. Otherwise
            # no need for ordering.
            inner_query.clear_ordering(force=False)
            if not inner_query.distinct:
                # If the inner query uses default select and it has some
                # aggregate annotations, then we must make sure the inner
                # query is grouped by the main model's primary key. However,
                # clearing the select clause can alter results if distinct is
                # used.
                has_existing_aggregate_annotations = any(
                    annotation
                    for annotation in existing_annotations
                    if getattr(annotation, "contains_aggregate", True)
                )
                if inner_query.default_cols and has_existing_aggregate_annotations:
                    inner_query.group_by = (
                        obj.model._meta.pk.get_col(inner_query.get_initial_alias()),
                    )
                inner_query.default_cols = False

            relabels = {t: "subquery" for t in inner_query.alias_map}
            relabels[None] = "subquery"
            # Remove any aggregates marked for reduction from the subquery
            # and move them to the outer AggregateQuery.
            col_cnt = 0
            for alias, expression in list(inner_query.annotation_select.items()):
                annotation_select_mask = inner_query.annotation_select_mask
                if expression.is_summary:
                    expression, col_cnt = inner_query.rewrite_cols(expression, col_cnt)
                    outer_query.annotations[alias] = expression.relabeled_clone(
                        relabels
                    )
                    del inner_query.annotations[alias]
                    annotation_select_mask.remove(alias)
                # Make sure the annotation_select wont use cached results.
                inner_query.set_annotation_mask(inner_query.annotation_select_mask)
            if (
                inner_query.select == ()
                and not inner_query.default_cols
                and not inner_query.annotation_select_mask
            ):
                # In case of Model.objects[0:3].count(), there would be no
                # field selected in the inner query, yet we must use a subquery.
                # So, make sure at least one field is selected.
                inner_query.select = (
                    obj.model._meta.pk.get_col(inner_query.get_initial_alias()),
                )
        else:
            outer_query = obj
            obj.select = ()
            obj.default_cols = False
            obj.extra = {}

        empty_set_result = [
            expression.empty_result_set_value
            for expression in outer_query.annotation_select.values()
        ]
        elide_empty = not any(result is NotImplemented for result in empty_set_result)
        outer_query.clear_ordering(force=True)
        outer_query.clear_limits()
        outer_query.select_for_update = False
        outer_query.select_related = False
        return outer_query.get_compiler(using, elide_empty=elide_empty)

    def _get_compiler_from_queryset(self, queryset: QuerysetWrapperType) -> Any:
        """
        if queryset is wrapped in QuerysetCountWrapper, then we need to call _get_fetch_count_compiler_from_queryset
        else we can call get_compiler directly from queryset's query
        """

        if isinstance(queryset, QuerysetCountWrapper):
            compiler = self._get_fetch_count_compiler_from_queryset(
                queryset.queryset, using=queryset.queryset.db
            )
        elif isinstance(queryset, QuerysetGetOrNoneWrapper):
            _queryset = queryset.queryset
            compiler = _queryset.query.get_compiler(using=_queryset.db)
        else:
            # queryset is the normal django queryset not wrapped by anything
            compiler = queryset.query.get_compiler(using=queryset.db)

        return compiler

    def _get_sanitized_sql_param(self, param: str) -> str:
        try:
            from psycopg import sql

            return sql.quote(param)
        except ImportError:
            try:
                from psycopg2.extensions import QuotedString

                return QuotedString(param).getquoted().decode("utf-8")
            except ImportError:
                raise ImportError("psycopg or psycopg2 not installed")

    def _get_django_sql_for_queryset(self, queryset: QuerysetWrapperType) -> str:
        """
        gets the sql that django would normally execute for the queryset, return empty string
        if queryset will always return empty irrespective of params ()
        """

        # handle param quoting for IN queries (TODO: find some psycopg2 way to do this)
        # this is a bit hacky, but it works for now

        quoted_params: Tuple[Any, ...] = ()
        compiler = self._get_compiler_from_queryset(queryset=queryset)
        try:
            sql, params = compiler.as_sql(
                with_col_aliases=True
            )  # add col aliases other wise json
            # build object cant handle same column name from two tables => two duplicate keys in dict
            # (one primary, one joined for example)
        except EmptyResultSet:
            return ""

        for param in params:
            if isinstance(param, str):
                # this is to handle special char handling
                param = self._get_sanitized_sql_param(param)
            elif isinstance(param, UUID) or isinstance(param, datetime.datetime):
                param = f"'{param}'"
            elif isinstance(param, int) or isinstance(param, float):
                # type which can be passed as is
                pass
            else:
                # keep strict
                raise ValueError(f"Unsupported param type: {type(param)}")
            quoted_params += (param,)

        django_sql = sql % quoted_params

        return f"(SELECT COALESCE(json_agg(item), '[]') FROM ({django_sql}) item)"

    def _transform_object_to_handle_json_agg(self, obj):
        """
        because of json_agg some default field level parsing/handling broke, patch it for now
        """

        if not obj:
            return obj

        for field in obj.__class__._meta.fields:
            if issubclass(DecimalField, field.__class__):
                float_value = getattr(obj, field.attname)
                if float_value is not None:
                    setattr(
                        obj, field.attname, Decimal(str(getattr(obj, field.attname)))
                    )
            elif issubclass(UUIDField, field.__class__):
                uuid_value = getattr(obj, field.attname)
                if uuid_value is not None and isinstance(uuid_value, str):
                    setattr(obj, field.attname, UUID(uuid_value))
            elif issubclass(DateField, field.__class__):
                # note datefield is subclass of datetimefield, so this check should come before
                date_value = getattr(obj, field.attname)
                if date_value is not None and isinstance(date_value, str):
                    setattr(obj, field.attname, parse_datetime(date_value).date())
            elif issubclass(DateTimeField, field.__class__):
                datetime_value = getattr(obj, field.attname)
                if datetime_value is not None and isinstance(datetime_value, str):
                    setattr(obj, field.attname, parse_datetime(datetime_value))
        return obj

    def _get_instances_from_results_for_model_iterable(
        self, queryset: QuerySet, results: list
    ):
        """
        slightly modified copy paste from source of ModelIterable
        """
        instances = []

        # convert results coming from json_build_object to list of tuples, convert back json values to raw strings

        new_results = []

        for row_dict in results:
            for key, value in row_dict.items():
                # both dict and list can be possible values for json field
                if isinstance(value, dict) or isinstance(value, list):
                    row_dict[key] = json.dumps(value)
                else:
                    pass
            new_results.append(tuple(row_dict.values()))

        results = [new_results]

        db = queryset.db
        compiler = queryset.query.get_compiler(using=db)
        compiler.as_sql()  # calling this will set some internal state of compiler, this is usually done when
        # executing the queryset normalling
        select, klass_info, annotation_col_map = compiler.get_select()
        model_cls = klass_info["model"]
        select_fields = klass_info["select_fields"]
        model_fields_start, model_fields_end = select_fields[0], select_fields[-1] + 1
        init_list = [
            f[0].target.attname for f in select[model_fields_start:model_fields_end]
        ]
        related_populators = get_related_populators(klass_info, select, db)
        known_related_objects = [
            (
                field,
                related_objs,
                operator.attrgetter(
                    *[
                        field.attname
                        if from_field == "self"
                        else queryset.model._meta.get_field(from_field).attname
                        for from_field in field.from_fields
                    ]
                ),
            )
            for field, related_objs in queryset._known_related_objects.items()
        ]

        for row in compiler.results_iter(results):
            obj = model_cls.from_db(
                db, init_list, row[model_fields_start:model_fields_end]
            )

            # because of json_agg some field level parsing/handling broke, patch it for now
            # TODO: point field handling
            for rel_populator in related_populators:
                rel_populator.populate(row, obj)
            if annotation_col_map:
                for attr_name, col_pos in annotation_col_map.items():
                    setattr(obj, attr_name, row[col_pos])

            obj = self._transform_object_to_handle_json_agg(obj=obj)
            # Add the known related objects to the model.
            for field, rel_objs, rel_getter in known_related_objects:
                # Avoid overwriting objects loaded by, e.g., select_related().
                if field.is_cached(obj):
                    continue
                rel_obj_id = rel_getter(obj)
                try:
                    rel_obj = rel_objs[rel_obj_id]
                except KeyError:
                    pass  # May happen in qs1 | qs2 scenarios.
                else:
                    setattr(obj, field.name, rel_obj)

            obj_fields_cache = {}
            # because of json_agg some field level parsing/handling broke, patch it for prefetched objects
            for prefetched_obj_name, prefetched_obj in obj._state.fields_cache.items():
                obj_fields_cache[prefetched_obj_name] = (
                    self._transform_object_to_handle_json_agg(obj=prefetched_obj)
                )
            obj._state.fields_cache = obj_fields_cache
            instances.append(obj)
        return instances

    def _convert_raw_results_to_final_queryset_results(
        self, queryset: QuerysetWrapperType, queryset_raw_results: list
    ):
        if isinstance(queryset, QuerysetCountWrapper):
            queryset_results = queryset_raw_results[0]["__count"]
        else:
            if isinstance(queryset, QuerysetGetOrNoneWrapper):
                django_queryset = queryset.queryset
            else:
                django_queryset = queryset

            if issubclass(django_queryset._iterable_class, ModelIterable):
                queryset_results = self._get_instances_from_results_for_model_iterable(
                    queryset=django_queryset, results=queryset_raw_results
                )
            elif issubclass(django_queryset._iterable_class, ValuesIterable):
                queryset_results = queryset_raw_results
            elif issubclass(django_queryset._iterable_class, FlatValuesListIterable):
                queryset_results = [
                    list(row_dict.values())[0] for row_dict in queryset_raw_results
                ]
            elif issubclass(django_queryset._iterable_class, ValuesListIterable):
                queryset_results = [
                    list(row_dict.values()) for row_dict in queryset_raw_results
                ]
            else:
                raise ValueError(
                    f"Unsupported queryset iterable class: {django_queryset._iterable_class}"
                )

        if isinstance(queryset, QuerysetGetOrNoneWrapper):
            # convert queryset_results to either row or none
            queryset_results = queryset_results[0] if queryset_results else None

        return queryset_results

    def _get_empty_queryset_value(self, queryset: QuerysetWrapperType) -> Any:
        empty_sql_val: Any

        if isinstance(queryset, QuerysetCountWrapper):
            empty_sql_val = 0
        elif isinstance(queryset, QuerysetGetOrNoneWrapper):
            empty_sql_val = None
        else:
            # normal queryset
            empty_sql_val = []

        return empty_sql_val

    def execute(self) -> list[list[Any]]:
        django_sqls_for_querysets = [
            self._get_django_sql_for_queryset(queryset=queryset)
            for queryset in self.querysets
        ]

        final_result_list: List[Any] = []

        for queryset_sql, queryset in zip(django_sqls_for_querysets, self.querysets):
            if not queryset_sql:
                final_result_list.append(
                    self._get_empty_queryset_value(queryset=queryset)
                )
            else:
                final_result_list.append(
                    RESULT_PLACEHOLDER
                )  # will be replaced by actual result below

        non_empty_django_sqls_for_querysets = [
            sql for sql in django_sqls_for_querysets if sql
        ]
        if non_empty_django_sqls_for_querysets:
            raw_sql = f"""
                SELECT
                    json_build_object(
                        {", ".join([f"'{i}', {sql}" for i, sql in enumerate(non_empty_django_sqls_for_querysets)])}
                )
            """
            with connections["default"].cursor() as cursor:
                cursor.execute(raw_sql, params={})
                raw_sql_result_dict: dict = cursor.fetchone()[0]
        else:
            # all querysets are always empty (EmptyResultSet)
            raw_sql_result_dict = {}

        final_result = []
        index = 0
        for queryset, result in zip(self.querysets, final_result_list):
            if result is not RESULT_PLACEHOLDER:
                # empty sql case
                final_result.append(result)
                continue
            final_result.append(
                self._convert_raw_results_to_final_queryset_results(
                    queryset=queryset,
                    queryset_raw_results=raw_sql_result_dict[str(index)],
                )
            )
            index += 1

        return final_result
