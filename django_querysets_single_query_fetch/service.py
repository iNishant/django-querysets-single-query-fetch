import datetime
import json
import logging
import operator
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.core.exceptions import EmptyResultSet
from django.db import connections
from django.db.models import DecimalField, QuerySet, UUIDField
from django.db.models.query import (
    FlatValuesListIterable,
    ModelIterable,
    QuerySet,
    ValuesIterable,
    ValuesListIterable,
    get_related_populators,
)

logger = logging.getLogger(__name__)


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

    def __init__(self, querysets: list[QuerySet]) -> None:
        self.querysets = querysets

    def _get_django_sql_for_queryset(self, queryset: QuerySet) -> str:
        """
        gets the sql that django would normally execute for the queryset, return empty string
        if queryset will always return empty irrespective of params ()
        """

        # handle param quoting for IN queries (TODO: find some psycopg2 way to do this)
        # this is a bit hacky, but it works for now

        quoted_params = ()
        db = queryset.db
        compiler = queryset.query.get_compiler(using=db)
        try:
            sql, params = compiler.as_sql(with_col_aliases=True)  # add col aliases other wise json
            # build object cant handle same column name from two tables => two duplicate keys in dict
            # (one primary, one joined for example)
        except EmptyResultSet:
            return ""

        for param in params:
            if isinstance(param, str) or isinstance(param, UUID) or isinstance(param, datetime.datetime):
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

        for field in obj.__class__._meta.fields:
            if issubclass(DecimalField, field.__class__):
                float_value = getattr(obj, field.attname)
                if float_value is not None:
                    setattr(obj, field.attname, Decimal(str(getattr(obj, field.attname))))
            elif issubclass(UUIDField, field.__class__):
                uuid_value = getattr(obj, field.attname)
                if uuid_value is not None:
                    setattr(obj, field.attname, UUID(uuid_value))

        return obj

    def _get_instances_from_results_for_model_iterable(self, queryset: QuerySet, results: list):
        """
        slightly modified copy paste from source of ModelIterable
        """
        instances = []

        # convert results coming from json_build_object to list of tuples, convert back json values to raw strings

        new_results = []

        for row_dict in results:
            for key, value in row_dict.items():
                if isinstance(value, dict):
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
        init_list = [f[0].target.attname for f in select[model_fields_start:model_fields_end]]
        related_populators = get_related_populators(klass_info, select, db)
        known_related_objects = [
            (
                field,
                related_objs,
                operator.attrgetter(
                    *[
                        field.attname if from_field == "self" else queryset.model._meta.get_field(from_field).attname
                        for from_field in field.from_fields
                    ]
                ),
            )
            for field, related_objs in queryset._known_related_objects.items()
        ]

        for row in compiler.results_iter(results):
            obj = model_cls.from_db(db, init_list, row[model_fields_start:model_fields_end])

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

            instances.append(obj)

        return instances

    def _convert_raw_results_to_final_queryset_results(self, queryset: QuerySet, queryset_raw_results: list):
        queryset_results = []
        if issubclass(queryset._iterable_class, ModelIterable):
            queryset_results = self._get_instances_from_results_for_model_iterable(
                queryset=queryset, results=queryset_raw_results
            )
        elif issubclass(queryset._iterable_class, ValuesIterable):
            queryset_results = queryset_raw_results
        elif issubclass(queryset._iterable_class, FlatValuesListIterable):
            queryset_results = [list(row_dict.values())[0] for row_dict in queryset_raw_results]
        elif issubclass(queryset._iterable_class, ValuesListIterable):
            queryset_results = [list(row_dict.values()) for row_dict in queryset_raw_results]
        else:
            raise ValueError(f"Unsupported queryset iterable class: {queryset._iterable_class}")
        return queryset_results

    def execute(self) -> list[list[Any]]:

        django_sqls_for_querysets = [
            self._get_django_sql_for_queryset(queryset=queryset) for queryset in self.querysets
        ]

        final_result_list = []

        for queryset_sql in django_sqls_for_querysets:
            if not queryset_sql:
                final_result_list.append([])
            else:
                final_result_list.append(None)  # will be replaced by actual result below

        non_empty_django_sqls_for_querysets = [sql for sql in django_sqls_for_querysets if sql]

        raw_sql = f"""
            SELECT
                json_build_object(
                    {', '.join([f"'{i}', {sql}" for i, sql in enumerate(non_empty_django_sqls_for_querysets)])}
            )
        """

        with connections["default"].cursor() as cursor:
            cursor.execute(raw_sql, params={})
            raw_sql_result_dict: dict = cursor.fetchone()[0]

        final_result = []
        index = 0

        for queryset, result in zip(self.querysets, final_result_list):
            if result is not None:
                # empty case EmptyResultSet
                final_result.append(result)
                continue
            final_result.append(
                self._convert_raw_results_to_final_queryset_results(
                    queryset=queryset, queryset_raw_results=raw_sql_result_dict[str(index)]
                )
            )
            index += 1

        return final_result
