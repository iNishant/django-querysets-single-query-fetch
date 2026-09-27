## Django Querysets Single Query Fetch

Executes multiple querysets in a single database round trip and returns results which would have been returned in normal evaluation of querysets. This can help in critical paths to avoid network latency. Ideal use case is fetching multiple small and optimised independent querysets where above mentioned latencies can dominate total execution time.

Supports PostgreSQL and MySQL

- **PostgreSQL**: querysets are combined into a single SQL query (using `json_build_object` and `json_agg`) and results are parsed back from JSON
- **MySQL**: querysets are sent as a single multi statement query and each queryset's result set is parsed by Django as usual. This needs multi statements enabled on the connection, which is the default for `mysqlclient` (don't set `"multi_statements": False` in the database `OPTIONS`)

> [!NOTE]
> (PostgreSQL) The performance gains from this utility were pretty significant for our use cases so in some places we have added quick hacks (see `_transform_object_to_handle_json_agg`) to get around some parsing/conversion issues where raw values are not parsed properly into their python types (for eg. datetime, UUID, Decimal). This is usually done in [from_db_value](https://docs.djangoproject.com/en/5.0/ref/models/fields/#django.db.models.Field.from_db_value) for custom fields and by database-specific backends for django supported fields (psycopg for postgres). If you encounter a similar issue, just send a patch with a quick hack. For a complete solution, we would have to dive deeper into psycopg/postgres and its handling of `json_agg` output.

## Installation

```bash
uv pip install django-querysets-single-query-fetch
```

## Usage

```py

from django_querysets_single_query_fetch.service import QuerysetsSingleQueryFetch

querysets = [queryset1, queryset2, ...]
results = QuerysetsSingleQueryFetch(querysets=querysets).execute()

assert results == [list(queryset) for queryset in querysets]
```

Following tests pass (assuming no `prefetch_related` in querysets)

```py

# without (no. of queries is equal to no. of querysets)
with self.assertNumQueries(len(querysets)):
    results = [list(queryset) for queryset in querysets]

# with (irrespective of no. of querysets, only one network call is made)
with self.assertNumQueries(1):
    results = QuerysetsSingleQueryFetch(querysets=querysets).execute()

```

Fetching count of queryset using `QuerysetCountWrapper` (since `queryset.count()` is not a lazy method)

```py
from django_querysets_single_query_fetch.service import QuerysetsSingleQueryFetch, QuerysetCountWrapper

querysets = [QuerysetCountWrapper(queryset=queryset1), queryset2, ...]
results =  QuerysetsSingleQueryFetch(querysets=querysets)

assert results == [queryset1.count(), list(queryset2), ...]
```

## Contribution suggestions

- Add tests (django version matrix, different types and parsing etc)
- Add support for other databases 👀
- Add support for all aggregations using a similar approach as used in `QuerysetCountWrapper`
- Make parsing logic as close to actual querysets and with minimal diff (utilising as much django internal code/utils as possible, maybe submit proposals to django if you find better ways to organise code, for eg [BaseIterable](https://github.com/django/django/blob/main/django/db/models/query.py#L46) could probably have an abstract method called `convert_sql_row_to_transformed_result_row`?)
- Find a better package name? 😂 (think SEO)
- Add a diagram in README depicting the time saved during network trips
- Anything else which makes this better, open to ideas
- Better readable way of accessing results (instead of `results[0]`, `results[1]`)
- `QuerysetFirstWrapper`, `QuerysetLastWrapper` etc for lazy evaluating `.first()` and `.last()`
- "How it works" section/diagram?

## Notes

- The database still executes the querysets one after another, not in parallel. The time saved is the network round trips between querysets, which is why this helps most with many small and fast querysets. Postgres may still use its usual parallel query within a single queryset (for eg. a parallel scan of a large table), same as it would without this utility.
