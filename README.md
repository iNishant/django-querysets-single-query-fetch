## Django Querysets Single Query Fetch

Executes multiple querysets over a single db query and returns results which would have been returned in normal evaluation of querysets. This can help in critical paths to avoid network/connection-pooler latency if db cpu/mem are nowhere near exhaustion. Ideal use case is fetching multiple small and optimised independent querysets where above mentioned latencies can dominate total execution time.

Supports only Postgres as of now

## Installation

```bash
pip install django-querysets-single-query-fetch
```

## Usage

```py

from django_querysets_single_query_fetch.service import QuerysetsSingleQueryFetch

querysets = [queryset1, queryset2, ...]
results = QuerysetsSingleQueryFetch(querysets=querysets).execute()

assert results == [list(queryset) for queryset in querysets]

```

## Contribution suggestions

- Add tests
- Add support for other databases 👀
- Make parsing logic as close to actual querysets and with minimal diff (utilising as much django internal code/utils as possible, maybe submit proposals to django if you find better ways to organise code, for eg [BaseIterable](https://github.com/django/django/blob/main/django/db/models/query.py#L46) could probably have an abstract method called `convert_sql_row_to_transformed_result_row`?)
- Make Github action workflows for test/lint
- Add pip publishing workflow? At least setup.py, install_requires, testapp/testproject excluded from final tar etc..
- Find a better package name? 😂 (think SEO)
- Add a diagram in README depicting the time saved during network trips
- Anything else which makes this better, open to ideas

  
## Notes

> [!NOTE]
> Note parallelisation by postgres is not guaranteed, as it depends on lot of config params (max_parallel_workers_per_gather, min_parallel_table_scan_size, max_parallel_workers etc). Even without parallelisation, this can be faster than normal one-by-one evaluation of querysets due to reduced no of network trips.
