#!/bin/bash

set -e
set -o pipefail

ruff check .
ruff format . --check
mypy --ignore-missing-imports django_querysets_single_query_fetch/service.py
echo 'Check lint passed!'
