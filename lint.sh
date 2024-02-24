#!/bin/bash

set -e
set -o pipefail

ruff check . --fix
ruff format .

echo 'Lint successful!'
