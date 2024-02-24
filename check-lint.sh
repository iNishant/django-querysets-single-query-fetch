#!/bin/bash

set -e
set -o pipefail

ruff check .
ruff format . --check

echo 'Check lint passed!'
