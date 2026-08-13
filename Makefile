# A fresh clone on any machine should need nothing beyond these.
# The data is not in git, so `make data` is what reproduces it.

PY := python3

.PHONY: help setup data data-full clean

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

setup:  ## install pinned dependencies into the active venv
	$(PY) -m pip install -r requirements.txt

data:  ## fetch the default three months (~150MB)
	bash scripts/fetch_data.sh

data-full:  ## fetch all twelve months of 2024 (~700MB)
	bash scripts/fetch_data.sh --year 2024

clean:  ## remove Spark scratch output, keep the downloaded data
	rm -rf warehouse metastore_db derby.log logs/spark-events/*
