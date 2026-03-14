PYTHON ?= python3
RAW_ROOT ?= /Volumes/Data/港股Tick数据
MANIFEST_ROOT ?= /Volumes/Data/港股Tick数据/manifests
LOG_ROOT ?= /Volumes/Data/港股Tick数据/logs

.PHONY: help raw-inventory-2025 raw-inventory-2026 raw-inventory-smoke stage-2025 stage-2026 stage-sample-2025 stage-sample-2026 dqa-coverage-plan dqa-schema-plan dqa-linkage-plan verified-plan test lint

help:
	@printf "Available targets:\n"
	@printf "  raw-inventory-2025  Build the 2025 raw inventory manifest\n"
	@printf "  raw-inventory-2026  Build the 2026 raw inventory manifest\n"
	@printf "  raw-inventory-smoke Run a 50-file smoke test against 2025 raw data\n"
	@printf "  stage-2025          Build 2025 stage parquet outputs\n"
	@printf "  stage-2026          Build 2026 stage parquet outputs\n"
	@printf "  stage-sample-2025   Build a 3-day 2025 stage sample\n"
	@printf "  stage-sample-2026   Build a latest-3-day 2026 stage sample\n"
	@printf "  dqa-coverage-plan   Print the DQA coverage scaffold plan\n"
	@printf "  dqa-schema-plan     Print the DQA schema scaffold plan\n"
	@printf "  dqa-linkage-plan    Print the DQA linkage scaffold plan\n"
	@printf "  verified-plan       Print the verified-layer scaffold plan\n"
	@printf "  test                Run pytest\n"
	@printf "  lint                Run ruff check\n"

raw-inventory-2025:
	$(PYTHON) -m Scripts.build_raw_inventory --year 2025 --raw-root $(RAW_ROOT) --output-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT)

raw-inventory-2026:
	$(PYTHON) -m Scripts.build_raw_inventory --year 2026 --raw-root $(RAW_ROOT) --output-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT)

raw-inventory-smoke:
	$(PYTHON) -m Scripts.build_raw_inventory --year 2025 --raw-root $(RAW_ROOT) --output-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT) --max-files 50

stage-2025:
	$(PYTHON) -m Scripts.build_stage_parquet --year 2025 --raw-root $(RAW_ROOT) --output-root $(RAW_ROOT)/candidate_cleaned --manifest-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT)

stage-2026:
	$(PYTHON) -m Scripts.build_stage_parquet --year 2026 --raw-root $(RAW_ROOT) --output-root $(RAW_ROOT)/candidate_cleaned --manifest-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT)

stage-sample-2025:
	$(PYTHON) -m Scripts.build_stage_parquet --year 2025 --raw-root $(RAW_ROOT) --output-root $(RAW_ROOT)/candidate_cleaned --manifest-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT) --max-days 3

stage-sample-2026:
	$(PYTHON) -m Scripts.build_stage_parquet --year 2026 --raw-root $(RAW_ROOT) --output-root $(RAW_ROOT)/candidate_cleaned --manifest-root $(MANIFEST_ROOT) --log-root $(LOG_ROOT) --max-days 3 --latest-days

dqa-coverage-plan:
	$(PYTHON) -m Scripts.run_dqa_coverage --print-plan

dqa-schema-plan:
	$(PYTHON) -m Scripts.run_dqa_schema --print-plan

dqa-linkage-plan:
	$(PYTHON) -m Scripts.run_dqa_linkage --print-plan

verified-plan:
	$(PYTHON) -m Scripts.build_verified_layer --print-plan

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
