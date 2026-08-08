user ?= smolquery-dev
api_port ?= 4000
hot_port ?= 4001
web_port ?= 4002

help:
	@make -qpRr | egrep -e '^[a-z].*:$$' | sed -e 's~:~~g' | sort

.PHONY: setup
setup:
	mix deps.get
	mix assets.setup

.PHONY: dev
dev:
	@echo "Cleaning stale smolquery dev processes on :$(api_port), :$(hot_port) and :$(web_port) (if any)..."
	@for port in $(api_port) $(hot_port) $(web_port); do \
		for pid in $$(lsof -tiTCP:$$port -sTCP:LISTEN 2>/dev/null); do \
			if ps -p $$pid -o command= | grep -q smolquery; then \
				kill -15 $$pid 2>/dev/null || true; \
			else \
				echo "port $$port is held by a non-smolquery process (pid $$pid) — leaving it alone"; \
			fi; \
		done; \
	done
	@epmd -daemon 2>/dev/null || true
	MIX_ENV=dev \
	ERL_AFLAGS="-kernel shell_history enabled" \
	iex --name smolquery@127.0.0.1 --cookie smolquery -S mix

.PHONY: dev_stop
dev_stop:
	@echo "Stopping smolquery dev processes on :$(api_port), :$(hot_port) and :$(web_port)..."
	@for port in $(api_port) $(hot_port) $(web_port); do \
		for pid in $$(lsof -tiTCP:$$port -sTCP:LISTEN 2>/dev/null); do \
			if ps -p $$pid -o command= | grep -q smolquery; then \
				kill -15 $$pid 2>/dev/null || true; \
			else \
				echo "port $$port is held by a non-smolquery process (pid $$pid) — leaving it alone"; \
			fi; \
		done; \
	done

.PHONY: services
services:
	@epmd -daemon 2>/dev/null || true
	@nc -z localhost 5432 2>/dev/null && echo "postgres: up" || \
		echo "postgres: DOWN — start your local Postgres on :5432 (postgres/postgres)"
	@nc -z localhost 9000 2>/dev/null && echo "minio: up" || \
		docker run -d --rm --name smolquery-minio -p 9000:9000 \
			-e MINIO_ROOT_USER=smolquery -e MINIO_ROOT_PASSWORD=smolquery-secret \
			minio/minio server /data

.PHONY: services_stop
services_stop:
	@docker stop smolquery-minio 2>/dev/null || true

.PHONY: test
test:
	mix test

.PHONY: test_integration
test_integration: services
	mix test --include integration

.PHONY: test_cluster
test_cluster:
	mix test --only cluster

.PHONY: kind_up
kind_up:
	./scripts/kind-up.sh

.PHONY: kind_down
kind_down:
	kind delete cluster --name smolquery

.PHONY: precommit
precommit:
	mix precommit

.PHONY: ci
ci:
	mix ci

.PHONY: ch_install
ch_install: ## install repo-local ClickHouse binary under .cache/clickhouse/
	./scripts/clickhouse/install.sh

.PHONY: ch_up
ch_up: ## start repo-local ClickHouse for the comparison bench
	./scripts/clickhouse/up.sh

.PHONY: ch_reset
ch_reset: ## reset ClickHouse data and restart (wipes .cache/clickhouse-data)
	./scripts/clickhouse/up.sh --reset

.PHONY: ch_down
ch_down: ## stop repo-local ClickHouse
	./scripts/clickhouse/down.sh

.PHONY: bench_compare
bench_compare: ## smolquery vs ClickHouse comparison bench (run ch_up first)
	@if [ ! -f scripts/clickhouse/install.sh ] || [ ! -f scripts/clickhouse/up.sh ]; then \
		echo "scripts/clickhouse missing — sync the bench/clickhouse-compare branch" >&2; exit 1; \
	fi
	@clickhouse_bin=; for f in .cache/clickhouse/*/clickhouse; do [ -e "$$f" ] && clickhouse_bin="$$f" && break; done; \
	if [ -z "$$clickhouse_bin" ] || [ ! -x "$$clickhouse_bin" ]; then \
		echo "clickhouse not installed — run make ch_install first" >&2; exit 1; \
	fi
	@if [ ! -f .cache/clickhouse-data/clickhouse.pid ] \
		|| ! kill -0 $$(cat .cache/clickhouse-data/clickhouse.pid 2>/dev/null) 2>/dev/null; then \
		echo "clickhouse not running — run make ch_up first" >&2; exit 1; \
	fi
	mix run bench/compare.exs

.PHONY: bench_compare_smolquery
bench_compare_smolquery: ## comparison bench, smolquery arm only (no ClickHouse needed)
	ARMS=smolquery mix run bench/compare.exs

clean:
	rm -rf _build && rm -rf deps
