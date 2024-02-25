export RUST_LOG ?= warn,fila=trace
export NOCAPTURE ?= 0
FILTER ?=

PG_USER ?= $(shell printenv USER)
PG_DATABASE ?= fila_dev
export DATABASE_URL = postgresql://$(PG_USER)@localhost/$(PG_DATABASE)

POSTGRES_ROOT := .postgresql
POSTGRES_DATABASE := $(POSTGRES_ROOT)/data
POSTGRES_VERSION := $(POSTGRES_DATABASE)/PG_VERSION
POSTGRES_SERVER := $(POSTGRES_DATABASE)/postmaster.pid

PSQL := psql $(DATABASE_URL)
PG_CTL := pg_ctl -D $(POSTGRES_DATABASE)

CARGO_TEST_RUST_FLAGS :=

ifeq ($(NOCAPTURE),1)
	NOCAPTURE := 1
	CARGO_TEST_RUST_FLAGS += --nocapture
endif

.PHONY: all
all:
	@exit 0

.PHONY: clean
clean: db.stop
	rm -rf $(POSTGRES_ROOT) target

.PHONY: check
check:
	cargo clippy --all-features --all

.PHONY: test
test: $(POSTGRES_SERVER)
	cargo test --all-features --all -- $(CARGO_TEST_RUST_FLAGS) $(FILTER)

$(POSTGRES_VERSION):
	mkdir -p $(POSTGRES_DATABASE)
	TZ=UTC initdb $(POSTGRES_DATABASE)

$(POSTGRES_SERVER): $(POSTGRES_VERSION)
	$(PG_CTL) -l $(POSTGRES_ROOT)/postgresql.log start
	@createdb $(PG_DATABASE) 2>/dev/null || true

.PHONY: db.psql
db.psql: $(POSTGRES_SERVER)
	@$(PSQL)

.PHONY: db.start
db.start: $(POSTGRES_SERVER)

.PHONY: db.stop
db.stop:
	$(PG_CTL) stop || echo "already stopped"

.PHONY: db.destroy
db.destroy: db.stop
	rm -rf $(POSTGRES_DATABASE)

.PHONY: db.setup
db.setup: db.start

.PHONY: db.status
db.status:
	@echo "DATABASE_URL is [$(DATABASE_URL)]"
	@echo
	@$(PG_CTL) status
	@[ "$$($(PSQL) -XAqt -c 'SELECT 1;')" = "1" ] && echo "ok"
