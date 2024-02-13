export RUST_LOG := warn,fila=trace
export NOCAPTURE := 0

export CARGO_TEST_RUST_FLAGS :=

ifeq ($(NOCAPTURE),1)
	NOCAPTURE := 1
	CARGO_TEST_RUST_FLAGS += --nocapture
endif

all:
	@exit 0

.PHONY: check
check:
	cargo clippy --all-features --all

.PHONY: test
test:
	cargo test --all-features --all -- $(CARGO_TEST_RUST_FLAGS)
