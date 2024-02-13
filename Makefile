all:
	@exit 0

.PHONY: check
check:
	cargo clippy --all-features --all

.PHONY: test
test:
	cargo test --all-features --all
