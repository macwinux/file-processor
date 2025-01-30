run: 
	cargo run
build:
	cargo test
	cargo build
clippy:
	cargo clippy --all --all-features --tests -- -D warnings
