# Makefile for PRICE Quantitative Trading Engine

.PHONY: build test run-worker run-server run-python-broker clean

build:
	cargo build

test:
	cargo test

run-worker:
	cargo run --bin price-worker

run-server:
	cargo run --bin price-server

run-python-broker:
	python -m uvicorn python-broker.app:app --host 127.0.0.1 --port 8001 --reload

clean:
	cargo clean
