SHELL:=/bin/bash


help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

build: ## Собрать «k6» с модулем локально
	@go install go.k6.io/xk6/cmd/xk6@latest
	@xk6 build --with $(shell go list -m)=.

test: ## Запуск тестов
	@xk6 build --with $(shell go list -m)=.
	@./k6 run test/test.js

.PHONY: build clean format help test
.DEFAULT_GOAL := help
