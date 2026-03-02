.PHONY: build test run stop e2e demo clean logs

HOST_PORT ?= 8080

test:
	docker build -t consistent-hashing-test -f Dockerfile.test .
	docker run --rm consistent-hashing-test
