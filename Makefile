.PHONY: build test race lint fmt

build:
	go build ./...

test:
	go test -p 2 ./...

race:
	go test -race -p 2 ./...

lint:
	golangci-lint run

fmt:
	gofmt -l -w .
