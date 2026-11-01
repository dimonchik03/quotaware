.PHONY: build test race lint fmt e2e

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

e2e:
	docker compose up -d --build --wait
	go test -count=1 -tags e2e ./e2e/...; status=$$?; docker compose down -v; exit $$status
