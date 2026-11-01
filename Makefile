.PHONY: build test race lint fmt e2e demo demo-down

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
	docker compose up -d --build --wait || { docker compose down -v; exit 1; }
	go test -count=1 -tags e2e ./e2e/...; status=$$?; docker compose down -v; exit $$status

demo:
	bench/kind/demo.sh

demo-down:
	bench/kind/down.sh
