.PHONY: build test race lint fmt e2e demo demo-down bench bench-smoke bench-changing

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
	docker compose up -d --build --wait || { docker compose logs; docker compose down -v; exit 1; }
	QUOTAWARE_E2E_OWNS_COMPOSE=1 go test -count=1 -tags e2e ./e2e/...; status=$$?; [ $$status -eq 0 ] || docker compose logs; docker compose down -v; exit $$status

demo:
	bench/kind/demo.sh

demo-down:
	bench/kind/down.sh

bench:
	bench/bench.sh

bench-smoke:
	BENCH_PHASES=half:30s:0.5,full:30s:1,burst:45s:3,idle:60s:0 BENCH_RESULTS=bench/results/smoke bench/bench.sh

bench-changing:
	BENCH_PROFILE=changing bench/bench.sh
