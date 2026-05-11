.PHONY: run test test-race bench lint build clean

run:
	go run ./cmd/server

build:
	go build -o bin/server ./cmd/server

test:
	go test ./...

test-race:
	go test ./... -race -count=3

bench:
	go test ./... -bench=. -benchmem

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ data/

data:
	mkdir -p data
