.PHONY: build test run docker

build:
	go build ./...

test:
	go test ./... -race -count=1

run:
	go run ./cmd/broker

docker:
	docker build -t go-kafkalike:dev .
