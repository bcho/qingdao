.PHONY: get-deps

get-deps:
	go get gopkg.in/redis.v4
	go get golang.org/x/net/context
	go install ./...
