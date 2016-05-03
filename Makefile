PROG_NAME := sqltojson
GIT_VERSION := $(shell git log -1 --pretty=format:"%h (%ci)" .)

build:
	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o "$(GOPATH)/bin/$(PROG_NAME)" \
		./cmd/sqltojson

dist:
	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o "dist/darwin-amd64/$(PROG_NAME)" \
		./cmd/sqltojson

.PHONY: dist
