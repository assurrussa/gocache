.DEFAULT_GOAL := check
UNIT_COVERAGE_MIN := 90
GO ?= go
.PHONY: check build tidy-check fmt fmt-check lint vet test test-race cover-html cover bench bench-all

check: tidy-check fmt-check build vet lint test test-race cover-html cover

build:
	$(GO) build ./...

tidy-check:
	$(GO) mod tidy -diff

fmt:
	$(GO) fmt ./...

fmt-check:
	@files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then \
		echo "gofmt required for:"; \
		echo "$$files"; \
		exit 1; \
	fi

lint:
	golangci-lint run -v --timeout=5m ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race -count=5 ./...

cover-html:
	@$(GO) test -coverprofile=./coverage.text -covermode=atomic $(shell $(GO) list ./...)
	@$(GO) tool cover -html=./coverage.text -o ./coverage.html && rm ./coverage.text

cover:
	@$(GO) test -coverpkg=./... -coverprofile=./cover_profile.out.tmp ./...
	@grep -v -e "mock" -e "\.pb\.go" -e "\.pb\.validate\.go" ./cover_profile.out.tmp > ./cover_profile.out && rm ./cover_profile.out.tmp
	@CUR_COVERAGE=$$($(GO) tool cover -func=cover_profile.out | tail -n 1 | awk '{ print $$3 }' | sed -e 's/^\([0-9]*\).*$$/\1/g') && \
		rm ./cover_profile.out && \
		echo "Current coverage: $$CUR_COVERAGE%" && \
		if [ "$$CUR_COVERAGE" -lt $(UNIT_COVERAGE_MIN) ]; then \
			echo "Coverage is not enough: $$CUR_COVERAGE% < $(UNIT_COVERAGE_MIN)%"; \
			exit 1; \
		else \
			echo "Coverage is enough: $$CUR_COVERAGE% >= $(UNIT_COVERAGE_MIN)%"; \
		fi

bench:
	$(GO) test -run '^$$' -bench=. -benchmem ./...

bench-all:
	$(GO) test -run '^$$' -bench=. -benchmem -cpu=12 -count=5 ./...
