GO ?= go

.PHONY: bench check fmt-check race test tidy-check vet

check: tidy-check fmt-check vet test race

tidy-check:
	$(GO) mod tidy -diff

fmt-check:
	@files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then \
		echo "gofmt required for:"; \
		echo "$$files"; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

test:
	$(GO) test -covermode=atomic -cover ./...

race:
	$(GO) test -race ./...

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./...
