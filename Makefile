GO ?= go

# Milestone test suites are gated by BALLAST_MILESTONE >= n (SPEC §17.3).
# Example: make sim BALLAST_MILESTONE=2
BALLAST_MILESTONE ?= 0
export BALLAST_MILESTONE

.PHONY: test test-race sim lint

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

# Simulation harness and milestone sim tests (SPEC §17.2).
# Reproduce one failure with: go test ./sim -run <Test> -seed <seed> -v
sim:
	@if [ -z "$$($(GO) list ./sim/... 2>/dev/null)" ]; then \
		echo "sim: no Go packages under ./sim yet"; \
	else \
		$(GO) test -count=1 ./sim/...; \
	fi

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi
	$(GO) vet ./...
