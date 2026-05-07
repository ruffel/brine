# Justfile for brine

COMPOSE := "test/integration/scripts/compose.sh"
COMPOSE_FILE := "test/integration/compose.yaml"

# Default recipe
default: test tools-test lint

# Run all tests with race detection
test:
    go test -race ./...

# Run linters
lint:
    golangci-lint run ./...

# Run deterministic API examples
examples:
    go test ./examples/...

# Run tests for the separate optional tools module
tools-test:
    go -C tools test ./...

# Run the deterministic demo tool from the separate tools module
demo *args:
    go -C tools run ./cmd/brine-demo {{args}}

# Clean build artifacts
clean:
    go clean

# Run go fmt
fmt:
    go fmt ./...
    go -C tools fmt ./...

# Run go mod tidy
tidy:
    go mod tidy
    go -C tools mod tidy

# Start the Salt integration environment and wait until ready
integration-up:
    {{COMPOSE}} -f {{COMPOSE_FILE}} up -d --build --force-recreate
    test/integration/scripts/wait-ready.sh

# Run REST transport contract tests against the Salt integration environment
contract-rest:
    test/integration/scripts/wait-ready.sh
    BRINE_INTEGRATION=1 go test -tags=integration ./transports/rest -run TestIntegration -count=1 -v

# Run Python command bridge contract tests against the Salt integration environment
contract-python:
    test/integration/scripts/wait-ready.sh
    BRINE_INTEGRATION=1 go test -tags=integration ./transports/python -run TestIntegration -count=1 -v

# Run all contract tests against the Salt integration environment
contract: contract-rest contract-python

# Print REST/Python contract compatibility table
compat:
    test/integration/scripts/wait-ready.sh
    BRINE_INTEGRATION=1 go run ./cmd/brine-compatcheck

# Print REST/Python contract compatibility as JSON
compat-json:
    test/integration/scripts/wait-ready.sh
    BRINE_INTEGRATION=1 go run ./cmd/brine-compatcheck --format json

# Run the brine CLI (defaults to local integration env, override with --url)
cli *args:
    BRINE_PASS=saltapi BRINE_BRIDGE_CMD=test/integration/scripts/python-bridge.sh go run ./cmd/brine {{args}}

# Capture sanitized REST fixtures from the Salt integration environment
integration-capture-rest:
    test/integration/scripts/capture-rest-fixtures.sh

# Stop and remove the Salt integration environment
integration-down:
    {{COMPOSE}} -f {{COMPOSE_FILE}} down -v

# Check for clean git state after running fmt and tidy
check-clean: fmt tidy
    git diff --exit-code
