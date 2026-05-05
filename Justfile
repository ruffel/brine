# Justfile for brine

COMPOSE := "test/integration/scripts/compose.sh"

# Default recipe
default: test lint

# Run all tests with race detection
test:
    go test -race ./...

# Run linters
lint:
    golangci-lint run ./...

# Clean build artifacts
clean:
    go clean

# Run go fmt
fmt:
    go fmt ./...

# Run go mod tidy
tidy:
    go mod tidy

# Start the Salt integration environment
integration-up:
    {{COMPOSE}} -f test/integration/compose.yaml up -d --build

# Wait for the Salt integration environment to be ready
integration-ready:
    test/integration/scripts/wait-ready.sh

# Run all integration-tagged tests that do not require a live Salt endpoint
integration-test:
    go test -tags=integration ./...

# Run REST transport contract tests against the Salt integration environment
contract-rest: integration-ready
    BRINE_INTEGRATION=1 go test -tags=integration ./transports/rest -run TestIntegrationRESTContracts -count=1 -v

# Run all REST integration tests against the Salt integration environment
integration-test-rest: integration-ready
    BRINE_INTEGRATION=1 go test -tags=integration ./transports/rest -count=1 -v

# Capture sanitized REST fixtures from the Salt integration environment
integration-capture-rest:
    test/integration/scripts/capture-rest-fixtures.sh

# Stop and remove the Salt integration environment
integration-down:
    {{COMPOSE}} -f test/integration/compose.yaml down -v

# Check for clean git state after running fmt and tidy
check-clean: fmt tidy
    git diff --exit-code
