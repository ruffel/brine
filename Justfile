# Justfile for brine

MODULES := ""
COMPOSE := env_var_or_default("BRINE_COMPOSE", "docker compose")

# Default recipe
default: test lint

# Run all tests with race detection
test:
    go test -race ./... $(for mod in {{MODULES}}; do echo "./$mod/..."; done)

# Run linters
lint:
    golangci-lint run ./... $(for mod in {{MODULES}}; do echo "./$mod/..."; done)

# Clean build artifacts
clean:
    go clean

# Run go fmt
fmt:
    go fmt ./...
    for mod in {{MODULES}}; do \
        (cd $mod && go fmt ./...); \
    done

# Run go mod tidy
tidy:
    go mod tidy
    for mod in {{MODULES}}; do \
        (cd $mod && go mod tidy); \
    done

# Start the Salt integration environment
integration-up:
    {{COMPOSE}} -f test/integration/compose.yaml up -d --build

# Wait for the Salt integration environment to be ready
integration-ready:
    test/integration/scripts/wait-ready.sh

# Capture sanitized REST fixtures from the Salt integration environment
integration-capture-rest:
    test/integration/scripts/capture-rest-fixtures.sh

# Stop and remove the Salt integration environment
integration-down:
    {{COMPOSE}} -f test/integration/compose.yaml down -v

# Check for clean git state after running fmt and tidy
check-clean: fmt tidy
    git diff --exit-code
