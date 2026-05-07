package main

import (
	"bytes"
	"context"
	"testing"
)

func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}

	want := `== typed wrapper ==
MINION  PACKAGE  INSTALLED  VERSION
web-1   nginx    true       1.2.3
web-2   nginx    false      -

== partial failure and missing minion ==
partial=true failed=api-2,api-3 missing=api-3 returned=api-1,api-2

== progress observer ==
request.expected_minions worker-1,worker-2
minion.returned worker-1
minion.returned worker-2
request.completed ok=true
`
	if got := out.String(); got != want {
		t.Fatalf("unexpected output\nwant:\n%s\ngot:\n%s", want, got)
	}
}
