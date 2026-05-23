# Release checklist

Use this checklist before tagging an MVP release or handing Brine to an
operator.

## Local verification

Run from the repository root:

```sh
just fmt
just
env PYTHONPYCACHEPREFIX=/tmp/brine-pycache \
  python3 -m py_compile transports/python/brine_salt_bridge.py
go mod tidy -diff
go -C tools mod tidy -diff
git diff --check
```

## Salt compatibility

Run the live REST/Python contract matrix against the Docker/Salt topology:

```sh
just integration-up
just compat
just integration-down
```

The expected MVP shape is:

- REST passes local, runner, state, async, progress, event, batch, lowstate,
  target-resolution, and failure-classification contracts.
- Python passes local, runner, state, async, progress, target-resolution, and
  failure-classification contracts.
- Python skips global events, batch, and raw lowstate because it intentionally
  does not advertise those capabilities.

## Tagging

Before creating a tag:

1. Reconcile the release branch with `origin/main`.
2. Confirm `README.md` and `COMPATIBILITY.md` describe the supported matrix.
3. Confirm the manual Compatibility workflow passes, or paste the local
   `just compat` summary into the release notes.
4. Tag the release, for example `v0.1.0`, and include the breaking change that
   typed wheel APIs were removed from the root API.
