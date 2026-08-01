# Contributing

Syzy is a spec-driven project. Start with [Concepts](docs/CONCEPTS.md), then read
the specification for the area you plan to change.

## Specifications and code

Update the relevant spec before changing behavior:

- External contracts—wire formats, on-disk layouts, public provider
  interfaces, invariants, and consistency claims—are authoritative in the
  specs. Implementations must match them.
- Internal Go structure is code-authoritative. Specs describe intent,
  invariants, and ordering without duplicating function bodies.
- Designs without code may use exact DDL or pseudocode until implemented.

The main system references are [ARCHITECTURE.md](docs/ARCHITECTURE.md),
[CRDT.md](docs/CRDT.md), [PROTOCOL.md](docs/PROTOCOL.md),
[SCHEMA.md](docs/SCHEMA.md), and [TRANSPORT.md](docs/TRANSPORT.md).
SQLite-specific behavior belongs to the
[SQLite architecture](sqlite/docs/ARCHITECTURE.md).
Postgres-specific behavior belongs to the [Postgres engine](docs/postgres.md).

## Building

You need Go 1.25 or newer and Make. SQLite builds also need a C compiler: Syzy
vendors the SQLite amalgamation and its extension is a `c-shared` cgo build, so
`CGO_ENABLED=1` is mandatory and `go install` cannot produce the extension — use
Make:

```bash
make build ext      # bin/syzy and ext/syzy.so
make build-pg       # bin/syzy-pg
export PATH="$PWD/bin:$PATH"
```

Then load the extension by path — `.load ./ext/syzy` — rather than by the bare
name an installed copy resolves under.

Release builds additionally stamp a version, which the CLI and the extension
compare on every attach:

```bash
make build ext VERSION=v0.1.0
```

## Modules

The repository is a Go workspace containing the root module and the nested
`pg` module. The SQLite runtime and lazy-restore support are packages under the
root module; the Postgres sidecar lives in `pg`.

Run the complete checks with:

```bash
make vet
make test
make test-pg
```

To test the module without ambient workspace resolution:

```bash
GOWORK=off go test ./...
```

## Change shape

- Keep the SQLite package focused on database and application lifecycle; the
  root module carries supporting protocol, transport, and runtime packages.
- Keep Postgres-specific capture, apply, and DDL realization in the nested
  `pg` module.
- Make `objectstore.Bucket` contract and backend changes in the standalone
  [`objectstore`](https://github.com/wjordan/objectstore) module; this repository
  owns the object layouts and runtime integration built on that contract.
- Prefer small provider interfaces over exposing internal state.
- Put implementation packages under `internal` unless outside callers must
  construct or implement the type.
- Add regression tests at the narrowest layer that owns the invariant.
- Delete superseded code and stale design commentary instead of retaining
  compatibility scaffolding without a production requirement.

Before submitting a change, run formatting, tests, vet, and `git diff --check`.
