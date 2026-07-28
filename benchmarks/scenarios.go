package main

// scenarios is the curated list of benchmarks shown in the report.
//
// Add new entries here; the report renders them in the order below,
// grouped by Group. Group names are stable strings — entries sharing
// a Group land in the same report section, in declaration order.
//
// Use CompareWith to express "this is overhead over X" — the renderer
// shows Δ ns and Δ% in the report. Pick a baseline that's intuitive
// for the reader: typically the closest fixture-floor row.
var scenarios = []Scenario{
	// ---- Local (top-line: producer commit-thread, paired with stock SQLite)

	{
		Group:       "Local",
		Name:        "sqlite (single)",
		Bench:       "BenchmarkBaselineInsert",
		Package:     "./sqlitebridge",
		Description: "Stock SQLite, no syzy hooks. Reference for what unmodified SQLite costs on this host.",
	},
	{
		Group:       "Local",
		Name:        "syzy (single)",
		Bench:       "BenchmarkCommitInsert",
		Package:     "./internal/producer",
		Description: "Same workload with syzy's preupdate touch journal + walhook + journal append wired in.",
		CompareWith: "sqlite (single)",
	},
	{
		Group:   "Local",
		Name:    "sqlite (batch=8)",
		Bench:   "BenchmarkBaselineInsertBatched/batch=8",
		Package: "./sqlitebridge",
	},
	{
		Group:       "Local",
		Name:        "syzy (batch=8)",
		Bench:       "BenchmarkCommitInsertBatched/batch=8",
		Package:     "./internal/producer",
		CompareWith: "sqlite (batch=8)",
	},
	{
		Group:   "Local",
		Name:    "sqlite (batch=64)",
		Bench:   "BenchmarkBaselineInsertBatched/batch=64",
		Package: "./sqlitebridge",
	},
	{
		Group:       "Local",
		Name:        "syzy (batch=64)",
		Bench:       "BenchmarkCommitInsertBatched/batch=64",
		Package:     "./internal/producer",
		CompareWith: "sqlite (batch=64)",
	},
	{
		Group:       "Local",
		Name:        "sqlite (batch=512)",
		Bench:       "BenchmarkBaselineInsertBatched/batch=512",
		Package:     "./sqlitebridge",
		Description: "At batch=512 per-row cost approaches what the SQLite vdbe alone costs.",
	},
	{
		Group:       "Local",
		Name:        "syzy (batch=512)",
		Bench:       "BenchmarkCommitInsertBatched/batch=512",
		Package:     "./internal/producer",
		CompareWith: "sqlite (batch=512)",
	},

	// ---- Replication (round-trip latency + pipelined throughput, both vs stock SQLite)

	{
		Group:   "Replication",
		Name:    "sqlite (single)",
		Bench:   "BenchmarkBaselineInsert",
		Package: "./sqlitebridge",
	},
	{
		Group:       "Replication",
		Name:        "syzy round-trip (single)",
		Bench:       "BenchmarkRoundTripInsert",
		Package:     "./internal/testcluster",
		Description: "Per-row WaitApplied — A's commit blocks until B reports it applied. nodestate.Cache + per-origin journals + drainer-side broadcast via OnEncoded.",
		CompareWith: "sqlite (single)",
	},
	{
		Group:       "Replication",
		Name:        "syzy pipelined (single)",
		Bench:       "BenchmarkPipelinedInsert",
		Package:     "./internal/testcluster",
		Description: "All INSERTs back-to-back, one WaitApplied at the end. A's commit, drainer encode, and B's apply run concurrently.",
		CompareWith: "sqlite (single)",
	},
	{
		Group:   "Replication",
		Name:    "sqlite (batch=8)",
		Bench:   "BenchmarkBaselineInsertBatched/batch=8",
		Package: "./sqlitebridge",
	},
	{
		Group:       "Replication",
		Name:        "syzy round-trip (batch=8)",
		Bench:       "BenchmarkRoundTripInsertBatched/batch=8",
		Package:     "./internal/testcluster",
		CompareWith: "sqlite (batch=8)",
	},
	{
		Group:       "Replication",
		Name:        "syzy pipelined (batch=8)",
		Bench:       "BenchmarkPipelinedInsertBatched/batch=8",
		Package:     "./internal/testcluster",
		CompareWith: "sqlite (batch=8)",
	},
	{
		Group:   "Replication",
		Name:    "sqlite (batch=64)",
		Bench:   "BenchmarkBaselineInsertBatched/batch=64",
		Package: "./sqlitebridge",
	},
	{
		Group:       "Replication",
		Name:        "syzy round-trip (batch=64)",
		Bench:       "BenchmarkRoundTripInsertBatched/batch=64",
		Package:     "./internal/testcluster",
		CompareWith: "sqlite (batch=64)",
	},
	{
		Group:       "Replication",
		Name:        "syzy pipelined (batch=64)",
		Bench:       "BenchmarkPipelinedInsertBatched/batch=64",
		Package:     "./internal/testcluster",
		CompareWith: "sqlite (batch=64)",
	},
	{
		Group:   "Replication",
		Name:    "sqlite (batch=512)",
		Bench:   "BenchmarkBaselineInsertBatched/batch=512",
		Package: "./sqlitebridge",
	},
	{
		Group:       "Replication",
		Name:        "syzy round-trip (batch=512)",
		Bench:       "BenchmarkRoundTripInsertBatched/batch=512",
		Package:     "./internal/testcluster",
		CompareWith: "sqlite (batch=512)",
	},
	{
		Group:       "Replication",
		Name:        "syzy pipelined (batch=512)",
		Bench:       "BenchmarkPipelinedInsertBatched/batch=512",
		Package:     "./internal/testcluster",
		CompareWith: "sqlite (batch=512)",
	},

	// ---- Producer commit-thread latency (component-level detail) ---------

	{
		Group:       "Producer commit-thread latency",
		Name:        "Single-row INSERT (full pipeline)",
		Bench:       "BenchmarkCommitInsert",
		Package:     "./internal/producer",
		Description: "Full producer hot path: preupdate touch journal, walHook, journal append. Drainer runs concurrently in its goroutine.",
		CompareWith: "Fixture floor: INSERT (no-op walHook)",
	},
	{
		Group:       "Producer commit-thread latency",
		Name:        "Single-row UPDATE (full pipeline)",
		Bench:       "BenchmarkCommitUpdate",
		Package:     "./internal/producer",
		Description: "UPDATE writes one fewer WAL frame than INSERT in the bench fixture, so the absolute time is lower.",
		CompareWith: "Fixture floor: UPDATE (no-op walHook)",
	},
	{
		Group:       "Producer commit-thread latency",
		Name:        "Fixture floor: INSERT (no-op walHook)",
		Bench:       "BenchmarkHookFixtureNoOpInsert",
		Package:     "./internal/producer",
		Description: "Touch journal + no-op walHook installed; no drainer, no journal append. Lower bound on syzy's commit-thread cost.",
	},
	{
		Group:   "Producer commit-thread latency",
		Name:    "Fixture floor: UPDATE (no-op walHook)",
		Bench:   "BenchmarkHookFixtureNoOpUpdate",
		Package: "./internal/producer",
	},

	// ---- Component-level paths -------------------------------------------

	{
		Group:       "Components",
		Name:        "Journal append (small payload)",
		Bench:       "BenchmarkAppend",
		Package:     "./internal/journal",
		Description: "Single record append into the mmap journal segment, in isolation.",
	},
	{
		Group:       "Components",
		Name:        "CRDT changeset build (INSERT)",
		Bench:       "BenchmarkBuildInsert",
		Package:     "./crdt",
		Description: "Encoding one Insert record into a Changeset. Runs on the drainer thread, off the commit-thread hot path.",
	},
	{
		Group:       "Components",
		Name:        "CRDT changeset decode (INSERT)",
		Bench:       "BenchmarkDecodeInsert",
		Package:     "./crdt",
		Description: "Inverse of the build above. Runs on remote nodes when applying inbound changesets.",
	},
	{
		Group:       "Components",
		Name:        "Broker apply: INSERT",
		Bench:       "BenchmarkApplyInsert",
		Package:     "./internal/broker",
		Description: "Remote-side path: decoded changeset → SQL apply against a peer's app.db, including row_clock checks and conflict resolution.",
	},
}

// groupNotes provides a short explainer rendered above each group's
// table.
var groupNotes = map[string]string{
	"Local":                          `Local commit-thread cost — no replication.`,
	"Replication":                    `Both syzy modes are paired against stock SQLite at the same batch size. **round-trip** waits per row for B to apply (the latency-floor measurement: A's commit serialized with B's apply). **pipelined** issues all INSERTs back-to-back with one WaitApplied at the end (the steady-state throughput measurement: A's commit, drainer encode, and B's apply run concurrently). In-process transport, so this is the protocol-overhead floor — production network latency adds to round-trip and may also bound pipelined when the slow side becomes the network rather than B's apply.`,
	"Producer commit-thread latency": `Per-iteration time covers SQLite's own statement execution + WAL fsync, plus syzy's commit-thread work.`,
}
