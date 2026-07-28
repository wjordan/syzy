// Package sqlite provides Syzy's experimental multi-writer SQLite engine over
// the shared crdt changeset, schema, transport, and anti-entropy system.
//
// Open starts a local replica. NewDB exposes the familiar database/sql shape;
// Restore bootstraps replicas from peers or an object-store bucket;
// Node.Subscribe observes applied changes.
// Most applications need only this package. The crdt, transport, objectstore,
// schemalog, and sqlitebridge packages define protocol or provider extension
// points for integrations.
package sqlite
