package metadata

// schemaVersion is the canonical metadata schema version. Open
// verifies that either no schema is present (fresh init) or the
// existing schema declares this version. Released columns remain reserved in
// the file format so feature removal does not require a destructive migration.
const schemaVersion = 4

// schemaSQL is the canonical metadata schema. Applied verbatim during Open
// against a fresh metadata. The script is idempotent against repeated
// application: every CREATE uses IF NOT EXISTS.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS frontier (
  origin   INTEGER PRIMARY KEY,
  last_seq INTEGER NOT NULL,
  last_hlc INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS row_clock (
  table_id    BLOB    NOT NULL,
  pk_blob     BLOB    NOT NULL,
  cl          INTEGER NOT NULL,
  base_hlc    INTEGER NOT NULL,
  base_origin INTEGER NOT NULL,
  PRIMARY KEY (table_id, pk_blob)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS syzy_table (
  table_id            BLOB    PRIMARY KEY,
  name                TEXT    NOT NULL,
  state               TEXT    NOT NULL,    -- 'active' | 'dropped'
  default_clock_group TEXT    NOT NULL,    -- 'row' | 'cell'
  create_seq          INTEGER NOT NULL,
  drop_seq            INTEGER
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS syzy_table_active_name
  ON syzy_table(name) WHERE state = 'active';

CREATE TABLE IF NOT EXISTS syzy_column (
  table_id    BLOB    NOT NULL,
  column_id   BLOB    NOT NULL,
  name        TEXT    NOT NULL,
  ordinal     INTEGER NOT NULL,
  state       TEXT    NOT NULL,            -- 'active' | 'dropped'
  clock_group TEXT    NOT NULL,            -- 'row' | 'cell'
  collation   INTEGER NOT NULL DEFAULT 0,  -- 0 = BINARY, 1 = NOCASE, 2 = RTRIM
  create_seq  INTEGER NOT NULL,
  drop_seq    INTEGER,
  declared_type TEXT  NOT NULL DEFAULT '', -- reserved
  auto_pk     INTEGER NOT NULL DEFAULT 0,  -- reserved
  PRIMARY KEY (table_id, column_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS syzy_key (
  table_id    BLOB    NOT NULL,
  key_id      BLOB    NOT NULL,            -- 0x00..00 = PK
  column_id   BLOB    NOT NULL,
  ordinal     INTEGER NOT NULL,
  state       TEXT    NOT NULL,            -- 'active' | 'dropped'
  coordinated INTEGER NOT NULL DEFAULT 0,  -- 1 = CP (reserved); 0 = eventual (loser-null)
  predicate   BLOB,                        -- compiled partial-index WHERE clause; NULL = total key
  create_seq  INTEGER NOT NULL,
  drop_seq    INTEGER,
  PRIMARY KEY (table_id, key_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS syzy_schema_event (
  schema_seq    INTEGER PRIMARY KEY,
  parent_seq    INTEGER NOT NULL,
  catalog_op    BLOB    NOT NULL,
  raw_sql       TEXT,
  applied_at_us INTEGER NOT NULL,
  apply_state   TEXT    NOT NULL           -- 'applied' | 'failed_local'
) STRICT;

-- syzy_synth_trigger tracks BEFORE DELETE / BEFORE UPDATE triggers
-- synthesized from FK cascade actions. The trigger lives on the
-- referenced (parent) table; on DROP TABLE of the child, the producer
-- enumerates rows here and emits matching DROP TRIGGER ops in the
-- same Bundle.
CREATE TABLE IF NOT EXISTS syzy_synth_trigger (
  child_table_id BLOB    NOT NULL,
  trigger_name   TEXT    NOT NULL,
  parent_table   TEXT    NOT NULL,
  PRIMARY KEY (child_table_id, trigger_name)
) STRICT, WITHOUT ROWID;

-- cell_clock holds per-(row, column) Stamp overrides. Sparse: only
-- written when a column needs independent ordering (UNIQUE arbitration
-- loser-null, blob-patch interaction). Effective Stamp falls through
-- cell_clock → row_clock baseline. See docs/ARCHITECTURE.md and sqlite/docs/DDL.md
-- (#sparse-clock-groups).
CREATE TABLE IF NOT EXISTS cell_clock (
  table_id   BLOB    NOT NULL,
  pk_blob    BLOB    NOT NULL,
  column_id  BLOB    NOT NULL,
  hlc        INTEGER NOT NULL,
  hlc_origin INTEGER NOT NULL,
  PRIMARY KEY (table_id, pk_blob, column_id)
) STRICT, WITHOUT ROWID;

-- blob_range_clock holds per-row, per-blob-column IntervalMap state
-- for blob_patch. At most one row per (table_id, pk_blob); empty for
-- typical INSERT/UPDATE workloads. Format of intervals (BE):
--   per blob column with entries:
--     16 bytes column_id
--      2 bytes n_intervals (u16)
--      n_intervals * (start u64, end u64, hlc u64, origin u64)
-- See BLOB_PATCH.md.
CREATE TABLE IF NOT EXISTS blob_range_clock (
  table_id  BLOB NOT NULL,
  pk_blob   BLOB NOT NULL,
  intervals BLOB NOT NULL,
  PRIMARY KEY (table_id, pk_blob)
) STRICT, WITHOUT ROWID;

-- sender_seq tracks the next-to-allocate seq for every origin whose
-- journal this daemon drains. With the in-process model (one writer
-- per node) this holds one row; with the loadable-extension model
-- (multiple writer processes per box, one daemon) it holds one row
-- per origin's per-process journal.
CREATE TABLE IF NOT EXISTS sender_seq (
  origin   INTEGER PRIMARY KEY,
  next_seq INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

-- apply_quarantine holds inbound changesets that failed apply with a
-- deterministic DML constraint violation (e.g. a partial record forced to
-- materialize a row whose cross-origin INSERT has not yet been delivered, so
-- a NOT NULL column is unsatisfiable). Rather than letting one such record
-- permanently + silently pin the per-origin frontier and starve every later
-- seq from that origin, the broker quarantines the payload here, advances the
-- frontier past it (restoring liveness), and re-applies it on later rounds
-- once the missing dependency lands. Purely additive (created on every Open via
-- CREATE TABLE IF NOT EXISTS); needs no schema_version bump and older builds
-- ignore it.
CREATE TABLE IF NOT EXISTS apply_quarantine (
  origin     INTEGER NOT NULL,
  seq        INTEGER NOT NULL,
  payload    BLOB    NOT NULL,
  err        TEXT    NOT NULL,
  first_seen INTEGER NOT NULL,
  attempts   INTEGER NOT NULL,
  PRIMARY KEY (origin, seq)
) STRICT, WITHOUT ROWID;
`

// pragmaSetupSQL runs on every connection: cheap, no writer-lock
// acquisition. journal_mode is intentionally NOT here — applying it
// requires the WAL writer slot, which under multi-process contention
// (host writer + N guest syzy.so) deterministically loses to SQLite's
// internal walTryBeginRead retry loop (100 retries, ~10s budget,
// independent of busy_timeout). applySchema reads the current
// journal_mode and only writes WAL when needed.
const pragmaSetupSQL = `
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;
PRAGMA wal_autocheckpoint = 500;
PRAGMA foreign_keys = OFF;
`

// pragmaConvertWALSQL is run only when the file is not already in WAL
// mode. Conversion takes the writer slot briefly; the first opener
// pays this cost, subsequent openers skip via the read in applySchema.
const pragmaConvertWALSQL = `PRAGMA journal_mode = WAL;`
