package postgres

import (
	"context"
	_ "embed"
	"strconv"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

//go:embed sql/ddl.sql
var ddlSQL string

//go:embed sql/gen_id.sql
var genIDSQL string

// ddlIntentTableName is the decodable spool the event triggers write to (§6).
// Capture turns its rows into DDL descriptors and prunes them — DDL has no
// logical-decoding trace of its own, so these trigger-written rows ARE the
// capture signal.
const ddlIntentTableName = "syzy_ddl_intent"

// ddlIntent is one decoded syzy_ddl_intent row: the structured descriptor the
// ddl_command_end / sql_drop trigger persisted for a single DDL command. It is
// the unambiguous catalog key — (classid, objid, objsubid) — plus the command
// tag and grouping keys; the typed CatalogOp is built from the catalog against
// this key in a later increment, never parsed from auditQuery.
type ddlIntent struct {
	seq            int64  // syzy_ddl_intent.seq, for high-water-mark pruning
	txid           int64  // groups one transaction's commands
	ordinal        int    // per-command order within the txn
	commandTag     string // 'CREATE TABLE', 'ALTER TABLE', 'DROP', ...
	objectType     string // 'table', 'index', 'view', 'column', ...
	classid        uint32 // catalog the object lives in
	objid          uint32 // object's oid
	objsubid       int32  // column attnum (0 for whole-object)
	schemaName     string
	objectIdentity string
	isDrop         bool
	auditQuery     string // current_query(): audit text only
}

// installDDLSupport creates syzy_ddl_intent and the event triggers (idempotent —
// CREATE … IF NOT EXISTS / OR REPLACE / existence-guarded EVENT TRIGGERs). It
// runs under the syzy.internal guard so its own DDL (and any re-run drop/create)
// never writes intent rows or fires the triggers being installed.
func installDDLSupport(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, `SET syzy.internal = 'on'`); err != nil {
		return err
	}
	_, execErr := conn.Exec(ctx, ddlSQL)
	if execErr == nil {
		// gen_id(): the cross-engine id default a SQLite-authored schema's
		// columns reference (sql/gen_id.sql). Installed with DDL support so
		// applied CREATE TABLEs carrying DEFAULT (gen_id('t')) resolve.
		_, execErr = conn.Exec(ctx, genIDSQL)
	}
	if _, err := conn.Exec(ctx, `SET syzy.internal = 'off'`); err != nil && execErr == nil {
		execErr = err
	}
	return execErr
}

// decodeDDLIntent extracts one syzy_ddl_intent row's fields from its decoded
// tuple. ok is false only if a required field is missing (a malformed row).
// pgoutput is text mode, so oids/ints arrive as decimal strings and bools as
// 't'/'f'; nullable text columns decode to "".
func (e *relEntry) decodeDDLIntent(t *pglogrepl.TupleData) (ddlIntent, bool) {
	text := func(name string) (string, bool) {
		i, present := e.intentIdx[name]
		if !present || i >= len(t.Columns) || t.Columns[i].DataType != 't' {
			return "", false
		}
		return string(t.Columns[i].Data), true
	}
	seqS, ok1 := text("seq")
	txidS, ok2 := text("txid")
	ordS, ok3 := text("ordinal")
	tag, ok4 := text("command_tag")
	classidS, ok5 := text("classid")
	objidS, ok6 := text("objid")
	objsubidS, ok7 := text("objsubid")
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7) {
		return ddlIntent{}, false
	}
	seq, e1 := strconv.ParseInt(seqS, 10, 64)
	txid, e2 := strconv.ParseInt(txidS, 10, 64)
	ord, e3 := strconv.Atoi(ordS)
	classid, e4 := strconv.ParseUint(classidS, 10, 32)
	objid, e5 := strconv.ParseUint(objidS, 10, 32)
	objsubid, e6 := strconv.ParseInt(objsubidS, 10, 32)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil {
		return ddlIntent{}, false
	}
	objType, _ := text("object_type")
	schema, _ := text("schema_name")
	ident, _ := text("object_identity")
	audit, _ := text("audit_query")
	dropS, _ := text("is_drop")
	return ddlIntent{
		seq:            seq,
		txid:           txid,
		ordinal:        ord,
		commandTag:     tag,
		objectType:     objType,
		classid:        uint32(classid),
		objid:          uint32(objid),
		objsubid:       int32(objsubid),
		schemaName:     schema,
		objectIdentity: ident,
		isDrop:         dropS == "t",
		auditQuery:     audit,
	}, true
}

// accumDDLIntent folds one decoded syzy_ddl_intent INSERT into the transaction,
// recording its seq for pruning.
func (c *capturer) accumDDLIntent(cur *txnAccum, e *relEntry, m *pglogrepl.InsertMessage) {
	if cur == nil {
		return
	}
	d, ok := e.decodeDDLIntent(m.Tuple)
	if !ok {
		return
	}
	cur.ddlIntents = append(cur.ddlIntents, d)
	cur.ddlIntentSeqs = append(cur.ddlIntentSeqs, d.seq)
}

// pruneDDLIntents deletes consumed intent rows up to the high-water mark of
// every seq ever delivered — self-healing: a previously
// failed prune's dead rows are swept by the next, and deleting a row can never
// drop a command (capture decodes from the WAL, and the DELETE is itself
// dropped by capture since syzy_ddl_intent carries no catalog entry). Called
// only from the single-threaded run loop, so ddlHWM needs no lock.
func (c *capturer) pruneDDLIntents(ctx context.Context, seqs []int64) {
	if c.pruneConn == nil || len(seqs) == 0 {
		return
	}
	for _, s := range seqs {
		if s > c.ddlHWM {
			c.ddlHWM = s
		}
	}
	_, _ = c.pruneConn.Exec(ctx, `DELETE FROM syzy_ddl_intent WHERE seq <= $1`, c.ddlHWM)
}
