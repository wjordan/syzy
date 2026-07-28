package producer

import (
	"strings"
	"testing"

	"github.com/wjordan/syzy/unique"
)

// coordFixture is a ddlFixture with a reservation backend, so
// coordinated keys are admissible.
func coordFixture(t *testing.T) *ddlFixture {
	t.Helper()
	return newDDLFixtureCfg(t, Config{UniqueRegistry: unique.NewLocal()})
}

func mustExec(t *testing.T, f *ddlFixture, sql string) {
	t.Helper()
	if err := f.app.Exec(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func wantReject(t *testing.T, f *ddlFixture, sql, why string) {
	t.Helper()
	if err := f.app.Exec(sql); err == nil {
		t.Errorf("%q accepted; want rejection (%s)", sql, why)
	}
}

func TestDDL_RejectsGeneratedUniqueMember(t *testing.T) {
	f := coordFixture(t)
	// Inline path: generated member in CREATE TABLE.
	wantReject(t, f,
		`CREATE TABLE g (id BLOB PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL GENERATED ALWAYS AS (a+1) STORED UNIQUE)`,
		"generated columns are recomputed per replica, never captured")
	// Standalone path: CREATE UNIQUE INDEX on a generated column.
	mustExec(t, f, `CREATE TABLE h (id BLOB PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL GENERATED ALWAYS AS (a+1) STORED)`)
	f.waitDrain(t)
	wantReject(t, f, `CREATE UNIQUE INDEX h_b ON h(b)`, "generated member via index")
}

func TestDDL_RejectsBlobUniqueIndex(t *testing.T) {
	// Standalone path: BLOB member rejected regardless of NOT NULL.
	f := coordFixture(t)
	mustExec(t, f, `CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL, fp BLOB NOT NULL)`)
	f.waitDrain(t)
	wantReject(t, f, `CREATE UNIQUE INDEX b_fp ON b(fp)`, "BLOB member via index")
}

func TestDDL_RejectsTriggerWritingCoordinatedTable(t *testing.T) {
	f := coordFixture(t)
	mustExec(t, f, `CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
	mustExec(t, f, `CREATE TABLE log (id BLOB PRIMARY KEY NOT NULL, note TEXT)`)
	f.waitDrain(t)

	wantReject(t, f,
		`CREATE TRIGGER tr AFTER INSERT ON log BEGIN INSERT INTO u (id, email) VALUES (randomblob(16), 'x'); END`,
		"trigger INSERT into coordinated table")
	wantReject(t, f,
		`CREATE TRIGGER tr AFTER INSERT ON log BEGIN UPDATE u SET email = 'y'; END`,
		"trigger UPDATE of coordinated table")
	// DELETE bodies are fine: the vacated value is observed and freed.
	mustExec(t, f, `CREATE TRIGGER tr_del AFTER INSERT ON log BEGIN DELETE FROM u WHERE email = new.note; END`)
	// Writes to non-coordinated tables are fine too.
	mustExec(t, f, `CREATE TRIGGER tr_log AFTER DELETE ON u BEGIN INSERT INTO log (id, note) VALUES (randomblob(16), 'gone'); END`)
}

func TestDDL_RejectsCoordinatedKeyOnTriggerTarget(t *testing.T) {
	f := coordFixture(t)
	mustExec(t, f, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT NOT NULL)`)
	mustExec(t, f, `CREATE TABLE src (id BLOB PRIMARY KEY NOT NULL, v TEXT)`)
	mustExec(t, f, `CREATE TRIGGER mirror AFTER INSERT ON src BEGIN UPDATE t SET v = new.v; END`)
	f.waitDrain(t)

	wantReject(t, f, `CREATE UNIQUE INDEX t_v ON t(v)`,
		"existing trigger writes the table")
	// A coordinated key in a fresh CREATE TABLE is blocked the same way
	// when an existing trigger targets that (not-yet-created) name.
	mustExec(t, f, `CREATE TRIGGER pre AFTER INSERT ON src BEGIN INSERT INTO future (id, v) VALUES (new.id, new.v); END`)
	wantReject(t, f, `CREATE TABLE future (id BLOB PRIMARY KEY NOT NULL, v TEXT NOT NULL UNIQUE)`,
		"pre-existing trigger writes the new table's name")
	// Dropping the trigger clears the path.
	mustExec(t, f, `DROP TRIGGER mirror`)
	mustExec(t, f, `CREATE UNIQUE INDEX t_v ON t(v)`)
	f.waitDrain(t)
	if uk := onlyUniqueKey(t, f, "t"); !uk.Coordinated {
		t.Errorf("key not coordinated after trigger dropped: %+v", uk)
	}
}

func TestDDL_RejectsCascadeUpdateOnCoordinatedChild(t *testing.T) {
	f := coordFixture(t)
	mustExec(t, f, `CREATE TABLE parent (id BLOB PRIMARY KEY NOT NULL)`)
	f.waitDrain(t)
	// SET NULL synthesizes an UPDATE-child trigger; the child declares a
	// coordinated key → rejected.
	wantReject(t, f,
		`CREATE TABLE child (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE, pid BLOB REFERENCES parent(id) ON DELETE SET NULL)`,
		"SET NULL cascade writes the coordinated child")
	// ON DELETE CASCADE only deletes child rows — admissible.
	mustExec(t, f,
		`CREATE TABLE child (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE, pid BLOB REFERENCES parent(id) ON DELETE CASCADE)`)
}

func TestDDL_RejectsSameTxnCoordinatedDDLAndDML(t *testing.T) {
	f := coordFixture(t)
	mustExec(t, f, `BEGIN`)
	mustExec(t, f, `CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
	mustExec(t, f, `INSERT INTO u (id, email) VALUES (randomblob(16), 'a@x.com')`)
	err := f.app.Exec(`COMMIT`)
	if err == nil {
		t.Fatal("COMMIT accepted; want rejection (DML on a table whose coordinated key is pending in the same txn)")
	}
	if !strings.Contains(err.Error(), "constraint") && !strings.Contains(err.Error(), "COMMIT") {
		t.Logf("commit error (informational): %v", err)
	}
	_ = f.app.Exec(`ROLLBACK`)

	// Control: the same shape without a coordinated key commits.
	mustExec(t, f, `BEGIN`)
	mustExec(t, f, `CREATE TABLE v (id BLOB PRIMARY KEY NOT NULL, email TEXT)`)
	mustExec(t, f, `INSERT INTO v (id, email) VALUES (randomblob(16), 'a@x.com')`)
	mustExec(t, f, `COMMIT`)

	// Control: coordinated DDL alone in a txn commits, and DML in the
	// next transaction reserves normally.
	mustExec(t, f, `BEGIN`)
	mustExec(t, f, `CREATE TABLE w (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
	mustExec(t, f, `COMMIT`)
	f.waitDrain(t)
	mustExec(t, f, `INSERT INTO w (id, email) VALUES (randomblob(16), 'a@x.com')`)
}

func TestDDL_RejectsCompositeCoordinatedOnCounterTable(t *testing.T) {
	f := coordFixture(t)
	wantReject(t, f,
		`CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, a TEXT NOT NULL, b TEXT NOT NULL, n INTEGER COUNTER NOT NULL DEFAULT 0, UNIQUE(a, b))`,
		"composite coordinated key on a counter (cell-group) table")
	// Single-column coordinated key on the same shape is fine.
	mustExec(t, f,
		`CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, a TEXT NOT NULL UNIQUE, n INTEGER COUNTER NOT NULL DEFAULT 0)`)
}

// TestDDL_TriggerBanFoldsIdentifiers: SQLite folds identifiers and
// accepts quoting forms the DDL parser does not, so a trigger body
// naming the coordinated table in any of them writes it. Each case here
// committed two rows holding one coordinated value — no reservation, no
// index anywhere to catch it — before the ban matched case-insensitively
// and failed closed on unparsable trigger SQL.
func TestDDL_TriggerBanFoldsIdentifiers(t *testing.T) {
	bodies := map[string]string{
		"upcased":   `INSERT INTO U (id, email) VALUES (randomblob(16), 'dup')`,
		"quoted":    `INSERT INTO "u" (id, email) VALUES (randomblob(16), 'dup')`,
		"backtick":  "INSERT INTO `u` (id, email) VALUES (randomblob(16), 'dup')",
		"bracketed": `INSERT INTO [u] (id, email) VALUES (randomblob(16), 'dup')`,
		"mixedcase": `UPDATE uSeR SET email = 'dup'`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			f := coordFixture(t)
			mustExec(t, f, `CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
			mustExec(t, f, `CREATE TABLE uSeR (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
			mustExec(t, f, `CREATE TABLE log (id BLOB PRIMARY KEY NOT NULL, note TEXT)`)
			f.waitDrain(t)
			wantReject(t, f, `CREATE TRIGGER tr AFTER INSERT ON log BEGIN `+body+`; END`,
				"trigger body writes a coordinated table under SQLite identifier folding")
		})
	}
}

// TestDDL_RejectsIndexShadowingCoordinatedKey: DROP INDEX removes a
// coordinated key by column match, so a second index over exactly the
// key's columns would make the removal ambiguous — dropping an ordinary
// lookup index would silently drop the key. Admission refuses the
// ambiguity from both sides.
func TestDDL_RejectsIndexShadowingCoordinatedKey(t *testing.T) {
	f := coordFixture(t)
	mustExec(t, f, `CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
	f.waitDrain(t)
	wantReject(t, f, `CREATE INDEX ix_email ON u(email)`,
		"plain index over exactly a coordinated key's columns")
	// A different tuple is fine.
	mustExec(t, f, `CREATE INDEX ix_email_id ON u(email, id)`)

	// Other side: a coordinated key may not be created over columns an
	// existing index already covers.
	mustExec(t, f, `CREATE TABLE v (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL)`)
	mustExec(t, f, `CREATE INDEX ix_v_email ON v(email)`)
	f.waitDrain(t)
	wantReject(t, f, `CREATE UNIQUE INDEX uq_v_email ON v(email)`,
		"an existing index already covers the key's columns")
	mustExec(t, f, `DROP INDEX ix_v_email`)
	mustExec(t, f, `CREATE UNIQUE INDEX uq_v_email ON v(email)`)
	f.waitDrain(t)
	if uk := onlyUniqueKey(t, f, "v"); !uk.Coordinated {
		t.Errorf("key not coordinated: %+v", uk)
	}
}

// TestDDL_RejectsAllChildWritingFKActions: every FK action that rewrites
// the child's referencing column synthesizes a cascade trigger, whose
// writes run at trigger depth and never reach the reservation gate.
// ON UPDATE SET NULL / SET DEFAULT were admitted, so a parent-key update
// could rewrite two children to the same coordinated value.
func TestDDL_RejectsAllChildWritingFKActions(t *testing.T) {
	for _, action := range []string{
		"ON DELETE SET NULL", "ON DELETE SET DEFAULT",
		"ON UPDATE CASCADE", "ON UPDATE SET NULL", "ON UPDATE SET DEFAULT",
	} {
		t.Run(action, func(t *testing.T) {
			f := coordFixture(t)
			mustExec(t, f, `CREATE TABLE parent (id BLOB PRIMARY KEY NOT NULL)`)
			f.waitDrain(t)
			wantReject(t, f,
				`CREATE TABLE child (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE, pid BLOB REFERENCES parent(id) `+action+`)`,
				"cascade action writes the coordinated child")
		})
	}
}

// TestDDL_RejectsRenameIntoTriggerTarget: SQLite admits a trigger body
// naming a table that does not exist yet, so renaming a coordinated
// table onto that name installs an ungated write channel after the fact.
// The trigger scan must re-run against the new name.
func TestDDL_RejectsRenameIntoTriggerTarget(t *testing.T) {
	f := coordFixture(t)
	mustExec(t, f, `CREATE TABLE src (id BLOB PRIMARY KEY NOT NULL, v TEXT)`)
	mustExec(t, f, `CREATE TRIGGER tr AFTER INSERT ON src BEGIN INSERT INTO future (id, email) VALUES (new.id, 'x'); END`)
	mustExec(t, f, `CREATE TABLE old (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`)
	f.waitDrain(t)
	wantReject(t, f, `ALTER TABLE old RENAME TO future`,
		"an existing trigger writes the new name")
	// A name no trigger writes renames fine.
	mustExec(t, f, `DROP TRIGGER tr`)
	mustExec(t, f, `ALTER TABLE old RENAME TO renamed`)
}
