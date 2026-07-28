package sqlitebridge

// ObjectExists reports whether sqlite_master has a row of the given
// type ("table" | "index" | "view" | "trigger") with the given name.
// Used by callers that need to gate DDL replay on the current state of
// the catalog without parsing the SQL.
func ObjectExists(conn *Conn, kind, name string) (bool, error) {
	stmt, _, err := conn.Prepare(`SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?`)
	if err != nil {
		return false, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, kind); err != nil {
		return false, err
	}
	if err := stmt.BindText(2, name); err != nil {
		return false, err
	}
	return stmt.Step()
}

// IsVirtualTable reports whether name is a module-backed virtual
// table. Shadow tables are ordinary tables and do not match.
func IsVirtualTable(conn *Conn, name string) (bool, error) {
	stmt, _, err := conn.Prepare(`SELECT 1 FROM pragma_table_list WHERE name = ? AND type = 'virtual'`)
	if err != nil {
		return false, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, name); err != nil {
		return false, err
	}
	return stmt.Step()
}

// ColumnExists reports whether pragma_table_info(table) yields a row
// whose name matches col. Cheaper and less fragile than parsing the
// table's CREATE statement.
func ColumnExists(conn *Conn, table, col string) (bool, error) {
	stmt, _, err := conn.Prepare(`SELECT 1 FROM pragma_table_info(?) WHERE name = ?`)
	if err != nil {
		return false, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		return false, err
	}
	if err := stmt.BindText(2, col); err != nil {
		return false, err
	}
	return stmt.Step()
}
