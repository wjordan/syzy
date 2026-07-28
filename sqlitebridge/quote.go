package sqlitebridge

import "strings"

// QuoteIdent wraps name in double quotes per SQLite identifier rules,
// escaping embedded quotes by doubling.
func QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
