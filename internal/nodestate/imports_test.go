package nodestate

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// Guards the engine seam: nodestate is shared replication core that a
// second engine builds on directly, so it must stay free of engine
// adapter imports. The table shape recovery needs arrives through
// crdt.CellTable / the CellTables resolver instead.
func TestNoAdapterImports(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range pkgs {
		for file, f := range pkg.Files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			for _, imp := range f.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				if strings.Contains(path, "sqlitecatalog") || strings.Contains(path, "sqlitebridge") {
					t.Errorf("%s imports engine adapter package %s", file, path)
				}
			}
		}
	}
}
