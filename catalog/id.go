// Package catalog contains replication-catalog identities and canonical key
// encoding. Database introspection and DDL rendering belong to engine modules.
package catalog

import (
	"crypto/rand"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

func AllocTableID() crdt.TableID {
	var id crdt.TableID
	fillNonzero(id[:])
	return id
}

func AllocColumnID() crdt.ColumnID {
	var id crdt.ColumnID
	fillNonzero(id[:])
	return id
}

func AllocKeyID() crdt.KeyID {
	var id crdt.KeyID
	fillNonzero(id[:])
	return id
}

func fillNonzero(id []byte) {
	for {
		if _, err := rand.Read(id); err != nil {
			panic(fmt.Errorf("catalog: allocate id: %w", err))
		}
		for _, b := range id {
			if b != 0 {
				return
			}
		}
	}
}
