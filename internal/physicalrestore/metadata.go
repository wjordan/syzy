package physicalrestore

import (
	"encoding/binary"
	"fmt"

	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/publisher"
)

// ParentAppTXID returns metadata.db's app-stream consistency pin. A missing
// value is reported as zero.
func ParentAppTXID(path string) (uint64, error) {
	store, err := metadata.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open metadata: %w", err)
	}
	defer store.Close()
	v, ok, err := store.GetMeta(publisher.MetaKeyParentAppTXID)
	if err != nil {
		return 0, fmt.Errorf("read parent_app_txid: %w", err)
	}
	if !ok {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("parent_app_txid has width %d, want 8", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}
