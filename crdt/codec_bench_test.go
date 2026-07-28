package crdt

import (
	"encoding/binary"
	"testing"
)

// Single-record changeset for the standard event(id BLOB PK, n TEXT)
// schema. Mirrors the workload other inner-loop benches use.
func benchInsertRecord() Record {
	idCol := ColumnID{0x01}
	nCol := ColumnID{0x02}
	pkID := []byte{0x42}
	return Insert{
		Table: TableID{0xAA},
		PK:    PKBlob(pkID),
		CL:    1,
		Image: []ColValue{
			{Column: idCol, TypeTag: ColBlob, Bytes: pkID},
			{Column: nCol, TypeTag: ColText, Bytes: []byte("hello")},
		},
	}
}

func benchStamp() Stamp {
	return Stamp{Clock: Clock{WallTime: 1_700_000_000_000, Logical: 0}, Origin: 7}
}

func benchCluster() ClusterID {
	var c ClusterID
	binary.BigEndian.PutUint64(c[:8], 0xCCCCCCCCCCCCCCCC)
	binary.BigEndian.PutUint64(c[8:], 0xCCCCCCCCCCCCCCCC)
	return c
}

func BenchmarkBuildInsert(b *testing.B) {
	b.ReportAllocs()
	rec := benchInsertRecord()
	stamp := benchStamp()
	cluster := benchCluster()
	dot := Dot{Origin: 7, Seq: 1}
	recs := []Record{rec}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Build(dot, stamp, nil, cluster, recs)
		if err != nil {
			b.Fatalf("Build: %v", err)
		}
	}
}

func BenchmarkDecodeInsert(b *testing.B) {
	b.ReportAllocs()
	cs, err := Build(Dot{Origin: 7, Seq: 1}, benchStamp(), nil, benchCluster(), []Record{benchInsertRecord()})
	if err != nil {
		b.Fatalf("Build: %v", err)
	}
	payload := cs.Encoded()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(payload); err != nil {
			b.Fatalf("Decode: %v", err)
		}
	}
}
