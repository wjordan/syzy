package crdt

import "testing"

func TestDeps_Satisfied(t *testing.T) {
	d := Deps{SchemaChain: 5}

	cases := []struct {
		name string
		have map[ChainID]Seq
		want bool
	}{
		{"missing chain", nil, false},
		{"below", map[ChainID]Seq{SchemaChain: 4}, false},
		{"equal", map[ChainID]Seq{SchemaChain: 5}, true},
		{"above", map[ChainID]Seq{SchemaChain: 7}, true},
		{"extra chains ignored", map[ChainID]Seq{SchemaChain: 5, ChainID(1): 99}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := d.Satisfied(tc.have); got != tc.want {
				t.Errorf("Satisfied = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeps_NilEmptyEquivalent(t *testing.T) {
	var nilDeps Deps
	emptyDeps := Deps{}
	have := map[ChainID]Seq{SchemaChain: 0}
	if !nilDeps.Satisfied(have) {
		t.Error("nil deps should be satisfied")
	}
	if !emptyDeps.Satisfied(have) {
		t.Error("empty deps should be satisfied")
	}
	if !nilDeps.Equal(emptyDeps) {
		t.Error("nil and empty deps should be equal")
	}
}

func TestDeps_SetMonotonic(t *testing.T) {
	d := Deps{}
	d.Set(SchemaChain, 5)
	d.Set(SchemaChain, 5) // re-asserting same is fine
	d.Set(SchemaChain, 7) // raising is fine

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when lowering")
		}
	}()
	d.Set(SchemaChain, 3) // must panic
}

func TestDeps_Clone(t *testing.T) {
	d := Deps{SchemaChain: 5, ChainID(1): 9}
	c := d.Clone()
	if !c.Equal(d) {
		t.Error("clone should equal original")
	}
	c[SchemaChain] = 99
	if d[SchemaChain] == 99 {
		t.Error("clone should be independent")
	}
}
