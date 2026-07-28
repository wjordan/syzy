package crdt

import "maps"

// ChainID identifies one of (potentially several) totally-ordered
// CAS chains a Changeset can causally depend on. v1 uses one chain
// only:
//
//   - SchemaChain (= 0): the schema log.
//
// The shape allows v2 chains (FK targets, application-defined causal
// barriers, blob_patch base rows) at no encoding cost.
type ChainID uint16

// Reserved chain IDs.
const (
	SchemaChain ChainID = 0
)

// Deps is the one-hop minimal causal dependency set carried by a
// Changeset. Per Lloyd et al. COPS (SOSP 2011) / Eiger (NSDI 2013):
// each Changeset carries the transitive reduction of its causal
// dependencies — entries not already implied by other carried deps or
// by the receiver's frontier. The receiver checks every dep before
// applying — invariant (3) in CRDT.md (causal closure of Deps).
//
// At apply time the receiver enforces the Almeida δ-state
// causal-merging condition (Almeida et al. 2016, Def. 6): a delta is
// applied only when every dependency it references is already
// satisfied locally. The schema chain is the one cross-chain Dep; the
// broker gates on Deps[SchemaChain] against local schema_seq.
//
// nil and empty Deps are equivalent: no dependencies.
type Deps map[ChainID]Seq

// Set assigns d[chain] = seq. Panics on a Seq lower than an existing
// entry (deps are intended to be the maximum required position per
// chain; lowering would violate the one-hop minimal property).
func (d Deps) Set(chain ChainID, seq Seq) {
	if existing, ok := d[chain]; ok && seq < existing {
		panic("crdt: Deps.Set lowers an existing dependency")
	}
	d[chain] = seq
}

// Satisfied reports whether every dep in d is covered by have:
// have[chain] >= d[chain] for every chain in d.
//
// have may safely be nil; missing chains are treated as Seq(0).
func (d Deps) Satisfied(have map[ChainID]Seq) bool {
	for chain, need := range d {
		if have[chain] < need {
			return false
		}
	}
	return true
}

// Equal reports whether d and other carry the same chain→seq mappings.
func (d Deps) Equal(other Deps) bool { return maps.Equal(d, other) }

// Clone returns a deep copy. Useful before passing to a caller that may
// mutate.
func (d Deps) Clone() Deps { return maps.Clone(d) }
