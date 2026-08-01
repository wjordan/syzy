package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/unique"
)

// EnumerateCoordinated reads this node's replica and returns the coordinated
// claims its rows currently back, plus every active coordinated key identity.
//
// This is the leaseholder's derivation source, and the reason a reservation
// survives a leaseholder restart: the taken-set is not a durable ledger the
// registry maintains, it is re-derived from the rows themselves. A value is
// taken because a row holds it, and it re-enters the free pool only once an
// enumeration observes the row gone (then waits out the release hold). Keys
// with no participating rows are still reported, because an absent key
// identity makes the leaseholder refuse to serve that key rather than
// wrongly granting values under it.
//
// Called from the leaseholder's maintenance tick, off the orchestrator
// goroutine: it reads the decoupled coordIndex and its own connection, never
// the catalog or the apply session.
func (e *Engine) EnumerateCoordinated(ctx context.Context) (unique.Snapshot, error) {
	var snap unique.Snapshot
	if e.cat == nil || e.cat.coordIdx == nil || e.enumConn == nil {
		return snap, nil
	}
	for _, ct := range e.cat.coordIdx.snapshot() {
		for kid, members := range ct.keys {
			snap.Keys = append(snap.Keys, unique.KeyRef{Table: ct.tid, Key: kid})
			claims, err := enumerateKeyClaims(ctx, e.enumConn, ct, kid, members)
			if err != nil {
				return unique.Snapshot{}, err
			}
			snap.Claims = append(snap.Claims, claims...)
		}
	}
	return snap, nil
}

// enumerateKeyClaims reads every row participating in one coordinated key and
// encodes its claim. Values are read as text under the same canonical GUC
// pins capture uses, then run through the same canonical encoder, so an
// enumerated claim is byte-identical to the one its writer reserved — which
// is the whole point, since byte equality is what identifies a value.
func enumerateKeyClaims(ctx context.Context, conn *pgx.Conn, ct coordTable, kid [16]byte, members []coordCol) ([]unique.Claim, error) {
	sel := make([]string, 0, len(members)+len(ct.pk))
	for _, c := range members {
		sel = append(sel, quoteIdent(c.name)+"::text")
	}
	for _, c := range ct.pk {
		sel = append(sel, quoteIdent(c.name)+"::text")
	}
	// A coordinated key is NOT NULL by construction, so this filter is
	// defensive: it keeps a column that somehow lost its NOT NULL out of the
	// taken-set instead of failing the whole enumeration on a NULL member.
	where := make([]string, len(members))
	for i, c := range members {
		where[i] = quoteIdent(c.name) + " IS NOT NULL"
	}
	sql := "SELECT " + strings.Join(sel, ", ") +
		" FROM " + quoteIdent(appliedSchema) + "." + quoteIdent(ct.name) +
		" WHERE " + strings.Join(where, " AND ")

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("enumerate coordinated %s: %w", ct.name, err)
	}
	defer rows.Close()

	var out []unique.Claim
	for rows.Next() {
		vals := make([]*string, len(members)+len(ct.pk))
		dest := make([]any, len(vals))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("enumerate coordinated %s: %w", ct.name, err)
		}
		value, err := encodeCanonical(members, vals[:len(members)])
		if err != nil {
			return nil, fmt.Errorf("enumerate coordinated %s: key: %w", ct.name, err)
		}
		owner, err := encodeCanonical(ct.pk, vals[len(members):])
		if err != nil {
			return nil, fmt.Errorf("enumerate coordinated %s: pk: %w", ct.name, err)
		}
		out = append(out, unique.Claim{
			Table: ct.tid, Key: kid, Value: value, Owner: owner,
		})
	}
	return out, rows.Err()
}
