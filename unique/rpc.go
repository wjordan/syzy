package unique

// rpcServiceName is the net/rpc service name the leaseholder registers
// and clients call ("Unique.Reserve"). Reserve is the only RPC: releases
// are never signalled — the leaseholder observes vacated values in the
// replicated rows and starts the release hold from that observation.
const rpcServiceName = "Unique"

// ReserveArgs is the Reserve RPC request. Gen is the leaseholder
// generation the client expects (learned from the lease); the server
// rejects a request whose Gen does not match its currently-held lease so
// a client that has not yet noticed a handover re-reads and retries.
type ReserveArgs struct {
	Gen    uint64
	Claims []Claim
}

// ReserveReply is the Reserve RPC response. NotLeader is set when the
// server is not the current leaseholder for Gen (draining, fenced, or
// never leader) — the client treats it as ErrUnavailable and re-resolves
// the lease. Conflict names the first contended claim when OK is false.
type ReserveReply struct {
	OK        bool
	NotLeader bool
	Conflict  *Claim
}
