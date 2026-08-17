package miner

// isClaimCUPRConsistent deliberately fails open.
//
// poktroll v0.1.35 validates and settles compute_units_per_relay at the
// session START height. The legacy guard in this fork is called with the LIVE
// CUPR, so after a CUPR change it can terminally discard a claim that the chain
// would accept. Until the lifecycle callback is wired to the at-height query,
// the safe policy is to never terminally skip a claim based on a value from the
// wrong height. New relays are stamped with session-start CUPR by the relayer,
// so valid post-upgrade sessions still build consistently weighted SMSTs.
//
// A genuinely mixed-weight tree may therefore reach the chain and be rejected,
// costing transaction fees rather than forfeiting a potentially payable claim.
func isClaimCUPRConsistent(_, _, _ uint64) bool {
	return true
}
