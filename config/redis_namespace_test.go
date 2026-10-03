//go:build test

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRedisNamespaceValidate covers the base-prefix guard: a prefix that is not
// one flat segment would reach every SCAN pattern the KeyBuilder builds, and
// one of those feeds a delete.
//
// The retired per-family prefixes used to be guarded here too, as struct
// fields with a tailored hard error. Those fields are deleted -- a field per
// retired key is config that configures nothing -- so a config that still sets
// one is reported by the unknown-keys pass with its retiredKeys sentence
// instead. That YAML-door coverage lives in
// cmd/miner_validate_retired_keys_test.go (miner `validate`) and
// relayer/config_unknown_keys_test.go (relayer load warnings), which drive the
// real doors rather than a struct literal.
func TestRedisNamespaceValidate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		ns      RedisNamespaceConfig
		wantErr string
	}{
		{
			name: "empty namespace is the default",
			ns:   RedisNamespaceConfig{},
		},
		{
			name: "a custom base prefix is the supported knob",
			ns:   RedisNamespaceConfig{BasePrefix: "prod"},
		},
		{
			name:    "a glob in the base prefix would end up inside every SCAN pattern",
			ns:      RedisNamespaceConfig{BasePrefix: "ha:*"},
			wantErr: "single namespace segment",
		},
		{
			name:    "and so would a space",
			ns:      RedisNamespaceConfig{BasePrefix: "two words"},
			wantErr: "single namespace segment",
		},
		{
			name:    "a bracket is a glob character too",
			ns:      RedisNamespaceConfig{BasePrefix: "ha[12]"},
			wantErr: "single namespace segment",
		},
		// The two cases that caught the first version of this rule. It was
		// written as `[*?\[\]\\s]`, which in a Go raw string is the class
		// {* ? [ ] \ s} -- the LETTER s, and no whitespace at all. So it locked
		// out every base containing an "s" and let a space straight through,
		// which is the opposite of the rule on both counts. The cases above did
		// not catch it: "two words" errored on the "s" of "words" and "ha:*" on
		// the star, so both passed for the wrong reason.
		{
			name: "a base containing 's' is ordinary text and must be accepted",
			ns:   RedisNamespaceConfig{BasePrefix: "prod-us"},
		},
		{
			name: "and so is one that is mostly s",
			ns:   RedisNamespaceConfig{BasePrefix: "suppliers"},
		},
		{
			name:    "a bare space, with no other suspicious character",
			ns:      RedisNamespaceConfig{BasePrefix: "ha prod"},
			wantErr: "single namespace segment",
		},
		{
			name:    "a tab, likewise",
			ns:      RedisNamespaceConfig{BasePrefix: "ha\tprod"},
			wantErr: "single namespace segment",
		},
		// A colon does NOT "only add a segment". The base prefix is one
		// namespace segment; a colon turns it into a hierarchy the key layout
		// does not model, and two fleets nested that way are not disjoint --
		// base "ha" scans "ha:*", which matches every key of a fleet based at
		// "ha:prod", and that pattern feeds `redis flush --all`.
		{
			name:    "a colon nests one fleet inside another's scan pattern",
			ns:      RedisNamespaceConfig{BasePrefix: "ha:prod"},
			wantErr: "single namespace segment",
		},
		{
			// The empty segment this package promises cannot exist:
			// "prod:" builds "prod::cache:application:x".
			name:    "a trailing colon produces the empty segment",
			ns:      RedisNamespaceConfig{BasePrefix: "prod:"},
			wantErr: "single namespace segment",
		},
		{
			// Accepted since 2026-08-28. A dot is not a Redis namespace
			// separator, carries no glob, and a fleet based at "pocket.ha" is
			// disjoint from every other -- so rejecting it only meant an existing
			// fleet would refuse to start after upgrading, with the sole remedy a
			// rename that relocates the whole keyspace including the WAL the
			// miner is consuming. The rule rejects what Redis treats as
			// structure, not everything unfamiliar (ruling: Jorge, 2026-08-28).
			name: "a dot is not a separator, so it is accepted",
			ns:   RedisNamespaceConfig{BasePrefix: "pocket.ha"},
		},
		{
			// An omitted base_prefix is VALID and means the default. Validate
			// runs before WithDefaults, so checking the raw field would reject
			// every config that simply does not set it.
			name: "an omitted base_prefix falls to the default and must start",
			ns:   RedisNamespaceConfig{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ns.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err, "this config moves no keys and must start")
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr,
				"the error must name the offending field: an operator has to know WHICH line to remove")
		})
	}
}

// TestRedisNamespaceWithDefaultsOnlyFillsTheBase pins that defaulting fills
// only the base prefix. Every other segment is a constant in transport/redis,
// so a partial namespace cannot produce an empty segment.
func TestRedisNamespaceWithDefaultsOnlyFillsTheBase(t *testing.T) {
	got := RedisNamespaceConfig{}.WithDefaults()

	require.Equal(t, "ha", got.BasePrefix)
	require.Equal(t, RedisNamespaceConfig{BasePrefix: "ha"}, got,
		"WithDefaults must fill nothing but the base: anything else here "+
			"would make Validate reject a config the operator never wrote")
}
