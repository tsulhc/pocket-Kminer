//go:build test

package miner

import (
	"bytes"
	"context"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"

	prooftypes "github.com/pokt-network/poktroll/x/proof/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
)

func TestMarkReconciledClaimFound_RepairsTerminalClaimError(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestSessionStore(t)
	defer store.Close()

	coord := NewSessionCoordinator(testLogger(), store, SMSTRecoveryConfig{SupplierAddress: "pokt1supplier"})
	defer coord.Close()
	lifecycle := &SessionLifecycleManager{activeSessions: xsync.NewMap[string, *SessionSnapshot]()}
	mgr := &SupplierManager{
		logger:    testLogger(),
		suppliers: make(map[string]*SupplierState),
	}
	mgr.suppliers["pokt1supplier"] = &SupplierState{
		OperatorAddr:       "pokt1supplier",
		SessionStore:       store,
		SessionCoordinator: coord,
		LifecycleManager:   lifecycle,
	}

	snapshot := &SessionSnapshot{
		SessionID:               "sess-claim-heal",
		SupplierOperatorAddress: "pokt1supplier",
		ServiceID:               "svc",
		ApplicationAddress:      "pokt1app",
		SessionStartHeight:      100,
		SessionEndHeight:        110,
		State:                   SessionStateClaimTxError,
	}
	require.NoError(t, store.Save(ctx, snapshot))

	msg := prooftypes.MsgCreateClaim{
		SupplierOperatorAddress: "pokt1supplier",
		SessionHeader: &sessiontypes.SessionHeader{
			ApplicationAddress:      "pokt1app",
			ServiceId:               "svc",
			SessionId:               snapshot.SessionID,
			SessionStartBlockHeight: snapshot.SessionStartHeight,
			SessionEndBlockHeight:   snapshot.SessionEndHeight,
		},
		RootHash: bytes.Repeat([]byte{0x42}, SMSTRootLen),
	}
	msgBytes, err := msg.Marshal()
	require.NoError(t, err)

	mgr.markReconciledClaimFound(ctx, "pokt1supplier", rebroadcastEntry{MsgBytes: msgBytes, TxHash: "claim-tx"})

	got, err := store.Get(ctx, snapshot.SessionID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, SessionStateClaimed, got.State)
	require.Equal(t, "claim-tx", got.ClaimTxHash)
	require.Equal(t, bytes.Repeat([]byte{0x42}, SMSTRootLen), got.ClaimedRootHash)
	tracked, ok := lifecycle.activeSessions.Load(snapshot.SessionID)
	require.True(t, ok)
	require.Equal(t, SessionStateClaimed, tracked.State)
}

func TestMarkReconciledClaimFound_RepairsClaimedSessionMissingRoot(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestSessionStore(t)
	defer store.Close()

	coord := NewSessionCoordinator(testLogger(), store, SMSTRecoveryConfig{SupplierAddress: "pokt1supplier"})
	defer coord.Close()
	mgr := &SupplierManager{
		logger:    testLogger(),
		suppliers: make(map[string]*SupplierState),
	}
	mgr.suppliers["pokt1supplier"] = &SupplierState{
		OperatorAddr:       "pokt1supplier",
		SessionStore:       store,
		SessionCoordinator: coord,
	}

	snapshot := &SessionSnapshot{
		SessionID:               "sess-claimed-missing-root",
		SupplierOperatorAddress: "pokt1supplier",
		ServiceID:               "svc",
		ApplicationAddress:      "pokt1app",
		SessionStartHeight:      100,
		SessionEndHeight:        110,
		State:                   SessionStateClaimed,
		ClaimedRootHash:         nil,
		ClaimTxHash:             "",
	}
	require.NoError(t, store.Save(ctx, snapshot))

	rootHash := bytes.Repeat([]byte{0x24}, SMSTRootLen)
	msg := prooftypes.MsgCreateClaim{
		SupplierOperatorAddress: "pokt1supplier",
		SessionHeader: &sessiontypes.SessionHeader{
			ApplicationAddress:      "pokt1app",
			ServiceId:               "svc",
			SessionId:               snapshot.SessionID,
			SessionStartBlockHeight: snapshot.SessionStartHeight,
			SessionEndBlockHeight:   snapshot.SessionEndHeight,
		},
		RootHash: rootHash,
	}
	msgBytes, err := msg.Marshal()
	require.NoError(t, err)

	mgr.markReconciledClaimFound(ctx, "pokt1supplier", rebroadcastEntry{MsgBytes: msgBytes, OrigTxHash: "orig-claim-tx"})

	got, err := store.Get(ctx, snapshot.SessionID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, SessionStateClaimed, got.State)
	require.Equal(t, "orig-claim-tx", got.ClaimTxHash)
	require.Equal(t, rootHash, got.ClaimedRootHash)
}

func TestMarkReconciledProofFound_RepairsTerminalProofError(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestSessionStore(t)
	defer store.Close()

	coord := NewSessionCoordinator(testLogger(), store, SMSTRecoveryConfig{SupplierAddress: "pokt1supplier"})
	defer coord.Close()
	mgr := &SupplierManager{
		logger:    testLogger(),
		suppliers: make(map[string]*SupplierState),
	}
	mgr.suppliers["pokt1supplier"] = &SupplierState{
		OperatorAddr:       "pokt1supplier",
		SessionStore:       store,
		SessionCoordinator: coord,
		LifecycleCallback:  nil,
		LifecycleManager:   nil,
		SupplierClient:     nil,
		SMSTManager:        nil,
		Consumer:           nil,
	}

	snapshot := &SessionSnapshot{
		SessionID:               "sess-proof-heal",
		SupplierOperatorAddress: "pokt1supplier",
		ServiceID:               "svc",
		ApplicationAddress:      "pokt1app",
		SessionStartHeight:      100,
		SessionEndHeight:        110,
		State:                   SessionStateProofTxError,
	}
	require.NoError(t, store.Save(ctx, snapshot))

	mgr.markReconciledProofFound(ctx, "pokt1supplier", snapshot.SessionID, rebroadcastEntry{TxHash: "proof-tx"})

	got, err := store.Get(ctx, snapshot.SessionID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, SessionStateProved, got.State)
	require.Equal(t, "proof-tx", got.ProofTxHash)
}
