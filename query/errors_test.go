//go:build test

package query

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsEntityNotFound_DefinitiveAbsence(t *testing.T) {
	notFound := status.Error(codes.NotFound, "claim not found")
	for _, err := range []error{
		notFound,
		fmt.Errorf("query claim: %w", notFound),
		fmt.Errorf("outer: %w", fmt.Errorf("query claim: %w", notFound)),
	} {
		require.True(t, IsEntityNotFound(err))
	}
}

func TestIsEntityNotFound_TransientFailuresFailOpen(t *testing.T) {
	for _, err := range []error{
		status.Error(codes.Unknown, "header not found"),
		status.Error(codes.Internal, "header not found"),
		status.Error(codes.Unavailable, "peer not found"),
		fmt.Errorf("query claim: %w", status.Error(codes.Unknown, "header not found")),
		errors.New("dial tcp: route not found"),
		errors.New("claim not found for session"),
		status.Error(codes.DeadlineExceeded, "timeout"),
	} {
		require.False(t, IsEntityNotFound(err))
	}
}

func TestIsEntityNotFound_Nil(t *testing.T) {
	require.False(t, IsEntityNotFound(nil))
}
