package query

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsEntityNotFound reports only a definitive gRPC NotFound response.
// Transport and node failures must remain fail-open for terminal decisions.
func IsEntityNotFound(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}
