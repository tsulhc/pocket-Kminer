package rings

import (
	"os/exec"
	"strings"
	"testing"
)

// TestEthereumSecp256k1BuildTagIsRefused guards the guard.
//
// rings/ethereum_backend_unsupported.go stops `-tags ethereum_secp256k1` from
// building, because that tag silently forks ring signature validity for ~1
// challenge in 256. Nothing else would notice if that file were deleted or its
// build constraint typo'd — the normal build would stay green and the tag would
// quietly start working again. So actually try it.
func TestEthereumSecp256k1BuildTagIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("invokes the go toolchain; skipped under -short")
	}

	cmd := exec.Command("go", "build", "-o", "/dev/null", "-tags", "ethereum_secp256k1", ".")
	out, err := cmd.CombinedOutput()

	if err == nil {
		t.Fatal("`go build -tags ethereum_secp256k1 ./rings/` SUCCEEDED. It must not: " +
			"that tag selects a go-dleq backend whose HashToScalar disagrees with the " +
			"default for ~1 in 256 challenges, so a binary built with it signs relays " +
			"the rest of the fleet rejects. Restore the compile-time refusal in " +
			"rings/ethereum_backend_unsupported.go.")
	}

	// Fail for the RIGHT reason: any build error would satisfy err != nil,
	// including an unrelated one that would mask the guard going missing.
	if !strings.Contains(string(out), "ethereum_secp256k1 build tag is not supported") {
		t.Fatalf("the build failed, but not with the refusal message, so this test is no "+
			"longer proving the guard works. Output:\n%s", out)
	}
}

// TestDefaultBuildIsUnaffected pins the other half: the refusal must not leak
// into a normal build.
func TestDefaultBuildIsUnaffected(t *testing.T) {
	if testing.Short() {
		t.Skip("invokes the go toolchain; skipped under -short")
	}

	cmd := exec.Command("go", "build", "-o", "/dev/null", ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the default build of ./rings/ must always work, but it failed:\n%s", out)
	}
}
