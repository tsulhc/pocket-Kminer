package kleomedes24h

import (
	"os/exec"
	"testing"
)

func TestKleomedes24HourAnalysis(t *testing.T) {
	cmd := exec.Command("python3", "analyze.py")
	output, err := cmd.CombinedOutput()
	t.Logf("\n%s", output)
	if err != nil {
		t.Fatalf("analysis probe failed: %v", err)
	}
}
