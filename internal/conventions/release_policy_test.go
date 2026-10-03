package conventions

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	releasePublisherCondition = "github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v'))"
	releaseAssetsCondition    = "startsWith(github.ref, 'refs/tags/v')"
)

type releaseWorkflowDocument struct {
	On   map[string]any                `yaml:"on"`
	Jobs map[string]releaseWorkflowJob `yaml:"jobs"`
}

type releaseWorkflowJob struct {
	If    string                `yaml:"if"`
	Steps []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflowStep struct {
	Uses string         `yaml:"uses"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
}

// TestReleaseWorkflowRequiresExplicitPublicationEvents protects the release
// contract: branch pushes may build/test through CI but cannot publish images,
// manifests or release assets. Every publishing job is guarded independently
// so an accidental trigger broadening cannot make a main/dev push publish.
func TestReleaseWorkflowRequiresExplicitPublicationEvents(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}

	var workflow releaseWorkflowDocument
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse release workflow: %v", err)
	}

	if len(workflow.On) != 2 {
		t.Fatalf("release workflow triggers = %v, want only workflow_dispatch and version-tag push", workflow.On)
	}
	if _, ok := workflow.On["workflow_dispatch"]; !ok {
		t.Error("release workflow must allow explicit workflow_dispatch")
	}
	pushValue, ok := workflow.On["push"]
	if !ok {
		t.Fatal("release workflow must allow explicit version-tag pushes")
	}
	push, ok := pushValue.(map[string]any)
	if !ok {
		t.Fatalf("push trigger has type %T, want mapping", pushValue)
	}
	if len(push) != 1 {
		t.Fatalf("push trigger = %v, want only a version-tag filter and no branch filter", push)
	}
	tagsValue, ok := push["tags"]
	if !ok {
		t.Fatalf("push trigger = %v, want tags filter", push)
	}
	tags, ok := tagsValue.([]any)
	if !ok || len(tags) != 1 || tags[0] != "v*" {
		t.Fatalf("push tags = %v, want exactly [v*]", tagsValue)
	}

	var publisherJobs []string
	for name, job := range workflow.Jobs {
		if !releaseJobPublishes(job) {
			continue
		}
		publisherJobs = append(publisherJobs, name)
		wantCondition := releasePublisherCondition
		if name == "release" {
			wantCondition = releaseAssetsCondition
		}
		if normalizeWorkflowExpression(job.If) != wantCondition {
			t.Errorf("publishing job %q condition = %q, want %q", name, job.If, wantCondition)
		}
	}

	gotJobs := make(map[string]bool, len(publisherJobs))
	for _, name := range publisherJobs {
		gotJobs[name] = true
	}
	for _, want := range []string{"build-amd64", "build-arm64", "create-manifest", "release"} {
		if !gotJobs[want] {
			t.Errorf("publishing job %q was not detected; update the publisher inventory and guard", want)
		}
	}
	if len(gotJobs) != 4 {
		t.Errorf("detected publishing jobs = %v, want only build-amd64, build-arm64, create-manifest, and release", publisherJobs)
	}
}

func releaseJobPublishes(job releaseWorkflowJob) bool {
	for _, step := range job.Steps {
		uses := strings.ToLower(step.Uses)
		if strings.Contains(uses, "docker/build-push-action") {
			// Treat an absent or expression-valued push option conservatively as a
			// publisher. An explicit false is the only non-publishing declaration.
			if value, ok := step.With["push"]; !ok || !strings.EqualFold(fmt.Sprint(value), "false") {
				return true
			}
		}
		if strings.Contains(uses, "softprops/action-gh-release") {
			return true
		}

		run := strings.ToLower(step.Run)
		for _, command := range []string{
			"docker push",
			"docker manifest push",
			"docker buildx imagetools create",
			"oras push",
			"gh release create",
			"gh release upload",
		} {
			if strings.Contains(run, command) {
				return true
			}
		}
	}
	return false
}

func normalizeWorkflowExpression(expression string) string {
	return strings.Join(strings.Fields(expression), " ")
}
