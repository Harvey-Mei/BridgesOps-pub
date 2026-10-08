//go:build release

package web

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

var indexAssetPattern = regexp.MustCompile(`(?:src|href)=["'](/assets/[^"']+)["']`)

func TestReleaseIncludesFrontendDistribution(t *testing.T) {
	if !HasFrontend() {
		t.Fatal("release build is missing embedded frontend/index.html")
	}

	dist := DistFS()
	index, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		t.Fatalf("read embedded frontend/index.html: %v", err)
	}
	if len(index) == 0 {
		t.Fatal("embedded frontend/index.html is empty")
	}
	if !strings.Contains(strings.ToLower(string(index)), "<!doctype html") {
		t.Fatal("embedded frontend/index.html is not a built HTML document")
	}

	matches := indexAssetPattern.FindAllSubmatch(index, -1)
	if len(matches) == 0 {
		t.Fatal("embedded frontend/index.html does not reference any built assets")
	}
	for _, match := range matches {
		asset := strings.TrimPrefix(string(match[1]), "/")
		info, err := fs.Stat(dist, asset)
		if err != nil {
			t.Errorf("embedded asset %q referenced by index.html is unavailable: %v", asset, err)
			continue
		}
		if !info.Mode().IsRegular() || info.Size() == 0 {
			t.Errorf("embedded asset %q is not a non-empty regular file", asset)
		}
	}
}
