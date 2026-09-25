package adapters_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cspImports are import path prefixes that belong to one CSP.
var cspImports = map[string][]string{
	"aws": {"github.com/aws/", "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws"},
}

// compositionRoots wire CSP adapters into running code and may import them: the
// binaries and the e2e suite, which exercises the AWS adapter against real API servers.
var compositionRoots = []string{"cmd/", "test/e2e/"}

// TestCSPImportBoundary enforces that CSP SDKs and CSP adapters are imported only from
// internal/adapters/<csp>/ and the composition roots.
func TestCSPImportBoundary(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") || d.Name() == "decision-service" || d.Name() == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		for _, root := range compositionRoots {
			if strings.HasPrefix(rel, root) {
				return nil
			}
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			for csp, prefixes := range cspImports {
				allowed := strings.HasPrefix(rel, filepath.Join("internal", "adapters", csp)+"/")
				for _, pre := range prefixes {
					if strings.HasPrefix(p, pre) && !allowed {
						t.Errorf("%s imports %s; CSP code must stay under internal/adapters/%s", rel, p, csp)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
