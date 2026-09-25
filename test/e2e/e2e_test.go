//go:build e2e

// Package e2e runs Plumb against real API servers and the real kube-scheduler on
// disposable clusters (kind, minikube or kwokctl). GPU nodes are KWOK fake nodes, so no
// GPUs or cloud accounts are needed. See hack/e2e/up.sh.
//
//	PLUMB_E2E_KUBECONFIG         home region cluster (required)
//	PLUMB_E2E_REMOTE_KUBECONFIG  second region cluster (optional; multi-region tests skip without it)
//	PLUMB_E2E_ALLOW_ANY_CLUSTER  set to 1 to run against a context not named kind-*, kwok-*, minikube or plumb-e2e*
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
)

// cluster is one region's API server.
type cluster struct {
	name       string
	kubeconfig []byte
	cfg        *rest.Config
	c          client.Client
}

var (
	scheme       = runtime.NewScheme()
	home, remote *cluster
)

func init() {
	must(clientgoscheme.AddToScheme(scheme))
	must(v1alpha1.AddToScheme(scheme))
	must(apiextensionsv1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(k.NodePoolGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(k.NodePoolListGVK, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(k.NodeClaimGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(k.NodeClaimListGVK, &unstructured.UnstructuredList{})
}

var disposableContext = regexp.MustCompile(`^(kind-|kwok-|minikube$|plumb-e2e)`)

func TestMain(m *testing.M) {
	ctrllog.SetLogger(zap.New(zap.WriteTo(io.Discard)))
	var err error
	home, err = connect("home", os.Getenv("PLUMB_E2E_KUBECONFIG"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		fmt.Fprintln(os.Stderr, "e2e: create clusters with `make e2e-up` (PROVIDER=kind|minikube|kwok)")
		os.Exit(1)
	}
	if p := os.Getenv("PLUMB_E2E_REMOTE_KUBECONFIG"); p != "" {
		if remote, err = connect("remote", p); err != nil {
			fmt.Fprintln(os.Stderr, "e2e:", err)
			os.Exit(1)
		}
	}
	for _, cl := range []*cluster{home, remote} {
		if cl == nil {
			continue
		}
		if err := installCRDs(cl); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: installing CRDs in %s: %v\n", cl.name, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func connect(name, path string) (*cluster, error) {
	if path == "" {
		return nil, fmt.Errorf("PLUMB_E2E_KUBECONFIG is not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	kc, err := clientcmd.Load(raw)
	if err != nil {
		return nil, err
	}
	if !disposableContext.MatchString(kc.CurrentContext) && os.Getenv("PLUMB_E2E_ALLOW_ANY_CLUSTER") != "1" {
		return nil, fmt.Errorf("%s: context %q does not look disposable (the suite creates nodes and CRDs); "+
			"set PLUMB_E2E_ALLOW_ANY_CLUSTER=1 to override", path, kc.CurrentContext)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, err
	}
	cfg.QPS, cfg.Burst = 50, 100
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	return &cluster{name: name, kubeconfig: raw, cfg: cfg, c: c}, nil
}

// installCRDs applies the Plumb and Karpenter CRDs and waits for them to be served.
func installCRDs(cl *cluster) error {
	files, _ := filepath.Glob("../../config/crd/*.yaml")
	extra, _ := filepath.Glob("testdata/crds/*.yaml")
	files = append(files, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
		for {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := dec.Decode(crd); err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("%s: %w", f, err)
			}
			if crd.Name == "" {
				continue
			}
			existing := &apiextensionsv1.CustomResourceDefinition{}
			err := cl.c.Get(ctx, client.ObjectKey{Name: crd.Name}, existing)
			switch {
			case apierrors.IsNotFound(err):
				err = cl.c.Create(ctx, crd)
			case err == nil:
				crd.ResourceVersion = existing.ResourceVersion
				err = cl.c.Update(ctx, crd)
			}
			if err != nil {
				return fmt.Errorf("%s: %w", crd.Name, err)
			}
			if err := waitFor(ctx, 250*time.Millisecond, func() (bool, error) {
				got := &apiextensionsv1.CustomResourceDefinition{}
				if err := cl.c.Get(ctx, client.ObjectKey{Name: crd.Name}, got); err != nil {
					return false, err
				}
				for _, c := range got.Status.Conditions {
					if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				return fmt.Errorf("%s not established: %w", crd.Name, err)
			}
		}
	}
	// A fresh client picks up the new REST mappings.
	c, err := client.New(cl.cfg, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	cl.c = c
	return nil
}

// waitFor polls cond until it returns true, an error, or ctx ends.
func waitFor(ctx context.Context, every time.Duration, cond func() (bool, error)) error {
	for {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
