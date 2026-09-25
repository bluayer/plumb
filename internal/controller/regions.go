package controller

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// RegionFactory builds the CSP adapters for a region given a client for its cluster.
// cmd/plumb-agent wires the AWS factory; the controller itself stays CSP-neutral.
type RegionFactory func(c client.Client, spec v1alpha1.RegionSpec) adapters.Region

// Registry caches region adapters by cluster.
type Registry struct {
	Local   client.Client
	Reader  client.Reader
	Scheme  *runtime.Scheme
	Factory RegionFactory

	mu      sync.Mutex
	regions map[string]cachedRegion
}

type cachedRegion struct {
	version string
	region  adapters.Region
}

// Key identifies a region's cluster: the local cluster or a kubeconfig secret.
func Key(policyNS string, spec v1alpha1.RegionSpec) string {
	if spec.KubeconfigSecretRef == nil {
		return "local/" + spec.Name
	}
	return fmt.Sprintf("secret/%s/%s/%s/%s", policyNS, spec.KubeconfigSecretRef.Name, secretKey(spec.KubeconfigSecretRef), spec.Name)
}

func secretKey(r *v1alpha1.SecretKeyRef) string {
	if r.Key == "" {
		return "kubeconfig"
	}
	return r.Key
}

// Region returns cached adapters, rebuilding them if the kubeconfig secret changed.
func (r *Registry) Region(ctx context.Context, policyNS string, spec v1alpha1.RegionSpec) (adapters.Region, error) {
	key := Key(policyNS, spec)
	version := "local"
	var cfgBytes []byte
	if ref := spec.KubeconfigSecretRef; ref != nil {
		sec := &corev1.Secret{}
		if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: policyNS, Name: ref.Name}, sec); err != nil {
			return adapters.Region{}, fmt.Errorf("region %s kubeconfig: %w", spec.Name, err)
		}
		cfgBytes = sec.Data[secretKey(ref)]
		if len(cfgBytes) == 0 {
			return adapters.Region{}, fmt.Errorf("region %s: secret %s has no key %s", spec.Name, ref.Name, secretKey(ref))
		}
		version = sec.ResourceVersion
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.regions == nil {
		r.regions = map[string]cachedRegion{}
	}
	if c, ok := r.regions[key]; ok && c.version == version {
		c.region.Name = spec.Name
		return c.region, nil
	}
	cl := r.Local
	if cfgBytes != nil {
		rc, err := clientcmd.RESTConfigFromKubeConfig(cfgBytes)
		if err != nil {
			return adapters.Region{}, fmt.Errorf("region %s kubeconfig: %w", spec.Name, err)
		}
		cl, err = client.New(rc, client.Options{Scheme: r.Scheme})
		if err != nil {
			return adapters.Region{}, fmt.Errorf("region %s client: %w", spec.Name, err)
		}
	}
	reg := r.Factory(cl, spec)
	reg.Name = spec.Name
	r.regions[key] = cachedRegion{version: version, region: reg}
	return reg, nil
}
