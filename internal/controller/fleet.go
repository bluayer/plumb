/*
Copyright The Plumb Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	cpv1alpha1 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha1"
	"sigs.k8s.io/cluster-inventory-api/pkg/access"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// Leader election timing, the client-go and kube-controller-manager defaults.
const (
	LeaseDuration = 15 * time.Second
	RenewDeadline = 10 * time.Second
	RetryPeriod   = 2 * time.Second
)

// Fleet connects to every member cluster: this one, plus one per ClusterProfile
// (sigs.k8s.io/cluster-inventory-api) in the local namespace, credentials coming from the
// access providers (KEP-5339 exec plugins). Adding or removing a ClusterProfile adds or
// removes a member at runtime.
type Fleet struct {
	Self      string
	Namespace string // plumb's namespace in every member: ClusterProfiles here, the hub Lease in all
	Local     cluster.Cluster
	Access    *access.Config
	// NewCluster builds a member's client and cache; the Fleet starts and stops it.
	NewCluster func(*rest.Config) (cluster.Cluster, error)
	// Changed is called when any member's AdaptivePolicy changes.
	Changed func()

	mu       sync.Mutex
	members  map[string]*member // connected
	profiles map[string]bool    // every ClusterProfile, connected or not: all of them vote
}

type member struct {
	cl     cluster.Cluster
	leases coordinationclient.LeasesGetter
	access string // the ClusterProfile access the connection was built from
	cancel context.CancelFunc
}

// +kubebuilder:rbac:groups=multicluster.x-k8s.io,resources=clusterprofiles,verbs=get;list;watch

// SetupWithManager watches ClusterProfiles and connects the local cluster.
func (f *Fleet) SetupWithManager(mgr ctrl.Manager) error {
	leases, err := coordinationclient.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	f.members = map[string]*member{f.Self: {cl: f.Local, leases: leases}}
	f.profiles = map[string]bool{}
	return ctrl.NewControllerManagedBy(mgr).For(&cpv1alpha1.ClusterProfile{}).Named("clusterprofile").Complete(f)
}

// Reconcile (re)connects a member when its ClusterProfile's access changes.
func (f *Fleet) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Namespace != f.Namespace || req.Name == f.Self {
		return ctrl.Result{}, nil
	}
	cp := &cpv1alpha1.ClusterProfile{}
	if err := f.Local.GetClient().Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			f.drop(req.Name)
			f.mu.Lock()
			delete(f.profiles, req.Name)
			f.mu.Unlock()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	f.mu.Lock()
	f.profiles[req.Name] = true
	f.mu.Unlock()
	raw, _ := json.Marshal(cp.Status.AccessProviders)
	f.mu.Lock()
	m := f.members[req.Name]
	f.mu.Unlock()
	if m != nil && m.access == string(raw) {
		return ctrl.Result{}, nil
	}
	rc, err := f.Access.BuildConfigFromCP(cp)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("member %s: %w", req.Name, err)
	}
	cl, err := f.NewCluster(rc)
	if err != nil {
		return ctrl.Result{}, err
	}
	leases, err := coordinationclient.NewForConfig(rc)
	if err != nil {
		return ctrl.Result{}, err
	}
	mctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		if err := cl.Start(mctx); err != nil {
			log.FromContext(ctx).Error(err, "member cache stopped", "member", req.Name)
		}
	}()
	go f.watch(mctx, cl)
	f.drop(req.Name)
	f.mu.Lock()
	f.members[req.Name] = &member{cl: cl, leases: leases, access: string(raw), cancel: cancel}
	f.mu.Unlock()
	log.FromContext(ctx).Info("member connected", "member", req.Name, "server", rc.Host)
	return ctrl.Result{}, nil
}

func (f *Fleet) drop(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.members[name]; m != nil && m.cancel != nil {
		m.cancel()
		delete(f.members, name)
	}
}

// watch wakes the hub when a member's AdaptivePolicy (its report) changes.
func (f *Fleet) watch(ctx context.Context, cl cluster.Cluster) {
	inf, err := cl.GetCache().GetInformer(ctx, &v1alpha1.AdaptivePolicy{})
	if err != nil || f.Changed == nil {
		return
	}
	on := func(any) { f.Changed() }
	_, _ = inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{AddFunc: on, UpdateFunc: func(_, o any) { on(o) }, DeleteFunc: on})
}

// Cluster returns a connected member.
func (f *Fleet) Cluster(name string) (cluster.Cluster, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.members[name]
	if !ok {
		return nil, false
	}
	return m.cl, true
}

// loadProfiles records every ClusterProfile before the first election, so a member that
// has not connected to its peers yet cannot elect itself as a fleet of one.
func (f *Fleet) loadProfiles(ctx context.Context) error {
	list := &cpv1alpha1.ClusterProfileList{}
	if err := f.Local.GetAPIReader().List(ctx, list, client.InNamespace(f.Namespace)); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cp := range list.Items {
		if cp.Name != f.Self {
			f.profiles[cp.Name] = true
		}
	}
	return nil
}

// voters returns every member of the fleet, with a nil entry for members not connected.
func (f *Fleet) voters() map[string]*member {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]*member{f.Self: f.members[f.Self]}
	for name := range f.profiles {
		out[name] = f.members[name]
	}
	return out
}

// Holds reports whether the named member's copy of the hub Lease names identity: the hub
// checks it before writing to a shared object, so a deposed hub stops writing.
func (f *Fleet) Holds(ctx context.Context, name, identity string) bool {
	f.mu.Lock()
	m := f.members[name]
	f.mu.Unlock()
	if m == nil {
		return false
	}
	l, err := m.leases.Leases(f.Namespace).Get(ctx, v1alpha1.HubLease, metav1.GetOptions{})
	return err == nil && v1alpha1.HubHolder(l, time.Now()) == identity
}

// quorumLock is a client-go resourcelock.Interface over the members' hub Leases: the
// standard resourcelock.LeaseLock per member, and a record counts only when a majority
// of members agree. LeaderElector runs on it unchanged, so leases, renewals, expiry and
// step-down behave exactly as in single-cluster leader election.
//
// Get returns the holder most members name (ties: latest renewal), so a candidate that
// sees a different member set still waits for a live leader's lease to expire. Writes
// use each Lease's resourceVersion (optimistic concurrency) and fail without a majority,
// so a partition's minority side cannot elect a hub. Fleets of two members need both;
// three or more tolerate losing a minority.
type quorumLock struct {
	id    string
	fleet *Fleet

	mu    sync.Mutex
	locks map[string]*resourcelock.LeaseLock
	found map[string]bool
}

func newQuorumLock(id string, f *Fleet) *quorumLock {
	return &quorumLock{id: id, fleet: f, locks: map[string]*resourcelock.LeaseLock{}, found: map[string]bool{}}
}

// voters maps every member to its lock; nil for members not connected, which count as
// unreachable.
func (q *quorumLock) voters() (map[string]*resourcelock.LeaseLock, int) {
	members := q.fleet.voters()
	q.mu.Lock()
	defer q.mu.Unlock()
	out := map[string]*resourcelock.LeaseLock{}
	for name, m := range members {
		if m == nil {
			out[name] = nil
			continue
		}
		l := q.locks[name]
		if l == nil || l.Client != m.leases {
			l = &resourcelock.LeaseLock{Client: m.leases, LockConfig: resourcelock.ResourceLockConfig{Identity: q.id},
				LeaseMeta: metav1.ObjectMeta{Namespace: q.fleet.Namespace, Name: v1alpha1.HubLease}}
			q.locks[name], q.found[name] = l, false
		}
		out[name] = l
	}
	return out, len(out)/2 + 1
}

// each runs fn on every voter concurrently and returns the successes.
func each(ctx context.Context, voters map[string]*resourcelock.LeaseLock, fn func(context.Context, string, *resourcelock.LeaseLock) error) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, RenewDeadline/2)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []string
	ok := 0
	for name, l := range voters {
		wg.Go(func() {
			err := errors.New("not connected")
			if l != nil {
				err = fn(ctx, name, l)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, name+": "+err.Error())
				return
			}
			ok++
		})
	}
	wg.Wait()
	slices.Sort(errs)
	if len(errs) > 0 {
		return ok, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return ok, nil
}

func (q *quorumLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	voters, quorum := q.voters()
	fleetMembers.Set(float64(len(voters)))
	var mu sync.Mutex
	var records []resourcelock.LeaderElectionRecord
	reached, err := each(ctx, voters, func(ctx context.Context, name string, l *resourcelock.LeaseLock) error {
		rec, _, err := l.Get(ctx)
		if apierrors.IsNotFound(err) {
			q.mu.Lock()
			q.found[name] = false
			q.mu.Unlock()
			return nil
		}
		if err != nil {
			return err
		}
		mu.Lock()
		q.mu.Lock()
		q.found[name] = true
		q.mu.Unlock()
		records = append(records, *rec)
		mu.Unlock()
		return nil
	})
	if reached < quorum {
		return nil, nil, fmt.Errorf("%d of %d members reachable, need %d: %w", reached, len(voters), quorum, err)
	}
	if len(records) == 0 {
		return nil, nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, v1alpha1.HubLease)
	}
	best := plurality(records)
	raw, err := json.Marshal(best)
	return &best, raw, err
}

func plurality(records []resourcelock.LeaderElectionRecord) resourcelock.LeaderElectionRecord {
	votes := map[string]int{}
	for _, r := range records {
		votes[r.HolderIdentity]++
	}
	return slices.MaxFunc(records, func(a, b resourcelock.LeaderElectionRecord) int {
		va, vb := votes[a.HolderIdentity], votes[b.HolderIdentity]
		if a.HolderIdentity == "" {
			va = 0
		}
		if b.HolderIdentity == "" {
			vb = 0
		}
		if va != vb {
			return va - vb
		}
		return a.RenewTime.Compare(b.RenewTime.Time)
	})
}

func (q *quorumLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	return q.Update(ctx, ler)
}

// Update writes the record to every member as a compare-and-swap against the Lease last
// read, and succeeds on a majority. A member whose Lease changed since is never
// overwritten: it is re-read, so the next attempt (after LeaderElector re-checks the
// holder) writes against what is there now.
func (q *quorumLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	voters, quorum := q.voters()
	ok, err := each(ctx, voters, func(ctx context.Context, name string, l *resourcelock.LeaseLock) error {
		q.mu.Lock()
		found := q.found[name]
		q.mu.Unlock()
		write := l.Create
		if found {
			write = l.Update
		}
		err := write(ctx, ler)
		if err == nil {
			q.mu.Lock()
			q.found[name] = true
			q.mu.Unlock()
			return nil
		}
		if _, _, gerr := l.Get(ctx); gerr == nil || apierrors.IsNotFound(gerr) {
			q.mu.Lock()
			q.found[name] = gerr == nil
			q.mu.Unlock()
		}
		return err
	})
	if ok < quorum {
		return fmt.Errorf("lease written on %d of %d members, need %d: %w", ok, len(voters), quorum, err)
	}
	return nil
}

func (q *quorumLock) RecordEvent(string) {}
func (q *quorumLock) Identity() string   { return q.id }
func (q *quorumLock) Describe() string {
	return q.fleet.Namespace + "/" + v1alpha1.HubLease + " (fleet quorum)"
}

// Elector runs fleet-wide leader election under the member's own (local) leader
// election, and runs lead while this member is the hub.
type Elector struct {
	Identity string
	Fleet    *Fleet
	Lead     func(ctx context.Context)
}

func (e *Elector) NeedLeaderElection() bool { return true }

func (e *Elector) Start(ctx context.Context) error {
	if err := e.Fleet.loadProfiles(ctx); err != nil {
		return fmt.Errorf("loading ClusterProfiles: %w", err)
	}
	for ctx.Err() == nil {
		le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock: newQuorumLock(e.Identity, e.Fleet), Name: v1alpha1.HubLease, ReleaseOnCancel: true,
			LeaseDuration: LeaseDuration, RenewDeadline: RenewDeadline, RetryPeriod: RetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: e.Lead,
				OnStoppedLeading: func() { log.FromContext(ctx).Info("stopped leading the fleet", "identity", e.Identity) },
				OnNewLeader:      func(id string) { log.FromContext(ctx).Info("fleet hub", "identity", id) },
			},
		})
		if err != nil {
			return err
		}
		le.Run(ctx) // returns when leadership is lost; stand again
	}
	return nil
}
