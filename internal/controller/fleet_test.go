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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// fleetOf builds a Fleet of in-memory members; down[name] makes a member unreachable.
func fleetOf(down map[string]*atomic.Bool, names ...string) *Fleet {
	f := &Fleet{Self: names[0], Namespace: "plumb-system", members: map[string]*member{}, profiles: map[string]bool{}}
	for _, n := range names {
		if n != f.Self {
			f.profiles[n] = true
		}
		cs := fake.NewClientset()
		flag := &atomic.Bool{}
		down[n] = flag
		cs.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			if flag.Load() {
				return true, nil, errors.New("unreachable")
			}
			return false, nil, nil
		})
		f.members[n] = &member{leases: cs.CoordinationV1()}
	}
	return f
}

func record(id string) resourcelock.LeaderElectionRecord {
	now := metav1.Now()
	return resourcelock.LeaderElectionRecord{HolderIdentity: id, LeaseDurationSeconds: 15, AcquireTime: now, RenewTime: now}
}

func TestQuorumLockNeedsMajority(t *testing.T) {
	ctx := context.Background()
	down := map[string]*atomic.Bool{}
	f := fleetOf(down, "a", "b", "c")
	q := newQuorumLock("a/pod", f)
	if _, _, err := q.Get(ctx); err == nil || !isNotFound(err) {
		t.Fatalf("empty fleet: %v", err)
	}
	down["c"].Store(true) // a majority is still reachable
	if err := q.Create(ctx, record("a/pod")); err != nil {
		t.Fatal(err)
	}
	if rec, _, err := q.Get(ctx); err != nil || rec.HolderIdentity != "a/pod" {
		t.Fatalf("get: %+v %v", rec, err)
	}
	down["b"].Store(true) // minority side: no reads, no writes
	if _, _, err := q.Get(ctx); err == nil {
		t.Fatal("read without a majority")
	}
	if err := q.Update(ctx, record("a/pod")); err == nil {
		t.Fatal("write without a majority")
	}
}

// A member missed the last renewals (it was partitioned): the majority's holder wins, and
// the next write brings the stale member back in line.
// A member that is listed but not connected still votes, as unreachable: two members of a
// three-member fleet cannot lose the third and still let one member elect itself.
func TestQuorumLockCountsUnconnectedMembers(t *testing.T) {
	f := fleetOf(map[string]*atomic.Bool{}, "a", "b")
	f.profiles["c"] = true
	delete(f.members, "b")
	if err := newQuorumLock("a/pod", f).Create(context.Background(), record("a/pod")); err == nil {
		t.Fatal("elected with 1 of 3 members")
	}
}

func TestQuorumLockPlurality(t *testing.T) {
	ctx := context.Background()
	down := map[string]*atomic.Bool{}
	f := fleetOf(down, "a", "b", "c")
	q := newQuorumLock("b/pod", f)
	down["c"].Store(true)
	if err := q.Create(ctx, record("b/pod")); err != nil {
		t.Fatal(err)
	}
	down["c"].Store(false)
	stale := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: "plumb-system", Name: v1alpha1.HubLease}}
	stale.Spec.HolderIdentity = new(string)
	*stale.Spec.HolderIdentity = "old/pod"
	if _, err := f.members["c"].leases.Leases("plumb-system").Create(ctx, stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	rec, _, err := q.Get(ctx)
	if err != nil || rec.HolderIdentity != "b/pod" {
		t.Fatalf("plurality: %+v %v", rec, err)
	}
	if err := q.Update(ctx, record("b/pod")); err != nil {
		t.Fatal(err)
	}
	l, _ := f.members["c"].leases.Leases("plumb-system").Get(ctx, v1alpha1.HubLease, metav1.GetOptions{})
	if *l.Spec.HolderIdentity != "b/pod" {
		t.Fatalf("stale member not repaired: %s", *l.Spec.HolderIdentity)
	}
}

// The standard client-go LeaderElector runs on the quorum lock: one leader at a time, and
// the next one takes over after the first steps down. The durations stay well above one
// second: LeaderElector sees a renewal only when the record's JSON changes, and its
// RenewTime (metav1.Time) marshals to whole seconds (client-go leaselock.go Get).
func TestElectionFailover(t *testing.T) {
	f := fleetOf(map[string]*atomic.Bool{}, "a", "b", "c")
	var leading atomic.Int32
	run := func(ctx context.Context, id string, led chan<- string) {
		le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock: newQuorumLock(id, f), LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second,
			RetryPeriod: 200 * time.Millisecond, ReleaseOnCancel: true, Name: v1alpha1.HubLease,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					if leading.Add(1) > 1 {
						t.Error("two leaders")
					}
					led <- id
					<-ctx.Done()
					leading.Add(-1)
				},
				OnStoppedLeading: func() {},
			},
		})
		if err != nil {
			t.Error(err)
			return
		}
		le.Run(ctx)
	}
	led := make(chan string, 2)
	ctxA, stopA := context.WithCancel(context.Background())
	go run(ctxA, "a/pod", led)
	if got := <-led; got != "a/pod" {
		t.Fatalf("first leader %s", got)
	}
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	go run(ctxB, "b/pod", led)
	select {
	case got := <-led:
		t.Fatalf("%s led while a/pod held the lease", got)
	case <-time.After(4 * time.Second):
	}
	stopA()
	select {
	case got := <-led:
		if got != "b/pod" {
			t.Fatalf("failover to %s", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no failover")
	}
}

func isNotFound(err error) bool {
	var s interface{ Status() metav1.Status }
	return errors.As(err, &s) && s.Status().Reason == metav1.StatusReasonNotFound
}
