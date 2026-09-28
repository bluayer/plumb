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

// Package v1alpha1 contains the AdaptivePolicy API of the plumb-k8s.github.io group.
// +kubebuilder:object:generate=true
// +groupName=plumb-k8s.github.io
package v1alpha1

import (
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
)

// The fencing contract between the hub and every member's scaler: an intent is valid
// only while Intent.Hub is the holder of this member's copy of the hub Lease.

// HubLease is the coordination.k8s.io/v1 Lease every member keeps in its namespace. The
// hub holds it on a majority of members; each member reads its own copy to check that
// an intent comes from the current hub.
const HubLease = "plumb-hub"

// HubHolder reads this member's copy of the hub Lease: the identity it names, if the
// lease has not expired.
func HubHolder(l *coordinationv1.Lease, now time.Time) string {
	s := l.Spec
	if s.HolderIdentity == nil || s.RenewTime == nil || s.LeaseDurationSeconds == nil {
		return ""
	}
	if now.After(s.RenewTime.Add(time.Duration(*s.LeaseDurationSeconds) * time.Second)) {
		return ""
	}
	return *s.HolderIdentity
}
