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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics, served with controller-runtime's on the agent's metrics endpoint.
var (
	hubLeading = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "plumb_hub_leading", Help: "1 while this agent is the fleet hub."})
	fleetMembers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "plumb_fleet_members", Help: "Members in the fleet, this one included; connected or not."})
	fleetEscalated = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "plumb_fleet_escalated", Help: "1 while the policy's fleet is escalated (hub only)."}, []string{"policy"})
	hubDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "plumb_hub_decisions_total", Help: "Hub decisions that changed something, by action, ranking source and whether they were applied."},
		[]string{"policy", "action", "source", "applied"})
	hubStepErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "plumb_hub_step_errors_total", Help: "Hub steps that failed to read or write a member or route."}, []string{"policy"})
	modelRequests = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "plumb_model_request_duration_seconds", Help: "Model ranking calls, by result (ok, error).",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5}}, []string{"result"})
	timeToReady = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "plumb_decision_time_to_ready_seconds", Help: "From a decision raising a floor to the cluster having that many ready replicas.",
		Buckets: []float64{15, 30, 60, 120, 300, 600, 1200}}, []string{"policy"})
	memberNeeded = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "plumb_member_needed_replicas", Help: "Replicas this cluster needs beyond what it runs."}, []string{"policy"})
	memberStaticRoom = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "plumb_member_static_room_replicas", Help: "Replicas that still fit on this cluster's existing nodes."}, []string{"policy"})
)

func init() {
	metrics.Registry.MustRegister(hubLeading, fleetMembers, fleetEscalated, hubDecisions, hubStepErrors, modelRequests, timeToReady,
		memberNeeded, memberStaticRoom)
}
