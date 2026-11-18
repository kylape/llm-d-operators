/*
Copyright 2026 The llm-d Authors.

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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RouterPoolSpec declares a shared InferencePool owned by the Router
// itself. When set, the Router creates an InferencePool named
// <router>-pool upfront with the given selector and activates
// immediately (the pool links to its own EPP service); N model-server
// workloads join that one pool instead of each creating their own.
// This is the shared-pool mode: one EPP, one pool, many backends.
type RouterPoolSpec struct {
	// Selector is the pool's endpoint selector exactly as given:
	// workloads join the pool by carrying these labels on their pods.
	// Unlike the ModelServer path it is NOT derived from a workload —
	// the platform admin owns the label contract.
	// +required
	// +kubebuilder:validation:MinProperties=1
	Selector map[string]string `json:"selector"`

	// TargetPort is the engine container port the pool routes to.
	// Defaults to 8000.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	TargetPort *int32 `json:"targetPort,omitempty"`
}

// RouterSpec defines the desired state of Router: a deployable
// llm-d-router Endpoint Picker (EPP) in gateway mode. The Router never
// renders a proxy; a Gateway API implementation (Envoy Gateway,
// agentgateway, Istio, ...) owns the data plane and consults this EPP
// through the InferencePool that points at it.
type RouterSpec struct {
	// Replicas is the number of EPP pods. One replica is
	// active; additional replicas require LeaderElection.
	// Defaults to 1.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Image overrides the EPP container image. When set here it is
	// pinned: later operator upgrades do not change it until the user
	// changes this field.
	// +optional
	Image *string `json:"image,omitempty"`

	// Resources for the EPP container. The GPU count concept does not
	// apply; the EPP is control plane only.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// LeaderElection enables the active-passive HA mode: one leader
	// serves scheduling decisions, the rest are warm standbys. When
	// false, all replicas are active (active-active) and must only run
	// active-active compatible plugins. Defaults to false.
	// +optional
	LeaderElection *bool `json:"leaderElection,omitempty"`

	// Config is the EndpointPickerConfig the EPP runs. When omitted,
	// the operator's default profile is used (queue-scorer,
	// kv-cache-utilization-scorer, prefix-cache-scorer with their
	// built-in defaults). The rendered document is observable via
	// status.configHash.
	// +optional
	Config *EndpointPickerConfig `json:"config,omitempty"`

	// Pool declares a shared InferencePool the Router owns upfront,
	// activating it without waiting for a ModelServer-created pool to
	// link. ModelServers join it via spec.poolRef.
	// +optional
	Pool *RouterPoolSpec `json:"pool,omitempty"`
}

// RouterStatus defines the observed state of Router.
type RouterStatus struct {
	// Conditions of the Router.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ConfigHash is a sha256 digest of the effective
	// EndpointPickerConfig document rendered into the EPP's ConfigMap.
	// It makes the effective configuration observable without exposing
	// the full object in status.
	// +optional
	ConfigHash string `json:"configHash,omitempty"`

	// ServiceName is the name of the EPP Service that InferencePools
	// reference in their endpointPickerRef.
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// PoolName is the name of the single InferencePool currently wired
	// to this Router's EPP, when one is linked.
	// +optional
	PoolName string `json:"poolName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=router
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Pool",type="string",JSONPath=".status.poolName"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Router is a deployable llm-d-router Endpoint Picker. A Router stays
// inert (no EPP Deployment) until exactly one InferencePool references
// its Service via endpointPickerRef; the EPP watches that pool. That
// link arrives either from a ModelServer-created pool or from the
// Router's own spec.pool (shared-pool mode, which activates it
// upfront). Routers are shared infrastructure, typically created by
// the platform admin and referenced by ModelServers.
type Router struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RouterSpec   `json:"spec,omitempty"`
	Status RouterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RouterList contains a list of Router.
type RouterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Router `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Router{}, &RouterList{})
}
