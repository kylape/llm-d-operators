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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultTargetPort is the engine container port assumed when
// spec.targetPort is unset.
const DefaultTargetPort int32 = 8000

// DefaultGatewayAnnotation is the namespace annotation naming the
// Gateway that ModelServers without an explicit spec.gateway attach
// their HTTPRoute to. The platform admin sets it once per namespace.
const DefaultGatewayAnnotation = "serving.llm-d.ai/default-gateway"

// WorkloadRef references the workload primitive that runs the model
// server engine. The ModelServer derives the InferencePool selector
// from the referenced workload's own selector, so no label contract is
// maintained by hand.
type WorkloadRef struct {
	// APIVersion of the referenced workload.
	// +kubebuilder:validation:Enum=apps/v1
	// +required
	APIVersion string `json:"apiVersion"`

	// Kind of the referenced workload. v0 supports Deployment;
	// LeaderWorkerSet and DisaggregatedSet arrive with multi-node
	// support.
	// +kubebuilder:validation:Enum=Deployment
	// +required
	Kind string `json:"kind"`

	// Name of the referenced workload, in the same namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// ModelServerSpec defines the desired state of ModelServer: the wiring
// of one model-server workload into inference-aware routing. The
// ModelServer creates the InferencePool (selector derived from the
// workload) and the HTTPRoute (backendRef to the pool, parentRef to a
// Gateway), or joins an existing pool via poolRef (shared-pool mode).
// It never creates the Gateway itself.
type ModelServerSpec struct {
	// Workload to wire into routing.
	// +required
	Workload WorkloadRef `json:"workload"`

	// RouterRef names the Router whose EPP serves this model server.
	// The pool's endpointPickerRef points at that Router's Service.
	// +required
	// +kubebuilder:validation:MinLength=1
	RouterRef string `json:"routerRef"`

	// PoolRef names an existing InferencePool to join instead of
	// creating one (shared-pool mode: N model servers behind one
	// Router-owned pool). The referenced pool must exist and its
	// selector must match the referenced workload's selector; the
	// HTTPRoute backendRef points at the referenced pool. When unset,
	// this ModelServer creates and owns its pool.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PoolRef *string `json:"poolRef,omitempty"`

	// TargetPort is the engine container port that the pool routes to.
	// Defaults to 8000.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	TargetPort *int32 `json:"targetPort,omitempty"`

	// Gateway names the Gateway the HTTPRoute attaches to. When
	// omitted, the namespace's default gateway annotation
	// (serving.llm-d.ai/default-gateway) is used. When neither is set,
	// no HTTPRoute is created and RouteReady reports
	// GatewayNotConfigured.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Gateway *string `json:"gateway,omitempty"`

	// Hostname for the HTTPRoute. When omitted the route matches all
	// hostnames the Gateway listener accepts.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Hostname *string `json:"hostname,omitempty"`
}

// ModelServerStatus defines the observed state of ModelServer.
type ModelServerStatus struct {
	// Conditions of the ModelServer.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PoolName is the name of the InferencePool owned by this
	// ModelServer.
	// +optional
	PoolName string `json:"poolName,omitempty"`

	// EPPServiceName is the EPP Service the pool points at.
	// +optional
	EPPServiceName string `json:"eppServiceName,omitempty"`

	// RouteName is the name of the HTTPRoute owned by this
	// ModelServer, when one is rendered.
	// +optional
	RouteName string `json:"routeName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ms
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Workload",type="string",JSONPath=".spec.workload.name"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ModelServer wires one model-server workload (a Deployment, later
// LeaderWorkerSet or DisaggregatedSet) into inference-aware routing:
// it owns the InferencePool and the HTTPRoute that join the workload
// to a Router's EPP and a Gateway. ModelServers are created directly
// (bring-your-own workload) or owned by an InferenceStack.
type ModelServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelServerSpec   `json:"spec,omitempty"`
	Status ModelServerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ModelServerList contains a list of ModelServer.
type ModelServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelServer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ModelServer{}, &ModelServerList{})
}
