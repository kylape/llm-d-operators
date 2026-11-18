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

// DefaultRouterAnnotation is the namespace annotation naming the
// Router that InferenceStacks with `routing: {}` attach to. The
// platform admin sets it once per namespace.
const DefaultRouterAnnotation = "serving.llm-d.ai/default-router"

// RuntimeCatalogConfigMapName names the ConfigMap, read from the
// InferenceStack's namespace, that overrides the operator's baked-in
// runtime catalog (runtime name → image + base args).
const RuntimeCatalogConfigMapName = "llm-d-runtime-catalog"

// RuntimeCatalogDataKey is the ConfigMap data key holding the catalog
// document: a JSON object mapping runtime names to
// {"image": string, "args": [string]} entries.
const RuntimeCatalogDataKey = "catalog.json"

// InferenceStackModel identifies the model being served.
type InferenceStackModel struct {
	// URI of the model to serve: an HF repo id, an s3:// or gs://
	// prefix, or whatever the runtime's model loader accepts.
	// +required
	// +kubebuilder:validation:MinLength=1
	URI string `json:"uri"`

	// Name is the model name exposed to clients (vLLM's
	// served-model-name). Defaults to metadata.name.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Name *string `json:"name,omitempty"`
}

// Parallelism declares the multi-dimensional parallelism of one
// engine replica.
type Parallelism struct {
	// Tensor is the tensor-parallel degree. The GPU count per replica
	// is ALWAYS derived from it (spec.resources has no GPU field);
	// nvidia.com/gpu request/limit = tensor regardless of catalog or
	// overrides.
	// +required
	// +kubebuilder:validation:Minimum=1
	Tensor int `json:"tensor"`

	// Pipeline is the pipeline-parallel degree. Schema-reserved:
	// rejected by the controller in v0 (multi-node workloads arrive
	// with LeaderWorkerSet support).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Pipeline *int `json:"pipeline,omitempty"`

	// Data is the data-parallel degree. Schema-reserved: rejected by
	// the controller in v0; how ranks map to pools is a joint design
	// with the pool schema.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Data *int `json:"data,omitempty"`
}

// Accelerator selects the hardware generation via a product name
// that the controller resolves through its catalog into a
// nodeSelector. v0 ships a small built-in catalog; a ConfigMap
// override comes later.
type Accelerator struct {
	// Product names a catalog entry, e.g. a100-80gb, h100-80gb.
	// +required
	// +kubebuilder:validation:MinLength=1
	Product string `json:"product"`
}

// EngineOverrides bounds how far the user may reach into the engine
// container without a full podSpec escape hatch.
type EngineOverrides struct {
	// Args are appended AFTER the derived runtime args, so argparse
	// last-wins semantics let a single flag override a derived one.
	// +optional
	Args []string `json:"args,omitempty"`

	// Env are added to the engine container.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// Image overrides the catalog image for this runtime. Once set it
	// is pinned: operator upgrades do not change it.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Image *string `json:"image,omitempty"`
}

// RoutingSpec attaches the rendered workload to inference-aware
// routing. An empty RoutingSpec (`routing: {}`) resolves the
// namespace's default Router via the serving.llm-d.ai/default-router
// annotation; an explicit routerRef bypasses the annotation.
type RoutingSpec struct {
	// RouterRef names the Router whose EPP serves this stack. When
	// omitted, the namespace's default-router annotation
	// (serving.llm-d.ai/default-router) is used.
	// +optional
	// +kubebuilder:validation:MinLength=1
	RouterRef *string `json:"routerRef,omitempty"`
}

// InferenceStackSpec defines the desired state of InferenceStack:
// typed model semantics in, a serving stack out. The controller
// renders a labeled Deployment and, when routing is set, an owned
// ModelServer. There is no podSpec: the escape hatch is the
// ModelServer layer below, not a field here.
type InferenceStackSpec struct {
	// Model to serve.
	// +required
	Model InferenceStackModel `json:"model"`

	// Runtime names the serving engine. The controller resolves it to
	// an image and base args through its runtime catalog (ConfigMap
	// llm-d-runtime-catalog in the namespace overrides the built-in
	// catalog). v0 supports vllm only.
	// +required
	// +kubebuilder:validation:Enum=vllm
	Runtime string `json:"runtime"`

	// Replicas of the engine workload. Defaults to 1.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Parallelism of one engine replica. The GPU count is derived
	// from tensor and never declared elsewhere.
	// +required
	Parallelism Parallelism `json:"parallelism"`

	// Accelerator constrains scheduling to a hardware product.
	// +optional
	Accelerator *Accelerator `json:"accelerator,omitempty"`

	// Engine bounds the overrides applied to the engine container.
	// +optional
	Engine *EngineOverrides `json:"engine,omitempty"`

	// Resources for the engine container. There is deliberately no
	// GPU count field: nvidia.com/gpu request/limit is always derived
	// from parallelism.tensor, and an explicit GPU quantity here is
	// overwritten by the derived value.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Routing attaches the workload to a Router. Omitted: plain
	// workload, no routing objects rendered. Empty ({}): the
	// namespace's default Router. Explicit routerRef: that Router.
	// +optional
	Routing *RoutingSpec `json:"routing,omitempty"`
}

// InferenceStackStatus defines the observed state of InferenceStack.
type InferenceStackStatus struct {
	// Conditions of the InferenceStack.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DeploymentName is the engine Deployment rendered by this stack.
	// +optional
	DeploymentName string `json:"deploymentName,omitempty"`

	// ModelServerName is the ModelServer rendered when routing is
	// set, wiring the Deployment into a Router.
	// +optional
	ModelServerName string `json:"modelServerName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=istack
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Runtime",type="string",JSONPath=".spec.runtime"
// +kubebuilder:printcolumn:name="Model",type="string",JSONPath=".spec.model.name"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// InferenceStack is the opinionated, user-facing object: typed model
// semantics in, a serving stack out. It renders a labeled Deployment
// (deterministic labels so a hand-authored ModelServer can target it)
// and, when routing is set, an owned ModelServer that wires the
// workload into a Router's EPP and a Gateway.
type InferenceStack struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InferenceStackSpec   `json:"spec,omitempty"`
	Status InferenceStackStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// InferenceStackList contains a list of InferenceStack.
type InferenceStackList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceStack `json:"items"`
}

func init() {
	SchemeBuilder.Register(&InferenceStack{}, &InferenceStackList{})
}
