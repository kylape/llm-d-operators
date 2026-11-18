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

package serving

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	servingv1alpha1 "github.com/llm-d/llm-d-operators/api/serving/v1alpha1"
	"github.com/llm-d/llm-d-operators/internal/kube"
)

// EngineContainerName is the engine container in the rendered
// Deployment.
const EngineContainerName = "engine"

// EnginePort is the engine container's HTTP port; the rendered
// ModelServer points its pool targetPort here.
const EnginePort int32 = 8000

// GPUResourceName is the NVIDIA GPU resource whose request/limit is
// derived from parallelism.tensor.
const GPUResourceName = corev1.ResourceName("nvidia.com/gpu")

// runtimeCatalogEntry is one row of the runtime catalog: the image
// and base args of a runtime. Catalog args may reference {model}
// (resolved to model.name) and {tensor} (resolved to
// parallelism.tensor).
type runtimeCatalogEntry struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

// defaultRuntimeCatalog is the baked-in fallback used when no
// llm-d-runtime-catalog ConfigMap exists in the namespace.
//
// NOTE: the vllm entry is a DEMO STAND-IN. It points at
// llm-d-inference-sim, a GPU-free vLLM simulator that accepts
// --model/--port instead of vLLM flags, so the prototype runs on any
// cluster. A real deployment overrides it with a namespaced
// llm-d-runtime-catalog ConfigMap whose vllm entry uses the actual
// vLLM image and flags (see config/samples for a copy-paste catalog
// with --tensor-parallel-size). The GPU-request invariant
// (nvidia.com/gpu = parallelism.tensor) is derived regardless of the
// catalog, even though the sim ignores GPUs.
var defaultRuntimeCatalog = map[string]runtimeCatalogEntry{
	"vllm": {
		Image: "ghcr.io/llm-d/llm-d-inference-sim:v0.11.2",
		Args:  []string{"--model", "{model}", "--port", "8000"},
	},
}

// acceleratorCatalog maps accelerator.product to the nodeSelector it
// resolves to. v0 ships this small built-in map; a ConfigMap override
// (like the runtime catalog) comes later.
var acceleratorCatalog = map[string]map[string]string{
	"a100-40gb":  {"nvidia.com/gpu.product": "NVIDIA-A100-SXM4-40GB"},
	"a100-80gb":  {"nvidia.com/gpu.product": "NVIDIA-A100-SXM4-80GB"},
	"h100-80gb":  {"nvidia.com/gpu.product": "NVIDIA-H100-SXM4-80GB"},
	"h200-141gb": {"nvidia.com/gpu.product": "NVIDIA-H200-SXM4-141GB"},
}

// InferenceStackReconciler reconciles an InferenceStack object.
type InferenceStackReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=serving.llm-d.ai,resources=inferencestacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=serving.llm-d.ai,resources=inferencestacks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=serving.llm-d.ai,resources=modelservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// Reconcile renders the engine Deployment (resolving runtime → image
// + args and accelerator → nodeSelector through catalogs) and, when
// routing is set, an owned ModelServer. Children render on EXISTENCE
// of their inputs, never on their readiness — readiness is reported
// via conditions only (see the ModelServer/Router wiring deadlock).
func (r *InferenceStackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("inferencestack", req.NamespacedName)

	stack := &servingv1alpha1.InferenceStack{}
	if err := r.Get(ctx, req.NamespacedName, stack); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := servingv1alpha1.InferenceStackStatus{ObservedGeneration: stack.Generation}

	// 1. Validate spec inputs the catalogs cannot resolve. Rendering
	// halts and previously rendered children retire, mirroring the
	// Router's hold-back of the EPP Deployment.
	if bad := unsupportedParallelism(stack); bad != "" {
		r.retireChildren(ctx, stack)
		setStackCondition(&status, "WorkloadReady", metav1.ConditionFalse, "UnsupportedParallelism",
			fmt.Sprintf("parallelism.%s is schema-reserved and rejected in v0; only tensor is implemented", bad))
		setStackCondition(&status, "Ready", metav1.ConditionFalse, "UnsupportedParallelism",
			"fix spec.parallelism")
		return ctrl.Result{}, r.patchStatus(ctx, stack, status)
	}

	nodeSelector, ok := resolveAccelerator(stack)
	if !ok {
		r.retireChildren(ctx, stack)
		setStackCondition(&status, "WorkloadReady", metav1.ConditionFalse, "UnknownAccelerator",
			fmt.Sprintf("accelerator product %q is not in the accelerator catalog (v0 built-in: %s)",
				stack.Spec.Accelerator.Product, strings.Join(acceleratorProducts(), ", ")))
		setStackCondition(&status, "Ready", metav1.ConditionFalse, "UnknownAccelerator",
			"fix spec.accelerator.product")
		return ctrl.Result{}, r.patchStatus(ctx, stack, status)
	}

	entry, problem, err := r.resolveRuntime(ctx, stack)
	if err != nil {
		return ctrl.Result{}, err
	}
	if problem != "" {
		r.retireChildren(ctx, stack)
		setStackCondition(&status, "WorkloadReady", metav1.ConditionFalse, "RuntimeCatalogInvalid", problem)
		setStackCondition(&status, "Ready", metav1.ConditionFalse, "RuntimeCatalogInvalid",
			"fix the runtime catalog")
		return ctrl.Result{}, r.patchStatus(ctx, stack, status)
	}
	if entry == nil {
		r.retireChildren(ctx, stack)
		setStackCondition(&status, "WorkloadReady", metav1.ConditionFalse, "RuntimeNotInCatalog",
			fmt.Sprintf("runtime %q has no entry in the catalog (ConfigMap %s or the built-in default)",
				stack.Spec.Runtime, servingv1alpha1.RuntimeCatalogConfigMapName))
		setStackCondition(&status, "Ready", metav1.ConditionFalse, "RuntimeNotInCatalog",
			"add the runtime to the catalog or use a supported runtime")
		return ctrl.Result{}, r.patchStatus(ctx, stack, status)
	}

	// 2. Render the engine Deployment: derived image/args/nodeSelector
	// and the GPU invariant.
	deployment := engineDeployment(stack, entry, nodeSelector)
	if err := ctrl.SetControllerReference(stack, deployment, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := kube.Upsert(ctx, r.Client, deployment, func(existing client.Object) {
		existing.(*appsv1.Deployment).Spec = deployment.Spec
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile deployment: %w", err)
	}
	status.DeploymentName = deployment.Name

	current := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}, current); err != nil {
		return ctrl.Result{}, err
	}
	if workloadAvailable(current) {
		setStackCondition(&status, "WorkloadReady", metav1.ConditionTrue, "Available",
			fmt.Sprintf("deployment %s is available (%d/%d replicas)",
				current.Name, current.Status.AvailableReplicas, ptr.Deref(current.Spec.Replicas, 1)))
	} else {
		setStackCondition(&status, "WorkloadReady", metav1.ConditionFalse, "Unavailable",
			fmt.Sprintf("deployment %s is not yet available", current.Name))
	}

	// 3. Routing: render the owned ModelServer on Deployment
	// EXISTENCE (never on its readiness — that deadlocked the
	// ModelServer ↔ Router wiring).
	if stack.Spec.Routing == nil {
		// Plain workload: retire a ModelServer an earlier generation
		// rendered. Garbage collection cascades to its pool/route.
		r.deleteChild(ctx, &servingv1alpha1.ModelServer{
			ObjectMeta: metav1.ObjectMeta{Name: stack.Name, Namespace: stack.Namespace},
		})
	} else {
		routerName, err := r.resolveRouter(ctx, stack)
		if err != nil {
			return ctrl.Result{}, err
		}
		if routerName == "" {
			r.deleteChild(ctx, &servingv1alpha1.ModelServer{
				ObjectMeta: metav1.ObjectMeta{Name: stack.Name, Namespace: stack.Namespace},
			})
			setStackCondition(&status, "RoutingReady", metav1.ConditionFalse, "NoDefaultRouter",
				fmt.Sprintf("routing is set without routerRef and namespace %s has no %s annotation",
					stack.Namespace, servingv1alpha1.DefaultRouterAnnotation))
		} else {
			ms := &servingv1alpha1.ModelServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      stack.Name,
					Namespace: stack.Namespace,
					Labels:    istackLabels(stack.Name),
				},
				Spec: servingv1alpha1.ModelServerSpec{
					Workload: servingv1alpha1.WorkloadRef{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       deployment.Name,
					},
					RouterRef:  routerName,
					TargetPort: ptr.To(EnginePort),
				},
			}
			if err := ctrl.SetControllerReference(stack, ms, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := kube.Upsert(ctx, r.Client, ms, func(existing client.Object) {
				existing.(*servingv1alpha1.ModelServer).Spec = ms.Spec
			}); err != nil {
				return ctrl.Result{}, fmt.Errorf("reconcile modelserver: %w", err)
			}
			status.ModelServerName = ms.Name

			// RoutingReady mirrors the owned ModelServer's Ready
			// condition upward.
			observed := &servingv1alpha1.ModelServer{}
			if err := r.Get(ctx, types.NamespacedName{Name: ms.Name, Namespace: ms.Namespace}, observed); err != nil {
				return ctrl.Result{}, err
			}
			if cond := meta.FindStatusCondition(observed.Status.Conditions, "Ready"); cond != nil {
				mirrored := *cond
				mirrored.Type = "RoutingReady"
				mirrored.LastTransitionTime = metav1.Now()
				meta.SetStatusCondition(&status.Conditions, mirrored)
			} else {
				setStackCondition(&status, "RoutingReady", metav1.ConditionUnknown, "ModelServerPending",
					fmt.Sprintf("model server %s has not reported readiness yet", ms.Name))
			}
		}
	}

	// 4. Aggregate.
	ready := true
	for _, cond := range status.Conditions {
		if cond.Type != "Ready" && cond.Status != metav1.ConditionTrue {
			ready = false
		}
	}
	if ready {
		setStackCondition(&status, "Ready", metav1.ConditionTrue, "Ready",
			"workload rendered and routing wired")
	} else {
		setStackCondition(&status, "Ready", metav1.ConditionFalse, "Incomplete",
			"see WorkloadReady and RoutingReady for the missing piece")
	}

	if err := r.patchStatus(ctx, stack, status); err != nil {
		return ctrl.Result{}, err
	}
	log.V(1).Info("reconciled inferencestack",
		"deployment", status.DeploymentName, "modelserver", status.ModelServerName)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the InferenceStack to its owned Deployment
// and ModelServer, and to namespaces: default-router annotation
// changes must requeue.
func (r *InferenceStackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&servingv1alpha1.InferenceStack{}).
		Owns(&appsv1.Deployment{}).
		Owns(&servingv1alpha1.ModelServer{}).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.namespacesToStacks)).
		Complete(r)
}

// namespacesToStacks requeues every InferenceStack in a changed
// namespace: the default-router annotation is read at reconcile time.
func (r *InferenceStackReconciler) namespacesToStacks(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &servingv1alpha1.InferenceStackList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetName())); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: list.Items[i].Namespace, Name: list.Items[i].Name,
		}})
	}
	return reqs
}

// resolveRouter returns the Router name for the routing field:
// explicit routerRef, else the namespace's default-router annotation,
// else "".
func (r *InferenceStackReconciler) resolveRouter(ctx context.Context, stack *servingv1alpha1.InferenceStack) (string, error) {
	if stack.Spec.Routing.RouterRef != nil {
		return *stack.Spec.Routing.RouterRef, nil
	}
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: stack.Namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return ns.Annotations[servingv1alpha1.DefaultRouterAnnotation], nil
}

// resolveRuntime resolves the runtime catalog entry: the ConfigMap
// llm-d-runtime-catalog in the stack's namespace when present (and
// valid), else the baked-in default. A nil entry with empty problem
// means the runtime has no entry anywhere; a non-empty problem means
// the catalog ConfigMap exists but cannot be used; an error is a
// transient infrastructure failure.
func (r *InferenceStackReconciler) resolveRuntime(ctx context.Context, stack *servingv1alpha1.InferenceStack) (*runtimeCatalogEntry, string, error) {
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{
		Name: servingv1alpha1.RuntimeCatalogConfigMapName, Namespace: stack.Namespace,
	}, cm)
	if apierrors.IsNotFound(err) {
		if entry, ok := defaultRuntimeCatalog[stack.Spec.Runtime]; ok {
			return &entry, "", nil
		}
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	raw, ok := cm.Data[servingv1alpha1.RuntimeCatalogDataKey]
	if !ok {
		return nil, fmt.Sprintf("configmap %s has no %s key",
			servingv1alpha1.RuntimeCatalogConfigMapName, servingv1alpha1.RuntimeCatalogDataKey), nil
	}
	catalog := map[string]runtimeCatalogEntry{}
	if err := json.Unmarshal([]byte(raw), &catalog); err != nil {
		return nil, fmt.Sprintf("configmap %s key %s is not valid catalog JSON: %v",
			servingv1alpha1.RuntimeCatalogConfigMapName, servingv1alpha1.RuntimeCatalogDataKey, err), nil
	}
	entry, ok := catalog[stack.Spec.Runtime]
	if !ok {
		return nil, "", nil
	}
	if entry.Image == "" {
		return nil, fmt.Sprintf("catalog entry for runtime %q has an empty image", stack.Spec.Runtime), nil
	}
	return &entry, "", nil
}

// engineDeployment renders the engine Deployment. Deterministic
// labels make it targetable by a hand-authored ModelServer; the
// selector is the same label set, so the pool selector a ModelServer
// derives from it is deterministic too.
func engineDeployment(stack *servingv1alpha1.InferenceStack, entry *runtimeCatalogEntry, nodeSelector map[string]string) *appsv1.Deployment {
	replicas := ptr.Deref(stack.Spec.Replicas, 1)
	image := entry.Image
	var env []corev1.EnvVar
	if engine := stack.Spec.Engine; engine != nil {
		if engine.Image != nil {
			image = *engine.Image
		}
		env = engine.Env
	}

	// Derived args first; engine.args are APPENDED after them so
	// argparse last-wins lets one flag override a derived one.
	var extraArgs []string
	if engine := stack.Spec.Engine; engine != nil {
		extraArgs = engine.Args
	}
	args := make([]string, 0, len(entry.Args)+len(extraArgs))
	args = append(args, substituteCatalogArgs(entry.Args, stack)...)
	args = append(args, extraArgs...)

	// The derive-don't-declare invariant: GPU count comes from
	// parallelism.tensor, whatever the catalog or resources say.
	resources := corev1.ResourceRequirements{}
	if stack.Spec.Resources != nil {
		resources = *stack.Spec.Resources.DeepCopy()
	}
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}
	gpus := resource.MustParse(strconv.Itoa(stack.Spec.Parallelism.Tensor))
	resources.Requests[GPUResourceName] = gpus
	resources.Limits[GPUResourceName] = gpus

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stack.Name,
			Namespace: stack.Namespace,
			Labels:    istackLabels(stack.Name),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: istackLabels(stack.Name)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: istackLabels(stack.Name)},
				Spec: corev1.PodSpec{
					NodeSelector: nodeSelector,
					Containers: []corev1.Container{{
						Name:  EngineContainerName,
						Image: image,
						Args:  args,
						Env:   env,
						Ports: []corev1.ContainerPort{{
							Name: "http", ContainerPort: EnginePort,
						}},
						Resources: resources,
					}},
				},
			},
		},
	}
}

// substituteCatalogArgs expands catalog arg placeholders: {model}
// resolves to model.name (defaulting to the object name), {tensor} to
// parallelism.tensor.
func substituteCatalogArgs(args []string, stack *servingv1alpha1.InferenceStack) []string {
	model := ptr.Deref(stack.Spec.Model.Name, stack.Name)
	out := make([]string, 0, len(args))
	for _, arg := range args {
		arg = strings.ReplaceAll(arg, "{model}", model)
		arg = strings.ReplaceAll(arg, "{tensor}", strconv.Itoa(stack.Spec.Parallelism.Tensor))
		out = append(out, arg)
	}
	return out
}

// resolveAccelerator returns the nodeSelector for the declared
// accelerator product, or ok=false when the product is unknown.
func resolveAccelerator(stack *servingv1alpha1.InferenceStack) (map[string]string, bool) {
	if stack.Spec.Accelerator == nil {
		return nil, true
	}
	selector, ok := acceleratorCatalog[stack.Spec.Accelerator.Product]
	if !ok {
		return nil, false
	}
	return selector, true
}

func acceleratorProducts() []string {
	products := make([]string, 0, len(acceleratorCatalog))
	for product := range acceleratorCatalog {
		products = append(products, product)
	}
	sort.Strings(products)
	return products
}

// unsupportedParallelism returns the name of a schema-reserved
// parallelism dimension that is set, or "".
func unsupportedParallelism(stack *servingv1alpha1.InferenceStack) string {
	if stack.Spec.Parallelism.Pipeline != nil {
		return "pipeline"
	}
	if stack.Spec.Parallelism.Data != nil {
		return "data"
	}
	return ""
}

// retireChildren removes the rendered children of an unrenderable
// stack.
func (r *InferenceStackReconciler) retireChildren(ctx context.Context, stack *servingv1alpha1.InferenceStack) {
	r.deleteChild(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: stack.Name, Namespace: stack.Namespace},
	})
	r.deleteChild(ctx, &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: stack.Name, Namespace: stack.Namespace},
	})
}

func (r *InferenceStackReconciler) deleteChild(ctx context.Context, obj client.Object) {
	_ = client.IgnoreNotFound(r.Delete(ctx, obj))
}

func (r *InferenceStackReconciler) patchStatus(ctx context.Context, stack *servingv1alpha1.InferenceStack, status servingv1alpha1.InferenceStackStatus) error {
	if stack.Status.ObservedGeneration == status.ObservedGeneration &&
		stack.Status.DeploymentName == status.DeploymentName &&
		stack.Status.ModelServerName == status.ModelServerName &&
		conditionsEqual(stack.Status.Conditions, status.Conditions) {
		return nil
	}
	stack.Status = status
	return r.Status().Update(ctx, stack)
}

func setStackCondition(status *servingv1alpha1.InferenceStackStatus, condType string, condStatus metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: condType, Status: condStatus, Reason: reason, Message: message,
		LastTransitionTime: metav1.Now(),
	})
}

// istackLabels are the deterministic labels of an InferenceStack's
// workload: the pool selector a ModelServer derives from the
// Deployment is exactly this set.
func istackLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":     "llm-d-operators",
		"serving.llm-d.ai/inference-stack": name,
		"app":                              name,
	}
}
