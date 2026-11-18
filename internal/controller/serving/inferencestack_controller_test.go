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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1alpha1 "github.com/llm-d/llm-d-operators/api/routing/v1alpha1"
	servingv1alpha1 "github.com/llm-d/llm-d-operators/api/serving/v1alpha1"
	"github.com/llm-d/llm-d-operators/internal/controller/routing"
	igwapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestInferenceStackRendersDeployment(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen3", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:      servingv1alpha1.InferenceStackModel{URI: "hf://Qwen/Qwen3-32B", Name: ptr.To("qwen3-32b")},
			Runtime:    "vllm",
			Replicas:   ptr.To(int32(2)),
			Parallelism: servingv1alpha1.Parallelism{Tensor: 4},
			Engine: &servingv1alpha1.EngineOverrides{
				Args: []string{"--dtype", "auto"},
				Env:  []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
			},
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			},
		},
	}
	require.NoError(t, c.Create(ctx, stack))

	_, err := r.Reconcile(ctx, requestFor("qwen3"))
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "qwen3", Namespace: "test"}, dep))

	// Deterministic labels; the pool selector derives from them.
	wantLabels := map[string]string{
		"app.kubernetes.io/managed-by":     "llm-d-operators",
		"serving.llm-d.ai/inference-stack": "qwen3",
		"app":                              "qwen3",
	}
	assert.Equal(t, wantLabels, dep.Labels)
	assert.Equal(t, wantLabels, dep.Spec.Template.Labels)
	assert.Equal(t, wantLabels, dep.Spec.Selector.MatchLabels)
	assert.Equal(t, int32(2), *dep.Spec.Replicas)

	require.Len(t, dep.Spec.Template.Spec.Containers, 1)
	engine := dep.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "engine", engine.Name)
	assert.Equal(t, "ghcr.io/llm-d/llm-d-inference-sim:v0.11.2", engine.Image,
		"baked-in catalog stand-in image")
	// Catalog args with {model} substituted, engine args appended after.
	assert.Equal(t, []string{"--model", "qwen3-32b", "--port", "8000", "--dtype", "auto"}, engine.Args)
	assert.Contains(t, engine.Env, corev1.EnvVar{Name: "FOO", Value: "bar"})
	assert.Equal(t, int32(8000), engine.Ports[0].ContainerPort)

	// The derive-don't-declare invariant: GPUs from parallelism.tensor.
	gpus := resource.MustParse("4")
	assert.Equal(t, gpus, engine.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")])
	assert.Equal(t, gpus, engine.Resources.Limits[corev1.ResourceName("nvidia.com/gpu")])
	assert.Equal(t, resource.MustParse("2"), engine.Resources.Requests[corev1.ResourceCPU],
		"non-GPU resources pass through")

	// No routing: plain workload, no ModelServer.
	ms := &servingv1alpha1.ModelServer{}
	err = c.Get(ctx, types.NamespacedName{Name: "qwen3", Namespace: "test"}, ms)
	require.Error(t, err, "no ModelServer must exist without routing")

	current := &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "qwen3", Namespace: "test"}, current))
	assert.Equal(t, "qwen3", current.Status.DeploymentName)
	assert.Empty(t, current.Status.ModelServerName)
	assertCondition(t, current.Status.Conditions, "WorkloadReady", false, "Unavailable")
	_, found := findCondition(current.Status.Conditions, "RoutingReady")
	assert.False(t, found, "no RoutingReady condition without routing")
	assertCondition(t, current.Status.Conditions, "Ready", false, "Incomplete")
}

func TestInferenceStackGPUDerivationOverridesDeclared(t *testing.T) {
	// A user smuggling a GPU count into resources must not win: the
	// derived value from parallelism.tensor always applies.
	ctx := context.Background()
	c, r := newStackHarness(t)

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "override", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://m"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 2},
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("override"))
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "override", Namespace: "test"}, dep))
	gpus := resource.MustParse("2")
	assert.Equal(t, gpus, dep.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceName("nvidia.com/gpu")])
	assert.Equal(t, gpus, dep.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceName("nvidia.com/gpu")])
}

func TestInferenceStackRuntimeCatalogOverride(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	// The real-vLLM catalog: image, --tensor-parallel-size with the
	// {tensor} placeholder.
	catalog := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: servingv1alpha1.RuntimeCatalogConfigMapName, Namespace: "test"},
		Data: map[string]string{servingv1alpha1.RuntimeCatalogDataKey: `{
			"vllm": {
				"image": "vllm/vllm-openai:v0.11.0",
				"args": ["--model", "{model}", "--port", "8000", "--tensor-parallel-size", "{tensor}"]
			}
		}`},
	}
	require.NoError(t, c.Create(ctx, catalog))

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "realvllm", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://Qwen/Qwen3-32B"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 4},
			Engine: &servingv1alpha1.EngineOverrides{
				Image: ptr.To("registry.example/vllm:v42"),
				Args:  []string{"--max-model-len", "32768"},
			},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("realvllm"))
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "realvllm", Namespace: "test"}, dep))
	engine := dep.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "registry.example/vllm:v42", engine.Image, "engine.image overrides the catalog image")
	assert.Equal(t, []string{
		"--model", "realvllm", // {model} defaults to metadata.name
		"--port", "8000",
		"--tensor-parallel-size", "4", // {tensor} from parallelism
		"--max-model-len", "32768", // appended after derived args
	}, engine.Args)
}

func TestInferenceStackBrokenCatalog(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	catalog := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: servingv1alpha1.RuntimeCatalogConfigMapName, Namespace: "test"},
		Data:       map[string]string{servingv1alpha1.RuntimeCatalogDataKey: `{"vllm": `},
	}
	require.NoError(t, c.Create(ctx, catalog))

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://m"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 1},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("broken"))
	require.NoError(t, err)

	current := &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "broken", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "WorkloadReady", false, "RuntimeCatalogInvalid")

	dep := &appsv1.Deployment{}
	err = c.Get(ctx, types.NamespacedName{Name: "broken", Namespace: "test"}, dep)
	require.Error(t, err, "no deployment must render from a broken catalog")
}

func TestInferenceStackAccelerator(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "accel", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://m"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 8},
			Accelerator: &servingv1alpha1.Accelerator{Product: "a100-80gb"},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("accel"))
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "accel", Namespace: "test"}, dep))
	assert.Equal(t, map[string]string{
		"nvidia.com/gpu.product": "NVIDIA-A100-SXM4-80GB",
	}, dep.Spec.Template.Spec.NodeSelector)

	// Unknown product: nothing renders, the condition explains.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "accel", Namespace: "test"}, stack))
	stack.Spec.Accelerator = &servingv1alpha1.Accelerator{Product: "tpu-v6"}
	require.NoError(t, c.Update(ctx, stack))
	_, err = r.Reconcile(ctx, requestFor("accel"))
	require.NoError(t, err)

	current := &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "accel", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "WorkloadReady", false, "UnknownAccelerator")
	dep = &appsv1.Deployment{}
	err = c.Get(ctx, types.NamespacedName{Name: "accel", Namespace: "test"}, dep)
	require.Error(t, err, "the deployment must retire on an unresolvable accelerator")
}

func TestInferenceStackUnsupportedParallelism(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "pp", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://m"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 1, Pipeline: ptr.To(2)},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("pp"))
	require.NoError(t, err)

	current := &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "pp", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "WorkloadReady", false, "UnsupportedParallelism")
	assertCondition(t, current.Status.Conditions, "Ready", false, "UnsupportedParallelism")
}

func TestInferenceStackRoutingEmptyResolvesDefaultRouter(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:        "test",
		Annotations: map[string]string{servingv1alpha1.DefaultRouterAnnotation: "shared"},
	}}
	require.NoError(t, c.Create(ctx, ns))

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "routed", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://m"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 1},
			Routing:     &servingv1alpha1.RoutingSpec{},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("routed"))
	require.NoError(t, err)

	// The owned ModelServer: workload ref to the rendered Deployment,
	// routerRef from the namespace annotation, targetPort 8000.
	ms := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "routed", Namespace: "test"}, ms))
	assert.Equal(t, "apps/v1", ms.Spec.Workload.APIVersion)
	assert.Equal(t, "Deployment", ms.Spec.Workload.Kind)
	assert.Equal(t, "routed", ms.Spec.Workload.Name)
	assert.Equal(t, "shared", ms.Spec.RouterRef)
	require.NotNil(t, ms.Spec.TargetPort)
	assert.Equal(t, int32(8000), *ms.Spec.TargetPort)
	assertController(t, ms, "routed")

	current := &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "routed", Namespace: "test"}, current))
	assert.Equal(t, "routed", current.Status.ModelServerName)
	assertCondition(t, current.Status.Conditions, "RoutingReady", false, "ModelServerPending")

	// No default-router annotation: RoutingReady=False/NoDefaultRouter
	// and no ModelServer.
	require.NoError(t, c.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "no-router"},
	}))
	stack2 := stack.DeepCopy()
	stack2.ResourceVersion = ""
	stack2.Name = "orphan"
	stack2.Namespace = "no-router"
	stack2.Spec.Routing = &servingv1alpha1.RoutingSpec{}
	require.NoError(t, c.Create(ctx, stack2))
	_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: "no-router", Name: "orphan",
	}})
	require.NoError(t, err)

	current = &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "orphan", Namespace: "no-router"}, current))
	assertCondition(t, current.Status.Conditions, "RoutingReady", false, "NoDefaultRouter")
	ms = &servingv1alpha1.ModelServer{}
	err = c.Get(ctx, types.NamespacedName{Name: "orphan", Namespace: "no-router"}, ms)
	require.Error(t, err, "no ModelServer without a resolvable router")
}

func TestInferenceStackRoutingExplicitRouterRef(t *testing.T) {
	ctx := context.Background()
	c, r := newStackHarness(t)

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "explicit", Namespace: "test"},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://m"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 1},
			Routing:     &servingv1alpha1.RoutingSpec{RouterRef: ptr.To("special")},
		},
	}
	require.NoError(t, c.Create(ctx, stack))
	_, err := r.Reconcile(ctx, requestFor("explicit"))
	require.NoError(t, err)

	ms := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "explicit", Namespace: "test"}, ms))
	assert.Equal(t, "special", ms.Spec.RouterRef)

	// Routing removed: the ModelServer retires.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "explicit", Namespace: "test"}, stack))
	stack.Spec.Routing = nil
	require.NoError(t, c.Update(ctx, stack))
	_, err = r.Reconcile(ctx, requestFor("explicit"))
	require.NoError(t, err)

	ms = &servingv1alpha1.ModelServer{}
	err = c.Get(ctx, types.NamespacedName{Name: "explicit", Namespace: "test"}, ms)
	require.Error(t, err, "the ModelServer must retire when routing is removed")
}

func TestInferenceStackColdStartConvergence(t *testing.T) {
	// The full wiring, driven through all three controllers the way
	// the manager would: InferenceStack → Deployment + ModelServer →
	// InferencePool + HTTPRoute → Router activates on pool linkage.
	// Children render on EXISTENCE; external controllers (deployment
	// rollout) are simulated by status updates.
	ctx := context.Background()
	c, stackR, msR, routerR := newFullStackHarness(t)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "test",
		Annotations: map[string]string{
			servingv1alpha1.DefaultRouterAnnotation:  "shared",
			servingv1alpha1.DefaultGatewayAnnotation: "demo-gateway",
		},
	}}
	require.NoError(t, c.Create(ctx, ns))
	require.NoError(t, c.Create(ctx, &routingv1alpha1.Router{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"},
	}))

	stack := &servingv1alpha1.InferenceStack{
		ObjectMeta: metav1.ObjectMeta{Name: "cold", Namespace: "test", Generation: 1},
		Spec: servingv1alpha1.InferenceStackSpec{
			Model:       servingv1alpha1.InferenceStackModel{URI: "hf://Qwen/Qwen3-32B"},
			Runtime:     "vllm",
			Parallelism: servingv1alpha1.Parallelism{Tensor: 1},
			Routing:     &servingv1alpha1.RoutingSpec{},
		},
	}
	require.NoError(t, c.Create(ctx, stack))

	// Pass 1: the stack renders the Deployment and ModelServer. The
	// ModelServer renders the pool even though nothing is Available
	// yet (existence, not readiness). The Router activates on the
	// pool link.
	_, err := stackR.Reconcile(ctx, requestFor("cold"))
	require.NoError(t, err)
	_, err = msR.Reconcile(ctx, requestFor("cold"))
	require.NoError(t, err)
	_, err = routerR.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	pool := &igwapi.InferencePool{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cold", Namespace: "test"}, pool))
	// The pool selector derives from the rendered Deployment's own
	// selector.
	assert.Equal(t, map[igwapi.LabelKey]igwapi.LabelValue{
		"app.kubernetes.io/managed-by":     "llm-d-operators",
		"serving.llm-d.ai/inference-stack": "cold",
		"app":                              "cold",
	}, pool.Spec.Selector.MatchLabels)
	assert.Equal(t, []igwapi.Port{{Number: 8000}}, pool.Spec.TargetPorts)

	// The EPP Deployment exists because the pool linked, even though
	// nothing is Available.
	epp := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, epp))

	// Nothing is Available yet: everything reports not-ready, without
	// deadlocking. (The stack ran before the ModelServer reported, so
	// RoutingReady is still pending.)
	current := &servingv1alpha1.InferenceStack{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cold", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "WorkloadReady", false, "Unavailable")
	assertCondition(t, current.Status.Conditions, "RoutingReady", false, "ModelServerPending")

	// Pass 2: the deployment controller rolls both workloads out.
	markAvailable(t, c, "cold")
	markAvailable(t, c, "shared-epp")

	_, err = routerR.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)
	router := &routingv1alpha1.Router{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, router))
	assertCondition(t, router.Status.Conditions, "Ready", true, "Ready")

	_, err = msR.Reconcile(ctx, requestFor("cold"))
	require.NoError(t, err)
	ms := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cold", Namespace: "test"}, ms))
	assertCondition(t, ms.Status.Conditions, "Ready", true, "Ready")

	route := &gwapiv1.HTTPRoute{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cold", Namespace: "test"}, route))
	assert.Equal(t, gwapiv1.ObjectName("demo-gateway"), route.Spec.ParentRefs[0].Name)
	assert.Equal(t, gwapiv1.ObjectName("cold"), route.Spec.Rules[0].BackendRefs[0].Name)

	// Pass 3: the stack aggregates.
	_, err = stackR.Reconcile(ctx, requestFor("cold"))
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cold", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "WorkloadReady", true, "Available")
	assertCondition(t, current.Status.Conditions, "RoutingReady", true, "Ready")
	assertCondition(t, current.Status.Conditions, "Ready", true, "Ready")
	assert.Equal(t, int64(1), current.Status.ObservedGeneration)
}

func TestSharedPoolColdStartConvergence(t *testing.T) {
	// Shared-pool mode end to end: one Router owning a pool with an
	// explicit selector, two workloads carrying the shared label, two
	// ModelServers joining via poolRef. Expected steady state: one
	// pool, one EPP, both ModelServers Ready.
	ctx := context.Background()
	c, _, msR, routerR := newFullStackHarness(t)

	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "test",
		Annotations: map[string]string{
			servingv1alpha1.DefaultGatewayAnnotation: "demo-gateway",
		},
	}}))
	require.NoError(t, c.Create(ctx, &routingv1alpha1.Router{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"},
		Spec: routingv1alpha1.RouterSpec{
			Pool: &routingv1alpha1.RouterPoolSpec{
				Selector: map[string]string{"llm-d.ai/shared-pool": "true"},
			},
		},
	}))

	for _, name := range []string{"sim-a", "sim-b"} {
		require.NoError(t, c.Create(ctx, availableDeployment(name, map[string]string{
			"app": name, "llm-d.ai/shared-pool": "true",
		})))
	}

	// The Router activates upfront on its own pool.
	_, err := routerR.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)
	markAvailable(t, c, "shared-epp")
	_, err = routerR.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)
	router := &routingv1alpha1.Router{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, router))
	assertCondition(t, router.Status.Conditions, "Ready", true, "Ready")
	assert.Equal(t, "shared-pool", router.Status.PoolName)

	// Both ModelServers join the shared pool.
	for _, name := range []string{"model-a", "model-b"} {
		ms := &servingv1alpha1.ModelServer{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
			Spec: servingv1alpha1.ModelServerSpec{
				Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "sim-" + name[len(name)-1:]},
				RouterRef: "shared",
				PoolRef:   ptr.To("shared-pool"),
			},
		}
		require.NoError(t, c.Create(ctx, ms))
		_, err = msR.Reconcile(ctx, requestFor(name))
		require.NoError(t, err)
	}

	pools := &igwapi.InferencePoolList{}
	require.NoError(t, c.List(ctx, pools))
	require.Len(t, pools.Items, 1, "exactly one pool: the Router-owned shared pool")

	for _, name := range []string{"model-a", "model-b"} {
		current := &servingv1alpha1.ModelServer{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: "test"}, current))
		assertCondition(t, current.Status.Conditions, "PoolReady", true, "Joined")
		assertCondition(t, current.Status.Conditions, "Ready", true, "Ready")

		route := &gwapiv1.HTTPRoute{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: "test"}, route))
		assert.Equal(t, gwapiv1.ObjectName("shared-pool"),
			route.Spec.Rules[0].BackendRefs[0].Name)
	}
}

// newStackHarness builds a fake client + InferenceStack reconciler.
func newStackHarness(t *testing.T) (client.WithWatch, *InferenceStackReconciler) {
	t.Helper()
	c, stackR, _, _ := newFullStackHarness(t)
	return c, stackR
}

// newFullStackHarness builds a fake client shared by the
// InferenceStack, ModelServer and Router reconcilers, the way the
// manager wires them.
func newFullStackHarness(t *testing.T) (client.WithWatch, *InferenceStackReconciler, *ModelServerReconciler, *routing.RouterReconciler) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, servingv1alpha1.AddToScheme(scheme))
	require.NoError(t, routingv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, igwapi.Install(scheme))
	require.NoError(t, gwapiv1.Install(scheme))

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&servingv1alpha1.InferenceStack{},
			&servingv1alpha1.ModelServer{},
			&routingv1alpha1.Router{},
		).Build()
	return c,
		&InferenceStackReconciler{Client: c, Scheme: scheme},
		&ModelServerReconciler{Client: c, Scheme: scheme},
		&routing.RouterReconciler{Client: c, Scheme: scheme}
}

// markAvailable simulates the deployment controller rolling a
// Deployment out.
func markAvailable(t *testing.T, c client.WithWatch, name string) {
	t.Helper()
	ctx := context.Background()
	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: "test"}, dep))
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	dep.Status.AvailableReplicas = replicas
	dep.Status.Conditions = append(dep.Status.Conditions, appsv1.DeploymentCondition{
		Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
		Reason: "MinimumReplicasAvailable", LastTransitionTime: metav1.Now(),
	})
	require.NoError(t, c.Status().Update(ctx, dep))
}

// assertController asserts that obj is controlled by the named owner.
func assertController(t *testing.T, obj client.Object, owner string) {
	t.Helper()
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			assert.Equal(t, owner, ref.Name)
			return
		}
	}
	t.Fatalf("no controller owner reference on %s", obj.GetName())
}

func findCondition(conds []metav1.Condition, condType string) (*metav1.Condition, bool) {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i], true
		}
	}
	return nil, false
}
