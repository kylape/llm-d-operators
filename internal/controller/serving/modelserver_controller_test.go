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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1alpha1 "github.com/llm-d/llm-d-operators/api/routing/v1alpha1"
	servingv1alpha1 "github.com/llm-d/llm-d-operators/api/serving/v1alpha1"
	igwapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestModelServerDerivesPoolAndRoute(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	// The BYO workload with its own selector — the pool selector must
	// be derived from it, not maintained by hand.
	deployment := availableDeployment("qwen-engine", map[string]string{"app": "qwen"})
	require.NoError(t, c.Create(ctx, deployment))

	router := readyRouter("shared")
	require.NoError(t, c.Create(ctx, router))

	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "qwen-engine"},
			RouterRef: "shared",
			Gateway:   ptr.To("inference-gateway"),
		},
	}
	require.NoError(t, c.Create(ctx, ms))

	_, err := r.Reconcile(ctx, requestFor("qwen"))
	require.NoError(t, err)

	// The pool: selector derived from the deployment, pointing at the
	// router's EPP service.
	pool := &igwapi.InferencePool{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "qwen", Namespace: "test"}, pool))
	assert.Equal(t, map[igwapi.LabelKey]igwapi.LabelValue{"app": "qwen"}, pool.Spec.Selector.MatchLabels)
	require.NotNil(t, pool.Spec.EndpointPickerRef)
	assert.Equal(t, igwapi.ObjectName("shared-epp"), pool.Spec.EndpointPickerRef.Name)
	assert.Equal(t, []igwapi.Port{{Number: 8000}}, pool.Spec.TargetPorts)

	// The route: cross-group backendRef to the pool, parentRef to the
	// gateway.
	route := &gwapiv1.HTTPRoute{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "qwen", Namespace: "test"}, route))
	require.Len(t, route.Spec.Rules, 1)
	backend := route.Spec.Rules[0].BackendRefs[0]
	assert.Equal(t, gwapiv1.ObjectName("qwen"), backend.Name)
	require.NotNil(t, backend.Group)
	assert.Equal(t, gwapiv1.Group("inference.networking.k8s.io"), *backend.Group)
	parent := route.Spec.ParentRefs[0]
	assert.Equal(t, gwapiv1.ObjectName("inference-gateway"), parent.Name)

	current := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "qwen", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "Ready", true, "Ready")
	assertCondition(t, current.Status.Conditions, "PoolReady", true, "Wired")
	assertCondition(t, current.Status.Conditions, "RouteReady", true, "Attached")
	assert.Equal(t, "qwen", current.Status.PoolName)
}

func TestModelServerGatewayFromNamespaceAnnotation(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	deployment := availableDeployment("llama-engine", map[string]string{"app": "llama"})
	require.NoError(t, c.Create(ctx, deployment))
	require.NoError(t, c.Create(ctx, readyRouter("shared")))

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "test",
		Annotations: map[string]string{servingv1alpha1.DefaultGatewayAnnotation: "platform-gw"},
	}}
	require.NoError(t, c.Create(ctx, ns))

	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "llama-engine"},
			RouterRef: "shared",
		},
	}
	require.NoError(t, c.Create(ctx, ms))

	_, err := r.Reconcile(ctx, requestFor("llama"))
	require.NoError(t, err)

	route := &gwapiv1.HTTPRoute{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "llama", Namespace: "test"}, route))
	assert.Equal(t, gwapiv1.ObjectName("platform-gw"), route.Spec.ParentRefs[0].Name)
}

func TestModelServerNoGateway(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	require.NoError(t, c.Create(ctx, availableDeployment("mixtral-engine", map[string]string{"app": "mixtral"})))
	require.NoError(t, c.Create(ctx, readyRouter("shared")))

	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "mixtral", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "mixtral-engine"},
			RouterRef: "shared",
		},
	}
	require.NoError(t, c.Create(ctx, ms))

	_, err := r.Reconcile(ctx, requestFor("mixtral"))
	require.NoError(t, err)

	current := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "mixtral", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "RouteReady", false, "GatewayNotConfigured")
	assertCondition(t, current.Status.Conditions, "Ready", true, "Ready") // routing itself is complete
	assert.Equal(t, "mixtral", current.Status.PoolName)
}

func TestModelServerWorkloadNotFound(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	require.NoError(t, c.Create(ctx, readyRouter("shared")))
	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "ghost", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "missing"},
			RouterRef: "shared",
		},
	}
	require.NoError(t, c.Create(ctx, ms))

	_, err := r.Reconcile(ctx, requestFor("ghost"))
	require.NoError(t, err)

	current := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "ghost", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "WorkloadReady", false, "WorkloadNotFound")
	assertCondition(t, current.Status.Conditions, "Ready", false, "WiringIncomplete")

	pool := &igwapi.InferencePool{}
	err = c.Get(ctx, types.NamespacedName{Name: "ghost", Namespace: "test"}, pool)
	require.Error(t, err, "no pool must be rendered for an unresolved workload")
}

func TestModelServerPoolRendersWhileRouterNotReady(t *testing.T) {
	// Regression: the pool must render when the workload and Router
	// exist, even if the Router is not yet Ready. The Router only
	// becomes Ready once the pool links to it — gating pool creation
	// on RouterReady deadlocks the wiring.
	ctx := context.Background()
	c, r := newTestHarness(t)

	require.NoError(t, c.Create(ctx, availableDeployment("gpt-engine", map[string]string{"app": "gpt"})))

	notReadyRouter := &routingv1alpha1.Router{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"},
		Status: routingv1alpha1.RouterStatus{
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionFalse, Reason: "NoPool",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	require.NoError(t, c.Create(ctx, notReadyRouter))

	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "gpt", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "gpt-engine"},
			RouterRef: "shared",
		},
	}
	require.NoError(t, c.Create(ctx, ms))

	_, err := r.Reconcile(ctx, requestFor("gpt"))
	require.NoError(t, err)

	pool := &igwapi.InferencePool{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "gpt", Namespace: "test"}, pool),
		"the pool must render even while the router is not ready")
	assert.Equal(t, igwapi.ObjectName("shared-epp"), pool.Spec.EndpointPickerRef.Name)

	current := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "gpt", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "PoolReady", true, "Wired")
	assertCondition(t, current.Status.Conditions, "RouterReady", false, "NoPool")
	assertCondition(t, current.Status.Conditions, "Ready", false, "WiringIncomplete")
}

func TestModelServerJoinsSharedPool(t *testing.T) {
	// Shared-pool mode: with poolRef set, the ModelServer creates no
	// pool of its own; it validates the referenced pool's selector
	// against its workload and points its HTTPRoute at the shared
	// pool. Two ModelServers, one pool.
	ctx := context.Background()
	c, r := newTestHarness(t)

	// The shared pool a Router with spec.pool would own.
	sharedPool := &igwapi.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-router-pool", Namespace: "test"},
		Spec: igwapi.InferencePoolSpec{
			Selector: igwapi.LabelSelector{MatchLabels: map[igwapi.LabelKey]igwapi.LabelValue{
				"llm-d.ai/shared-pool": "true",
			}},
			TargetPorts: []igwapi.Port{{Number: 8000}},
			EndpointPickerRef: &igwapi.EndpointPickerRef{
				Group: ptr.To(igwapi.Group("")),
				Kind:  igwapi.Kind("Service"),
				Name:  igwapi.ObjectName("shared-router-epp"),
				Port:  &igwapi.Port{Number: 9002},
			},
		},
	}
	require.NoError(t, c.Create(ctx, sharedPool))
	require.NoError(t, c.Create(ctx, readyRouter("shared-router")))

	// Two workloads sharing the pool label, each with its own app
	// label for pod ownership.
	require.NoError(t, c.Create(ctx, availableDeployment("sim-a", map[string]string{
		"app": "sim-a", "llm-d.ai/shared-pool": "true",
	})))
	require.NoError(t, c.Create(ctx, availableDeployment("sim-b", map[string]string{
		"app": "sim-b", "llm-d.ai/shared-pool": "true",
	})))

	for _, name := range []string{"model-a", "model-b"} {
		ms := &servingv1alpha1.ModelServer{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
			Spec: servingv1alpha1.ModelServerSpec{
				Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "sim-" + name[len(name)-1:]},
				RouterRef: "shared-router",
				PoolRef:   ptr.To("shared-router-pool"),
				Gateway:   ptr.To("inference-gateway"),
			},
		}
		require.NoError(t, c.Create(ctx, ms))
		_, err := r.Reconcile(ctx, requestFor(name))
		require.NoError(t, err)
	}

	pools := &igwapi.InferencePoolList{}
	require.NoError(t, c.List(ctx, pools))
	require.Len(t, pools.Items, 1, "the ModelServers must not create pools of their own")

	for _, name := range []string{"model-a", "model-b"} {
		current := &servingv1alpha1.ModelServer{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: "test"}, current))
		assertCondition(t, current.Status.Conditions, "PoolReady", true, "Joined")
		assertCondition(t, current.Status.Conditions, "Ready", true, "Ready")
		assert.Equal(t, "shared-router-pool", current.Status.PoolName)
		assert.Equal(t, "shared-router-epp", current.Status.EPPServiceName)

		route := &gwapiv1.HTTPRoute{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: "test"}, route))
		assert.Equal(t, gwapiv1.ObjectName("shared-router-pool"),
			route.Spec.Rules[0].BackendRefs[0].Name, "backendRef must point at the shared pool")
	}
}

func TestModelServerPoolRefSelectorMismatch(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	sharedPool := &igwapi.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-router-pool", Namespace: "test"},
		Spec: igwapi.InferencePoolSpec{
			Selector: igwapi.LabelSelector{MatchLabels: map[igwapi.LabelKey]igwapi.LabelValue{
				"llm-d.ai/shared-pool": "true",
			}},
			EndpointPickerRef: &igwapi.EndpointPickerRef{
				Name: igwapi.ObjectName("shared-router-epp"),
				Port: &igwapi.Port{Number: 9002},
			},
		},
	}
	require.NoError(t, c.Create(ctx, sharedPool))
	require.NoError(t, c.Create(ctx, readyRouter("shared-router")))
	require.NoError(t, c.Create(ctx, availableDeployment("drifted", map[string]string{"app": "drifted"})))

	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "drifted", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "drifted"},
			RouterRef: "shared-router",
			PoolRef:   ptr.To("shared-router-pool"),
			Gateway:   ptr.To("inference-gateway"),
		},
	}
	require.NoError(t, c.Create(ctx, ms))
	_, err := r.Reconcile(ctx, requestFor("drifted"))
	require.NoError(t, err)

	current := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "drifted", Namespace: "test"}, current))
	cond := mustFindCondition(t, current.Status.Conditions, "PoolReady")
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "SelectorMismatch", cond.Reason)
	assert.Contains(t, cond.Message, "llm-d.ai/shared-pool=true", "message must show the pool selector")
	assert.Contains(t, cond.Message, "app=drifted", "message must show the workload selector")
	assertCondition(t, current.Status.Conditions, "RouteReady", false, "HeldForPool")
	assertCondition(t, current.Status.Conditions, "Ready", false, "WiringIncomplete")

	route := &gwapiv1.HTTPRoute{}
	err = c.Get(ctx, types.NamespacedName{Name: "drifted", Namespace: "test"}, route)
	require.Error(t, err, "no route must render while the selector mismatches")
}

func TestModelServerPoolRefNotFound(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	require.NoError(t, c.Create(ctx, availableDeployment("orphan", map[string]string{"app": "orphan"})))
	require.NoError(t, c.Create(ctx, readyRouter("shared-router")))

	ms := &servingv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "test"},
		Spec: servingv1alpha1.ModelServerSpec{
			Workload:  servingv1alpha1.WorkloadRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "orphan"},
			RouterRef: "shared-router",
			PoolRef:   ptr.To("ghost-pool"),
		},
	}
	require.NoError(t, c.Create(ctx, ms))
	_, err := r.Reconcile(ctx, requestFor("orphan"))
	require.NoError(t, err)

	current := &servingv1alpha1.ModelServer{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "orphan", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "PoolReady", false, "PoolNotFound")
	assertCondition(t, current.Status.Conditions, "Ready", false, "WiringIncomplete")
}

func mustFindCondition(t *testing.T, conds []metav1.Condition, condType string) *metav1.Condition {
	t.Helper()
	cond, ok := findCondition(conds, condType)
	require.True(t, ok, "condition %s not found", condType)
	return cond
}

func newTestHarness(t *testing.T) (client.WithWatch, *ModelServerReconciler) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, servingv1alpha1.AddToScheme(scheme))
	require.NoError(t, routingv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, igwapi.Install(scheme))
	require.NoError(t, gwapiv1.Install(scheme))

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&servingv1alpha1.ModelServer{}).Build()
	return c, &ModelServerReconciler{Client: c, Scheme: scheme}
}

func readyRouter(name string) *routingv1alpha1.Router {
	return &routingv1alpha1.Router{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Status: routingv1alpha1.RouterStatus{
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
}

func availableDeployment(name string, selector map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selector},
		},
		Status: appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable",
			LastTransitionTime: metav1.Now(),
		}}},
	}
}

func requestFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "test", Name: name}}
}

func assertCondition(t *testing.T, conds []metav1.Condition, condType string, wantStatus bool, wantReason string) {
	t.Helper()
	for _, cond := range conds {
		if cond.Type == condType {
			assert.Equal(t, wantStatus, cond.Status == metav1.ConditionTrue, "condition %s status", condType)
			assert.Equal(t, wantReason, cond.Reason, "condition %s reason", condType)
			return
		}
	}
	t.Fatalf("condition %s not found", condType)
}
