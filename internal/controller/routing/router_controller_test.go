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

package routing

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1alpha1 "github.com/llm-d/llm-d-operators/api/routing/v1alpha1"
	igwapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
)

func TestRouterHoldsDeploymentUntilPoolLinks(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	router := &routingv1alpha1.Router{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"}}
	require.NoError(t, c.Create(ctx, router))

	_, err := r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	// ConfigMap and Service render unconditionally.
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-config", Namespace: "test"}, cm))
	assert.Contains(t, cm.Data["config.yaml"], "kind: EndpointPickerConfig")
	assert.Contains(t, cm.Data["config.yaml"], "prefix-cache-scorer") // default profile

	svc := &corev1.Service{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, svc))

	// No pool linked: no Deployment, conditions explain why.
	dep := &appsv1.Deployment{}
	err = c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, dep)
	require.Error(t, err, "deployment must be held back with no pool linked")

	current := &routingv1alpha1.Router{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "PoolLinked", false, "NoPool")
	assertCondition(t, current.Status.Conditions, "Ready", false, "NoPool")
	assert.NotEmpty(t, current.Status.ConfigHash, "config hash must be observable")
}

func TestRouterActivatesOnPoolLink(t *testing.T) {
	ctx := context.Background()
	c, r := newTestHarness(t)

	router := &routingv1alpha1.Router{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"}}
	require.NoError(t, c.Create(ctx, router))

	pool := &igwapi.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "test"},
		Spec: igwapi.InferencePoolSpec{
			EndpointPickerRef: &igwapi.EndpointPickerRef{
				Group: ptr.To(igwapi.Group("")),
				Kind:  igwapi.Kind("Service"),
				Name:  igwapi.ObjectName("shared-epp"),
				Port:  &igwapi.Port{Number: 9002},
			},
		},
	}
	require.NoError(t, c.Create(ctx, pool))

	_, err := r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, dep))
	args := strings.Join(dep.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(t, args, "--pool-name qwen")
	assert.Contains(t, args, "--pool-namespace test")
	assert.Contains(t, args, "--pool-group inference.networking.k8s.io")
	assert.Equal(t, "/config", dep.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath)

	current := &routingv1alpha1.Router{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, current))
	assert.Equal(t, "qwen", current.Status.PoolName)
	assertCondition(t, current.Status.Conditions, "PoolLinked", true, "Linked")
	assertCondition(t, current.Status.Conditions, "Ready", false, "EPPUnavailable") // deployment not Available in fake client

	// A second pool linking the same service must be rejected.
	pool2 := pool.DeepCopy()
	pool2.ResourceVersion = ""
	pool2.Name = "llama"
	require.NoError(t, c.Create(ctx, pool2))
	_, err = r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "PoolLinked", false, "MultiplePools")
}

func TestRouterSharedPoolActivatesUpfront(t *testing.T) {
	// Shared-pool mode: spec.pool makes the Router own an
	// InferencePool upfront, so it activates without waiting for a
	// ModelServer-created pool to link.
	ctx := context.Background()
	c, r := newTestHarness(t)

	router := &routingv1alpha1.Router{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"},
		Spec: routingv1alpha1.RouterSpec{
			Pool: &routingv1alpha1.RouterPoolSpec{
				Selector:   map[string]string{"llm-d.ai/shared-pool": "true"},
				TargetPort: ptr.To(int32(9000)),
			},
		},
	}
	require.NoError(t, c.Create(ctx, router))

	_, err := r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	// The shared pool: selector as given, endpointPickerRef at the
	// Router's own EPP service, targetPort as declared.
	pool := &igwapi.InferencePool{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-pool", Namespace: "test"}, pool))
	assert.Equal(t, map[igwapi.LabelKey]igwapi.LabelValue{
		"llm-d.ai/shared-pool": "true",
	}, pool.Spec.Selector.MatchLabels)
	require.NotNil(t, pool.Spec.EndpointPickerRef)
	assert.Equal(t, igwapi.ObjectName("shared-epp"), pool.Spec.EndpointPickerRef.Name)
	assert.Equal(t, igwapi.PortNumber(9002), pool.Spec.EndpointPickerRef.Port.Number)
	assert.Equal(t, []igwapi.Port{{Number: 9000}}, pool.Spec.TargetPorts)

	// The Router activated immediately: the EPP Deployment renders
	// with --pool-name pointing at its own pool.
	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, dep))
	args := strings.Join(dep.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(t, args, "--pool-name shared-pool")

	current := &routingv1alpha1.Router{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, current))
	assert.Equal(t, "shared-pool", current.Status.PoolName)
	assertCondition(t, current.Status.Conditions, "PoolLinked", true, "Linked")
	assertCondition(t, current.Status.Conditions, "Ready", false, "EPPUnavailable") // not Available in fake client

	// Removing spec.pool retires the shared pool and the Router goes
	// inert again.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, router))
	router.Spec.Pool = nil
	require.NoError(t, c.Update(ctx, router))
	_, err = r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	err = c.Get(ctx, types.NamespacedName{Name: "shared-pool", Namespace: "test"}, pool)
	require.Error(t, err, "the shared pool must retire when spec.pool is removed")
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared", Namespace: "test"}, current))
	assertCondition(t, current.Status.Conditions, "PoolLinked", false, "NoPool")
}

func TestRouterDoesNotDeleteForeignPool(t *testing.T) {
	// A hand-created pool that merely shares the <router>-pool name
	// must not be deleted when spec.pool is unset.
	ctx := context.Background()
	c, r := newTestHarness(t)

	foreign := &igwapi.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-pool", Namespace: "test"},
		Spec:       igwapi.InferencePoolSpec{},
	}
	require.NoError(t, c.Create(ctx, foreign))

	router := &routingv1alpha1.Router{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"}}
	require.NoError(t, c.Create(ctx, router))
	_, err := r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-pool", Namespace: "test"}, foreign),
		"a pool not controlled by the Router must be left alone")
}

func TestRouterEPPRunsAsDedicatedServiceAccount(t *testing.T) {
	// Regression: the EPP watches Pods and the InferencePool it serves;
	// running as the namespace default ServiceAccount (no permissions)
	// made its informers never sync — the pod crash-looped on a 2-minute
	// cache-sync timeout. The Router must render a dedicated
	// ServiceAccount + Role + RoleBinding and run the EPP as it.
	ctx := context.Background()
	c, r := newTestHarness(t)

	router := &routingv1alpha1.Router{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test"}}
	require.NoError(t, c.Create(ctx, router))

	pool := &igwapi.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "test"},
		Spec: igwapi.InferencePoolSpec{
			EndpointPickerRef: &igwapi.EndpointPickerRef{
				Name: igwapi.ObjectName("shared-epp"),
				Port: &igwapi.Port{Number: 9002},
			},
		},
	}
	require.NoError(t, c.Create(ctx, pool))
	_, err := r.Reconcile(ctx, requestFor("shared"))
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, dep))
	assert.Equal(t, "shared-epp", dep.Spec.Template.Spec.ServiceAccountName,
		"the EPP must run as the Router-rendered ServiceAccount, not the namespace default")

	sa := &corev1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, sa))

	role := &rbacv1.Role{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, role))
	rules := make([]string, 0, len(role.Rules))
	for _, rule := range role.Rules {
		rules = append(rules, fmt.Sprintf("%s:%s", strings.Join(rule.APIGroups, ","), strings.Join(rule.Resources, ",")))
	}
	assert.Contains(t, rules, ":pods", "the EPP watches pods for endpoint state")
	assert.Contains(t, rules, "inference.networking.k8s.io:inferencepools", "the EPP watches the pool it serves")

	binding := &rbacv1.RoleBinding{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "shared-epp", Namespace: "test"}, binding))
	require.Len(t, binding.Subjects, 1)
	assert.Equal(t, "shared-epp", binding.Subjects[0].Name)
	assert.Equal(t, "shared-epp", binding.RoleRef.Name)
}

func newTestHarness(t *testing.T) (client.WithWatch, *RouterReconciler) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, routingv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, igwapi.Install(scheme))

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&routingv1alpha1.Router{}).Build()
	return c, &RouterReconciler{Client: c, Scheme: scheme}
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
