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
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1alpha1 "github.com/llm-d/llm-d-operators/api/routing/v1alpha1"
	servingv1alpha1 "github.com/llm-d/llm-d-operators/api/serving/v1alpha1"
	"github.com/llm-d/llm-d-operators/internal/kube"
	igwapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ModelServerReconciler reconciles a ModelServer object.
type ModelServerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=serving.llm-d.ai,resources=modelservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=serving.llm-d.ai,resources=modelservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=inference.networking.k8s.io,resources=inferencepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=routing.llm-d.ai,resources=routers,verbs=get;list;watch

// Reconcile wires one workload into routing: it derives the
// InferencePool selector from the referenced workload's own selector,
// points the pool's endpointPickerRef at the referenced Router's EPP
// service, and attaches an HTTPRoute to the resolved Gateway.
func (r *ModelServerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("modelserver", req.NamespacedName)

	ms := &servingv1alpha1.ModelServer{}
	if err := r.Get(ctx, req.NamespacedName, ms); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := servingv1alpha1.ModelServerStatus{ObservedGeneration: ms.Generation}
	targetPort := ptr.Deref(ms.Spec.TargetPort, servingv1alpha1.DefaultTargetPort)

	// 1. The workload: source of truth for the pool selector. Existence
	// gates pool rendering; availability is reported, not gating — the
	// wiring is correct even while the workload rolls.
	workloadFound := false
	deployment := &appsv1.Deployment{}
	if ms.Spec.Workload.Kind != "Deployment" || ms.Spec.Workload.APIVersion != "apps/v1" {
		setCondition(&status, "WorkloadReady", metav1.ConditionFalse, "UnsupportedWorkload",
			fmt.Sprintf("workload kind %s/%s is not supported in v0; use a Deployment",
				ms.Spec.Workload.APIVersion, ms.Spec.Workload.Kind))
	} else if err := r.Get(ctx, types.NamespacedName{Name: ms.Spec.Workload.Name, Namespace: ms.Namespace}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			setCondition(&status, "WorkloadReady", metav1.ConditionFalse, "WorkloadNotFound",
				fmt.Sprintf("workload %s not found in namespace %s", ms.Spec.Workload.Name, ms.Namespace))
		} else {
			return ctrl.Result{}, err
		}
	} else {
		workloadFound = true
		if !workloadAvailable(deployment) {
			setCondition(&status, "WorkloadReady", metav1.ConditionFalse, "WorkloadUnavailable",
				fmt.Sprintf("deployment %s is not available", deployment.Name))
		} else {
			setCondition(&status, "WorkloadReady", metav1.ConditionTrue, "Available",
				fmt.Sprintf("deployment %s is available", deployment.Name))
		}
	}

	// 2. The router: the pool points at its EPP service. Existence
	// gates pool rendering; the Router's own readiness is reported,
	// not gating — the Router only becomes Ready once this pool links
	// to it, so gating here would deadlock the wiring.
	routerFound := false
	router := &routingv1alpha1.Router{}
	eppService := ""
	if err := r.Get(ctx, types.NamespacedName{Name: ms.Spec.RouterRef, Namespace: ms.Namespace}, router); err != nil {
		if apierrors.IsNotFound(err) {
			setCondition(&status, "RouterReady", metav1.ConditionFalse, "RouterNotFound",
				fmt.Sprintf("router %s not found in namespace %s", ms.Spec.RouterRef, ms.Namespace))
		} else {
			return ctrl.Result{}, err
		}
	} else {
		routerFound = true
		eppService = routingv1alpha1.EPPServiceName(router.Name)
		status.EPPServiceName = eppService
		if routerCond := meta.FindStatusCondition(router.Status.Conditions, "Ready"); routerCond != nil {
			cond := *routerCond
			cond.Type = "RouterReady"
			cond.LastTransitionTime = metav1.Now()
			meta.SetStatusCondition(&status.Conditions, cond)
		} else {
			setCondition(&status, "RouterReady", metav1.ConditionUnknown, "RouterPending",
				"router has not reported readiness yet")
		}
	}

	// 3. The pool: rendered once both ends exist, regardless of their
	// readiness. Two modes: poolRef joins an existing shared pool
	// (validating the selector matches the workload); otherwise this
	// ModelServer creates and owns the pool, deriving its selector
	// from the workload.
	poolRendered := false
	switch {
	case !workloadFound || !routerFound:
		setCondition(&status, "PoolReady", metav1.ConditionFalse, "HeldForWorkload",
			"the pool is not rendered until the workload and router exist")
	case ms.Spec.PoolRef != nil:
		poolName := *ms.Spec.PoolRef
		pool := &igwapi.InferencePool{}
		if err := r.Get(ctx, types.NamespacedName{Name: poolName, Namespace: ms.Namespace}, pool); err != nil {
			if apierrors.IsNotFound(err) {
				setCondition(&status, "PoolReady", metav1.ConditionFalse, "PoolNotFound",
					fmt.Sprintf("pool %s not found in namespace %s", poolName, ms.Namespace))
			} else {
				return ctrl.Result{}, err
			}
		} else {
			status.PoolName = pool.Name
			if pool.Spec.EndpointPickerRef != nil {
				status.EPPServiceName = string(pool.Spec.EndpointPickerRef.Name)
			}
			if !selectorsMatch(pool, deployment) {
				setCondition(&status, "PoolReady", metav1.ConditionFalse, "SelectorMismatch",
					fmt.Sprintf("pool %s selects %s but workload %s selects %s",
						pool.Name, formatIGWSelector(pool.Spec.Selector.MatchLabels),
						deployment.Name, formatSelector(deployment.Spec.Selector.MatchLabels)))
			} else {
				poolRendered = true
				setCondition(&status, "PoolReady", metav1.ConditionTrue, "Joined",
					fmt.Sprintf("joined pool %s (selector matches workload %s)", pool.Name, deployment.Name))
			}
		}
	case len(deployment.Spec.Selector.MatchLabels) == 0:
		setCondition(&status, "PoolReady", metav1.ConditionFalse, "EmptySelector",
			"the referenced workload has no selector matchLabels to derive a pool selector from")
	default:
		pool := &igwapi.InferencePool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ms.Name,
				Namespace: ms.Namespace,
				Labels:    msLabels(ms.Name),
			},
			Spec: igwapi.InferencePoolSpec{
				Selector: igwapi.LabelSelector{MatchLabels: toIGWLabels(deployment.Spec.Selector.MatchLabels)},
				TargetPorts: []igwapi.Port{{Number: igwapi.PortNumber(targetPort)}},
				EndpointPickerRef: &igwapi.EndpointPickerRef{
					Group:       ptr.To(igwapi.Group("")),
					Kind:        igwapi.Kind("Service"),
					Name:        igwapi.ObjectName(eppService),
					Port:        &igwapi.Port{Number: igwapi.PortNumber(routingv1alpha1.EPPGRPCPort)},
					FailureMode: igwapi.EndpointPickerFailOpen,
				},
			},
		}
		if err := ctrl.SetControllerReference(ms, pool, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := kube.Upsert(ctx, r.Client, pool, func(existing client.Object) {
			existing.(*igwapi.InferencePool).Spec = pool.Spec
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile inferencepool: %w", err)
		}
		poolRendered = true
		status.PoolName = pool.Name
		setCondition(&status, "PoolReady", metav1.ConditionTrue, "Wired",
			fmt.Sprintf("pool selects workload %s and points at %s:%d", deployment.Name, eppService, routingv1alpha1.EPPGRPCPort))
	}

	// 4. The route: attach the pool to a Gateway.
	gatewayName := ptr.Deref(ms.Spec.Gateway, "")
	routeExpected := gatewayName != ""
	if gatewayName == "" {
		ns := &corev1.Namespace{}
		if err := r.Get(ctx, types.NamespacedName{Name: ms.Namespace}, ns); err == nil {
			gatewayName = ns.Annotations[servingv1alpha1.DefaultGatewayAnnotation]
			routeExpected = gatewayName != ""
		}
	}

	// routeOK tracks whether an expected route rendered successfully.
	switch {
	case !routeExpected:
		setCondition(&status, "RouteReady", metav1.ConditionFalse, "GatewayNotConfigured",
			"no spec.gateway and no "+servingv1alpha1.DefaultGatewayAnnotation+" namespace annotation; no HTTPRoute rendered")
	case !poolRendered:
		setCondition(&status, "RouteReady", metav1.ConditionFalse, "HeldForPool",
			"the HTTPRoute is not rendered until the pool exists")
	// (pool-missing blocks Ready via the conditions loop below)
	default:
		route := &gwapiv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ms.Name,
				Namespace: ms.Namespace,
				Labels:    msLabels(ms.Name),
			},
			Spec: gwapiv1.HTTPRouteSpec{
				CommonRouteSpec: gwapiv1.CommonRouteSpec{
					ParentRefs: []gwapiv1.ParentReference{{
						Group: ptr.To(gwapiv1.Group("gateway.networking.k8s.io")),
						Kind:  ptr.To(gwapiv1.Kind("Gateway")),
						Name:  gwapiv1.ObjectName(gatewayName),
					}},
				},
				Rules: []gwapiv1.HTTPRouteRule{{
					BackendRefs: []gwapiv1.HTTPBackendRef{{
						BackendRef: gwapiv1.BackendRef{
							BackendObjectReference: gwapiv1.BackendObjectReference{
								Group: ptr.To(gwapiv1.Group("inference.networking.k8s.io")),
								Kind:  ptr.To(gwapiv1.Kind("InferencePool")),
								Name:  gwapiv1.ObjectName(status.PoolName),
								Port:  ptr.To(gwapiv1.PortNumber(targetPort)),
							},
							Weight: ptr.To(int32(1)),
						},
					}},
				}},
			},
		}
		if ms.Spec.Hostname != nil {
			route.Spec.Hostnames = []gwapiv1.Hostname{gwapiv1.Hostname(*ms.Spec.Hostname)}
		}
		if err := ctrl.SetControllerReference(ms, route, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := kube.Upsert(ctx, r.Client, route, func(existing client.Object) {
			existing.(*gwapiv1.HTTPRoute).Spec = route.Spec
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile httproute: %w", err)
		}
		status.RouteName = route.Name
		setCondition(&status, "RouteReady", metav1.ConditionTrue, "Attached",
			fmt.Sprintf("httproute %s attaches pool %s to gateway %s", route.Name, status.PoolName, gatewayName))
	}

	ready := poolRendered
	for _, cond := range status.Conditions {
		if cond.Type == "Ready" {
			continue
		}
		// A gateway that was never configured is a configuration gap,
		// not a wiring failure: the pool is routed, the route is not
		// expected.
		if cond.Type == "RouteReady" && cond.Reason == "GatewayNotConfigured" {
			continue
		}
		if cond.Status != metav1.ConditionTrue {
			ready = false
		}
	}
	if ready {
		setCondition(&status, "Ready", metav1.ConditionTrue, "Ready",
			"workload, router, pool and route are wired")
	} else {
		setCondition(&status, "Ready", metav1.ConditionFalse, "WiringIncomplete",
			"see the child conditions for the missing piece")
	}

	if err := r.patchStatus(ctx, ms, status); err != nil {
		return ctrl.Result{}, err
	}
	log.V(1).Info("reconciled modelserver", "pool", status.PoolName, "route", status.RouteName)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the ModelServer to its owned pool and route,
// and requeues on changes to the referenced workload or Router.
func (r *ModelServerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&servingv1alpha1.ModelServer{}).
		Owns(&igwapi.InferencePool{}).
		Owns(&gwapiv1.HTTPRoute{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.deploymentsToModelServers)).
		Watches(&routingv1alpha1.Router{}, handler.EnqueueRequestsFromMapFunc(r.routersToModelServers)).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.namespacesToModelServers)).
		Complete(r)
}

// namespacesToModelServers requeues every ModelServer in a changed
// namespace: the default-gateway annotation is read at reconcile time,
// so annotation changes must trigger reconciliation.
func (r *ModelServerReconciler) namespacesToModelServers(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &servingv1alpha1.ModelServerList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetName())); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for _, ms := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: ms.Namespace, Name: ms.Name,
		}})
	}
	return reqs
}

func (r *ModelServerReconciler) deploymentsToModelServers(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &servingv1alpha1.ModelServerList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, ms := range list.Items {
		if ms.Spec.Workload.Name == obj.GetName() {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: ms.Namespace, Name: ms.Name,
			}})
		}
	}
	return reqs
}

func (r *ModelServerReconciler) routersToModelServers(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &servingv1alpha1.ModelServerList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, ms := range list.Items {
		if ms.Spec.RouterRef == obj.GetName() {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: ms.Namespace, Name: ms.Name,
			}})
		}
	}
	return reqs
}

func (r *ModelServerReconciler) patchStatus(ctx context.Context, ms *servingv1alpha1.ModelServer, status servingv1alpha1.ModelServerStatus) error {
	if ms.Status.ObservedGeneration == status.ObservedGeneration &&
		ms.Status.PoolName == status.PoolName &&
		ms.Status.RouteName == status.RouteName &&
		ms.Status.EPPServiceName == status.EPPServiceName &&
		conditionsEqual(ms.Status.Conditions, status.Conditions) {
		return nil
	}
	ms.Status = status
	return r.Status().Update(ctx, ms)
}

func setCondition(status *servingv1alpha1.ModelServerStatus, condType string, condStatus metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: condType, Status: condStatus, Reason: reason, Message: message,
		LastTransitionTime: metav1.Now(),
	})
}

func workloadAvailable(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func conditionsEqual(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Status != b[i].Status || a[i].Reason != b[i].Reason || a[i].Message != b[i].Message {
			return false
		}
	}
	return true
}

func msLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":  "llm-d-operators",
		"serving.llm-d.ai/model-server": name,
	}
}

func toIGWLabels(in map[string]string) map[igwapi.LabelKey]igwapi.LabelValue {
	out := make(map[igwapi.LabelKey]igwapi.LabelValue, len(in))
	for k, v := range in {
		out[igwapi.LabelKey(k)] = igwapi.LabelValue(v)
	}
	return out
}

// selectorsMatch reports whether the pool's selector matches the
// workload's: every pool-selector label must appear in the workload's
// selector, so the pool selects the workload's pods. The workload may
// carry additional labels of its own (e.g. a per-deployment app label
// alongside the shared-pool label).
func selectorsMatch(pool *igwapi.InferencePool, deployment *appsv1.Deployment) bool {
	for k, v := range pool.Spec.Selector.MatchLabels {
		if deployment.Spec.Selector.MatchLabels[string(k)] != string(v) {
			return false
		}
	}
	return true
}

// formatSelector renders a selector map deterministically for
// condition messages.
func formatSelector(in map[string]string) string {
	pairs := make([]string, 0, len(in))
	for k, v := range in {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func formatIGWSelector(in map[igwapi.LabelKey]igwapi.LabelValue) string {
	plain := make(map[string]string, len(in))
	for k, v := range in {
		plain[string(k)] = string(v)
	}
	return formatSelector(plain)
}
