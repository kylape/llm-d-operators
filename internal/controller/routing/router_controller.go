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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	"github.com/llm-d/llm-d-operators/internal/kube"
	routingv1alpha1 "github.com/llm-d/llm-d-operators/api/routing/v1alpha1"
	igwapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
)

const (
	// EPPHealthPort is the EPP's gRPC health port.
	EPPHealthPort int32 = 9003
	// EPPMetricsPort is the EPP's Prometheus port.
	EPPMetricsPort int32 = 9090

	// PoolGroup is the GAIE API group the EPP watches pools in.
	PoolGroup = "inference.networking.k8s.io"
)

// DefaultEPPImage is the EPP image used when spec.image is unset. It
// is pinned per operator release; operator upgrades do not re-render
// existing Routers whose image was set explicitly.
var DefaultEPPImage = "ghcr.io/llm-d/llm-d-router-endpoint-picker:v1.0.0"

// RouterReconciler reconciles a Router object.
type RouterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=routing.llm-d.ai,resources=routers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=routing.llm-d.ai,resources=routers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inference.networking.k8s.io,resources=inferencepools,verbs=get;list;watch;create;update;patch;delete

// Reconcile renders the EPP ConfigMap and Service unconditionally, and
// the EPP Deployment only once exactly one InferencePool links to this
// Router's Service. The EPP process watches exactly one pool, so the
// Router activates on pool linkage rather than at creation.
func (r *RouterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("router", req.NamespacedName)

	router := &routingv1alpha1.Router{}
	if err := r.Get(ctx, req.NamespacedName, router); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := routingv1alpha1.RouterStatus{ObservedGeneration: router.Generation}

	// 1. Render the effective config document into a ConfigMap.
	config := router.Spec.Config
	if config == nil {
		config = defaultEPPConfig()
	}
	doc := &routingv1alpha1.EPPConfigDocument{
		APIVersion:           "llm-d.ai/v1",
		Kind:                 "EndpointPickerConfig",
		EndpointPickerConfig: *config.DeepCopy(),
	}
	configYAML, err := yaml.Marshal(doc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshal epp config: %w", err)
	}
	sum := sha256.Sum256(configYAML)
	status.ConfigHash = hex.EncodeToString(sum[:8])

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      router.Name + "-config",
			Namespace: router.Namespace,
			Labels:    ownedLabels(router.Name),
		},
		Data: map[string]string{"config.yaml": string(configYAML)},
	}
	if err := ctrl.SetControllerReference(router, configMap, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := kube.Upsert(ctx, r.Client, configMap, func(existing client.Object) {
		existing.(*corev1.ConfigMap).Data = configMap.Data
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configmap: %w", err)
	}

	// 2. The EPP Service always exists: pools reference it whether or
	// not the Deployment is up yet.
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EPPServiceName(router.Name),
			Namespace: router.Namespace,
			Labels:    ownedLabels(router.Name),
		},
		Spec: corev1.ServiceSpec{
			Selector: ownedLabels(router.Name),
			Ports: []corev1.ServicePort{
				{Name: "grpc", Port: routingv1alpha1.EPPGRPCPort, TargetPort: intstr.FromInt32(routingv1alpha1.EPPGRPCPort)},
				{Name: "metrics", Port: EPPMetricsPort, TargetPort: intstr.FromInt32(EPPMetricsPort)},
			},
		},
	}
	if err := ctrl.SetControllerReference(router, service, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := kube.Upsert(ctx, r.Client, service, func(existing client.Object) {
		existing.(*corev1.Service).Spec.Ports = service.Spec.Ports
		existing.(*corev1.Service).Spec.Selector = service.Spec.Selector
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile service: %w", err)
	}
	status.ServiceName = service.Name

	// 2.5 The EPP's own identity and permissions. The EPP watches Pods
	// and the InferencePool it serves, so it cannot run as the
	// namespace default ServiceAccount (which has no permissions) —
	// its informers would never sync. A dedicated ServiceAccount, Role
	// and RoleBinding render with the Service, unconditionally: they
	// are needed whenever the EPP Deployment runs.
	if err := r.renderEPPRBAC(ctx, router); err != nil {
		return ctrl.Result{}, err
	}

	// 2.6 Shared pool: when spec.pool is set the Router owns an
	// InferencePool upfront and activates immediately — the pool links
	// to its own EPP Service, so the discovery below finds it. N
	// model-server workloads join this one pool via ModelServer
	// spec.poolRef instead of each creating their own.
	if router.Spec.Pool != nil {
		sharedPool := &igwapi.InferencePool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      RouterPoolName(router.Name),
				Namespace: router.Namespace,
				Labels:    ownedLabels(router.Name),
			},
			Spec: igwapi.InferencePoolSpec{
				Selector: igwapi.LabelSelector{MatchLabels: toIGWLabels(router.Spec.Pool.Selector)},
				TargetPorts: []igwapi.Port{{
					Number: igwapi.PortNumber(ptr.Deref(router.Spec.Pool.TargetPort, 8000)),
				}},
				EndpointPickerRef: &igwapi.EndpointPickerRef{
					Group:       ptr.To(igwapi.Group("")),
					Kind:        igwapi.Kind("Service"),
					Name:        igwapi.ObjectName(service.Name),
					Port:        &igwapi.Port{Number: igwapi.PortNumber(routingv1alpha1.EPPGRPCPort)},
					FailureMode: igwapi.EndpointPickerFailOpen,
				},
			},
		}
		if err := ctrl.SetControllerReference(router, sharedPool, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := kube.Upsert(ctx, r.Client, sharedPool, func(existing client.Object) {
			existing.(*igwapi.InferencePool).Spec = sharedPool.Spec
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile shared inferencepool: %w", err)
		}
	} else if err := r.deleteOwnedPool(ctx, router); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Discover the pool wired to this Router. The EPP watches exactly
	// one pool, so the Router's spec never mentions pools; the wiring
	// flows the other way: a ModelServer creates a pool whose
	// endpointPickerRef names this Router's Service.
	pools := &igwapi.InferencePoolList{}
	if err := r.List(ctx, pools, client.InNamespace(router.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list inferencepools: %w", err)
	}
	var linked []igwapi.InferencePool
	for i := range pools.Items {
		ref := pools.Items[i].Spec.EndpointPickerRef
		if ref != nil && string(ref.Name) == service.Name {
			linked = append(linked, pools.Items[i])
		}
	}

	switch {
	case len(linked) == 0:
		setCondition(&status, "PoolLinked", metav1.ConditionFalse, "NoPool",
			"no InferencePool references this Router's service; the EPP deployment is held back")
		// If a pool was unlinked, retire the deployment.
		if err := r.Delete(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: EPPDeploymentName(router.Name), Namespace: router.Namespace},
		}); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		setCondition(&status, "DeploymentReady", metav1.ConditionFalse, "HeldForPoolLink",
			"the EPP deployment is not rendered until a pool links")
		setCondition(&status, "Ready", metav1.ConditionFalse, "NoPool",
			"waiting for an InferencePool to reference service "+service.Name)
	case len(linked) > 1:
		setCondition(&status, "PoolLinked", metav1.ConditionFalse, "MultiplePools",
			fmt.Sprintf("%d InferencePools reference this Router's service; the EPP watches exactly one", len(linked)))
		setCondition(&status, "DeploymentReady", metav1.ConditionFalse, "HeldForPoolLink",
			"the EPP deployment is not rendered while pool linkage is ambiguous")
		setCondition(&status, "Ready", metav1.ConditionFalse, "MultiplePools",
			"resolve the pool linkage to exactly one pool")
	default:
		pool := linked[0]
		status.PoolName = pool.Name
		setCondition(&status, "PoolLinked", metav1.ConditionTrue, "Linked",
			"pool "+pool.Name+" references this Router's service")

		deployment := r.eppDeployment(router, pool.Name)
		if err := ctrl.SetControllerReference(router, deployment, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := kube.Upsert(ctx, r.Client, deployment, func(existing client.Object) {
			existing.(*appsv1.Deployment).Spec = deployment.Spec
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile deployment: %w", err)
		}

		current := &appsv1.Deployment{}
		if err := r.Get(ctx, types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}, current); err != nil {
			return ctrl.Result{}, err
		}
		if deploymentAvailable(current) {
			setCondition(&status, "DeploymentReady", metav1.ConditionTrue, "Available",
				fmt.Sprintf("%d/%d EPP replicas available", current.Status.AvailableReplicas, ptr.Deref(current.Spec.Replicas, 1)))
			setCondition(&status, "Ready", metav1.ConditionTrue, "Ready", "EPP is serving pool "+pool.Name)
		} else {
			setCondition(&status, "DeploymentReady", metav1.ConditionFalse, "Unavailable",
				"EPP deployment is not yet available")
			setCondition(&status, "Ready", metav1.ConditionFalse, "EPPUnavailable",
				"EPP deployment is not yet available")
		}
	}

	if err := r.patchStatus(ctx, router, status); err != nil {
		return ctrl.Result{}, err
	}
	log.V(1).Info("reconciled router", "pool", status.PoolName, "configHash", status.ConfigHash)
	return ctrl.Result{}, nil
}

// eppDeployment renders the EPP Deployment for a linked pool. Image
// and args are derived; a user-pinned spec.image is never overwritten
// by an operator upgrade. The pod runs as the Router-rendered
// <router>-epp ServiceAccount, whose Role lets the EPP watch Pods and
// the pool it serves.
func (r *RouterReconciler) eppDeployment(router *routingv1alpha1.Router, poolName string) *appsv1.Deployment {
	image := DefaultEPPImage
	if router.Spec.Image != nil {
		image = *router.Spec.Image
	}
	replicas := ptr.Deref(router.Spec.Replicas, 1)
	leaderElection := ptr.Deref(router.Spec.LeaderElection, false)
	resources := routingv1alpha1.EPPDefaultResources()
	if router.Spec.Resources != nil {
		resources = *router.Spec.Resources
	}

	args := []string{
		"--pool-name", poolName,
		"--pool-namespace", router.Namespace,
		"--pool-group", PoolGroup,
		"--config-file", "/config/config.yaml",
		"--grpc-port", fmt.Sprint(routingv1alpha1.EPPGRPCPort),
		"--grpc-health-port", fmt.Sprint(EPPHealthPort),
		"--v", "2",
	}
	if leaderElection {
		args = append(args, "--ha-enable-leader-election")
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EPPDeploymentName(router.Name),
			Namespace: router.Namespace,
			Labels:    ownedLabels(router.Name),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: ownedLabels(router.Name)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: ownedLabels(router.Name)},
				Spec: corev1.PodSpec{
					ServiceAccountName: EPPServiceAccountName(router.Name),
					Containers: []corev1.Container{{
						Name:  "epp",
						Image: image,
						Args:  args,
						Ports: []corev1.ContainerPort{
							{Name: "grpc", ContainerPort: routingv1alpha1.EPPGRPCPort},
							{Name: "health", ContainerPort: EPPHealthPort},
							{Name: "metrics", ContainerPort: EPPMetricsPort},
						},
						Resources:    resources,
						VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/config"}},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: router.Name + "-config"},
						}},
					}},
				},
			},
		},
	}
}

// SetupWithManager wires the Router to its owned objects and to
// InferencePools: a pool referencing this Router's service requeues it.
func (r *RouterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1alpha1.Router{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Watches(&igwapi.InferencePool{}, handler.EnqueueRequestsFromMapFunc(r.poolsToRouters)).
		Complete(r)
}

func (r *RouterReconciler) poolsToRouters(_ context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*igwapi.InferencePool)
	if !ok || pool.Spec.EndpointPickerRef == nil {
		return nil
	}
	// endpointPickerRef names "<router>-epp"; strip the suffix.
	name := string(pool.Spec.EndpointPickerRef.Name)
	if len(name) > 4 && name[len(name)-4:] == "-epp" {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{
			Namespace: pool.Namespace,
			Name:      name[:len(name)-4],
		}}}
	}
	return nil
}

func (r *RouterReconciler) patchStatus(ctx context.Context, router *routingv1alpha1.Router, status routingv1alpha1.RouterStatus) error {
	if router.Status.ObservedGeneration == status.ObservedGeneration &&
		router.Status.ConfigHash == status.ConfigHash &&
		router.Status.PoolName == status.PoolName &&
		conditionsEqual(router.Status.Conditions, status.Conditions) {
		return nil
	}
	router.Status = status
	return r.Status().Update(ctx, router)
}

// defaultEPPConfig mirrors the chart's default-plugins.yaml profile.
func defaultEPPConfig() *routingv1alpha1.EndpointPickerConfig {
	return &routingv1alpha1.EndpointPickerConfig{
		Plugins: []routingv1alpha1.PluginSpec{
			{Type: "queue-scorer"},
			{Type: "kv-cache-utilization-scorer"},
			{Type: "prefix-cache-scorer"},
			{Type: "metrics-data-source", Parameters: &apiextensionsv1.JSON{
				Raw: []byte(`{"scheme":"http","path":"/metrics","insecureSkipVerify":true}`),
			}},
			{Type: "core-metrics-extractor"},
		},
		SchedulingProfiles: []routingv1alpha1.SchedulingProfile{{
			Name: "default",
			Plugins: []routingv1alpha1.SchedulingPlugin{
				{PluginRef: "queue-scorer", Weight: ptr.To(2.0)},
				{PluginRef: "kv-cache-utilization-scorer", Weight: ptr.To(2.0)},
				{PluginRef: "prefix-cache-scorer", Weight: ptr.To(3.0)},
			},
		}},
	}
}

// EPPServiceName is the deterministic name of a Router's EPP Service.
// ModelServers derive it to set endpointPickerRef.
func EPPServiceName(router string) string { return router + "-epp" }

// EPPServiceAccountName is the deterministic name of the
// ServiceAccount the EPP pod runs as.
func EPPServiceAccountName(router string) string { return router + "-epp" }

// renderEPPRBAC renders the ServiceAccount, Role and RoleBinding the
// EPP needs: watching Pods (endpoint state) and the InferencePool it
// serves. Without them the EPP's informers never sync and it
// crash-loops.
func (r *RouterReconciler) renderEPPRBAC(ctx context.Context, router *routingv1alpha1.Router) error {
	name := EPPServiceAccountName(router.Name)

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: router.Namespace,
			Labels:    ownedLabels(router.Name),
		},
	}
	if err := ctrl.SetControllerReference(router, sa, r.Scheme); err != nil {
		return err
	}
	if err := kube.Upsert(ctx, r.Client, sa, func(existing client.Object) {}); err != nil {
		return fmt.Errorf("reconcile epp serviceaccount: %w", err)
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: router.Namespace,
			Labels:    ownedLabels(router.Name),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{PoolGroup},
				Resources: []string{"inferencepools"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
	if err := ctrl.SetControllerReference(router, role, r.Scheme); err != nil {
		return err
	}
	if err := kube.Upsert(ctx, r.Client, role, func(existing client.Object) {
		existing.(*rbacv1.Role).Rules = role.Rules
	}); err != nil {
		return fmt.Errorf("reconcile epp role: %w", err)
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: router.Namespace,
			Labels:    ownedLabels(router.Name),
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      name,
			Namespace: router.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     name,
		},
	}
	if err := ctrl.SetControllerReference(router, binding, r.Scheme); err != nil {
		return err
	}
	if err := kube.Upsert(ctx, r.Client, binding, func(existing client.Object) {
		existing.(*rbacv1.RoleBinding).Subjects = binding.Subjects
		existing.(*rbacv1.RoleBinding).RoleRef = binding.RoleRef
	}); err != nil {
		return fmt.Errorf("reconcile epp rolebinding: %w", err)
	}
	return nil
}

// EPPDeploymentName is the deterministic name of a Router's EPP
// Deployment.
func EPPDeploymentName(router string) string { return router + "-epp" }

// RouterPoolName is the deterministic name of the shared
// InferencePool a Router with spec.pool owns.
func RouterPoolName(router string) string { return router + "-pool" }

// deleteOwnedPool retires the shared pool when spec.pool was removed,
// but only one actually controlled by this Router — a hand-created
// pool that happens to share the name is left alone.
func (r *RouterReconciler) deleteOwnedPool(ctx context.Context, router *routingv1alpha1.Router) error {
	pool := &igwapi.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: RouterPoolName(router.Name), Namespace: router.Namespace},
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pool), pool); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(pool, router) {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, pool))
}

func ownedLabels(router string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "llm-d-operators",
		"routing.llm-d.ai/router":      router,
	}
}

// toIGWLabels converts a plain selector map to the GAIE label types.
func toIGWLabels(in map[string]string) map[igwapi.LabelKey]igwapi.LabelValue {
	out := make(map[igwapi.LabelKey]igwapi.LabelValue, len(in))
	for k, v := range in {
		out[igwapi.LabelKey(k)] = igwapi.LabelValue(v)
	}
	return out
}

func deploymentAvailable(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func setCondition(status *routingv1alpha1.RouterStatus, condType string, condStatus metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: condType, Status: condStatus, Reason: reason, Message: message,
		LastTransitionTime: metav1.Now(),
	})
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

