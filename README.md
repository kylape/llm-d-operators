# llm-d-operators — prototype

Prototype operators for llm-d-native serving CRDs, replacing the
KServe `LLMInferenceService` deployment path. Three CRDs, all
implemented:

| CRD | Group | Status | Owns |
|---|---|---|---|
| `Router` | `routing.llm-d.ai` | **implemented** | EPP Deployment + Service + typed `EndpointPickerConfig` ConfigMap |
| `ModelServer` | `serving.llm-d.ai` | **implemented** | InferencePool (selector derived from the referenced workload) + HTTPRoute |
| `InferenceStack` | `serving.llm-d.ai` | **implemented** | engine Deployment (image/args/nodeSelector from catalogs) + owned ModelServer |

## Architecture

```
InferenceStack ──creates──►  ModelServer ──routerRef──►  Router
      │                            │                       │
      │ renders Deployment ◄───────┤                       │ discovers pool via
      ▼                            │ derives selector      │ endpointPickerRef
 typed engine workload      InferencePool ◄───────────────┘
                            + HTTPRoute (parentRef → BYO Gateway)
```

Key properties:

* **InferenceStack derives, never declares.** The GPU count is always
  `parallelism.tensor` (no GPU field in `resources`; an explicit one
  is overwritten). Runtime → image + base args comes from the runtime
  catalog — a `llm-d-runtime-catalog` ConfigMap in the namespace, else
  the baked-in default whose `vllm` entry is the **llm-d-inference-sim
  DEMO STAND-IN** (the sim accepts `--model/--port`, not vLLM flags;
  see `config/samples/serving_v1alpha1_runtime-catalog.yaml` for the
  real-vLLM override with `--tensor-parallel-size`). Accelerator →
  nodeSelector comes from a small built-in catalog (ConfigMap override
  later).
* **Router stays pool-agnostic by default.** A ModelServer creates a
  pool whose `endpointPickerRef` names the Router's `<name>-epp`
  Service; the Router controller discovers that link and renders the
  EPP Deployment with `--pool-name`. Exactly one pool per Router (the
  EPP watches one pool); zero pools holds the Deployment back with
  `PoolLinked=False/NoPool`, two or more with `MultiplePools`.
  The exception is **shared-pool mode**: `spec.pool` (selector +
  targetPort) makes the Router own an InferencePool named
  `<name>-pool` upfront, activating it immediately — N model servers
  then join that one pool via ModelServer `spec.poolRef` instead of
  each creating their own.
* **ModelServer derives, never duplicates.** The pool selector is the
  referenced workload's own `spec.selector` — no label contract is
  maintained by hand. With `spec.poolRef` (shared-pool mode) no pool
  is created; the referenced pool must exist and its selector must
  match the referenced workload's selector (`PoolReady=False/
  SelectorMismatch` with both selectors in the message otherwise),
  and the HTTPRoute backendRef points at the referenced pool.
* **Children render on existence, never on readiness.** Readiness is
  reported via conditions only; gating child rendering on a parent's
  Ready condition deadlocks the wiring (the Router only becomes Ready
  once the pool links, and the pool was once gated on Router Ready).
* **Gateway-mode only.** No proxy is rendered; a Gateway API
  implementation (Envoy Gateway, agentgateway, Istio, ...) owns the
  data plane. Standalone mode stays with the llm-d-router Helm chart.
* **Effective config is observable.** `status.configHash` on Router is
  a digest of the rendered `EndpointPickerConfig` document.

## Prerequisites

The cluster needs the GAIE CRD (`inferencepools.inference.networking.k8s.io`)
and the Gateway API CRDs (for HTTPRoute).

## Build and test

```sh
make test        # unit tests (fake client, no envtest)
make build       # bin/manager
make manifests   # regenerate CRDs + RBAC into config/
```

## Run against a cluster

```sh
kubectl apply -k config/crd
kubectl apply -k config/rbac    # or run with a privileged kubeconfig
KUBECONFIG=... ./bin/manager
```

The manager honors `KUBECONFIG` via controller-runtime's standard
config loading.

Or run it in-cluster instead of as a local binary:

```sh
podman build -t <your-registry>/llm-d-operators:dev .   # see Dockerfile
podman push <your-registry>/llm-d-operators:dev
kubectl apply -k config/default   # CRDs + RBAC + manager Deployment
```

`config/manager` holds the Deployment (leader election, health
probes, distroless image), ServiceAccount and the ClusterRoleBinding
to the generated `manager-role`; `config/default` composes CRDs +
RBAC + manager for a one-shot install.

## Example

```sh
kubectl apply -f config/samples/routing_v1alpha1_router.yaml
kubectl apply -f config/samples/serving_v1alpha1_modelserver.yaml
kubectl get router,ms,inferencepool,httproute -o wide
```

The ModelServer sample wraps the optimized-baseline guide's vLLM
Deployment (`optimized-baseline-nvidia-gpu-vllm-decode`) — the BYO
path. The pool selector is derived from that Deployment's selector.

Demos (self-contained, each with its own namespace):

```sh
kubectl apply -k config/demo         # BYO: hand-authored Deployment + ModelServer
kubectl apply -k config/demo-istack  # InferenceStack: routing {} and plain-workload variants
kubectl apply -k config/demo-shared  # shared pool: one Router pool, two ModelServers joining it
kubectl get istack,ms,router,inferencepool,httproute -n llm-d-operators-istack
```

## Vendored type

`api/routing/v1alpha1/epp_config_types.go` vendors llm-d-router's
`EndpointPickerConfig` (upstream: `apix/config/v1alpha1`, a pseudo-CRD
serialization schema). Adaptations: `apiextensionsv1.JSON` parameters
(preserved unknown fields in the CRD schema), no TypeMeta (added at
ConfigMap render time as `llm-d.ai/v1`). When upstream promotes the
type to a real CRD, reference it and delete the copy.
