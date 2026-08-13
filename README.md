# Namespace Class

This repository contains a small Kubernetes controller and CRD that implement a "NamespaceClass":

- A `NamespaceClass` CRD (`crd/namespaceclass-crd.yaml`) declares a set of resources (NetworkPolicies, ServiceAccounts, etc.) that should be created when a `Namespace` opts into that class.
- A controller (`controller/namespace-controller.go`) watches `Namespace` and `NamespaceClass` objects and ensures the resources defined by a class are applied to namespaces that reference it.

Purpose: implement class-based namespace bootstrapping and switching (create resources when a namespace selects a class; delete or update resources when the class changes or the namespace switches classes). The assignment description is in `ASSIGNMENT.md`.

Quickstart

1. Apply the CRD to your cluster:

```bash
kubectl apply -f crd/namespaceclass-crd.yaml
```

2. Build and run the controller from this repository root:

```bash
go build ./controller
./controller
```

3. Example resources to play with are under `test/`:

- `test/ns-1.yaml`, `test/ns-2.yaml` — sample Namespaces
- `test/nsc-1.yaml`, `test/ns-1-switchclass.yaml` — sample NamespaceClass resources and switch examples

Run the simple scripted tests (local cluster required, e.g. kind/minikube):

```bash
./test/test.sh
```

Notes and next work

- The controller currently uses the dynamic client + unstructured objects for simplicity (no generated typed client). This could be improved by creating a typed client with its own definition spec.
- The code includes helpers to diff `spec.resources` between class revisions; cleanup/apply logic is TODO and should be implemented carefully to avoid deleting user-owned resources.

See `ASSIGNMENT.md` for full requirements and rationale.
