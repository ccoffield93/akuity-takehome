# Namespace Class

This repository contains a small Kubernetes controller and CRD that implement a "NamespaceClass":

- A `NamespaceClass` CRD (`crd/namespaceclass-crd.yaml`) declares a set of resources (NetworkPolicies, ServiceAccounts, etc.) that should be created when a `Namespace` opts into that class.
- A controller (`controller/namespace-controller.go`) watches `Namespace` and `NamespaceClass` objects and ensures the resources defined by a class are applied to namespaces that reference it.

Purpose: implement class-based namespace bootstrapping and switching (create resources when a namespace selects a class; delete or update resources when the class changes or the namespace switches classes). The assignment description is in `ASSIGNMENT.md`.

## Quickstart

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

It is recommended that the user *wait* for a few seconds after steps that modify Namespaces or NamespaceClasses before proceeding with the next step to retrieve resources in the cluster, as it may take a few seconds for reconciliation to occur and objects to be created/deleted.

## Notes and concerns 

- There is a design consideration that should be brought up: what to do if an object already exists when the controller tries to 'add' it. Current behavior just writes whatever is in the NSC (which is a blank object, always, right now). I would bring this to the attention of a product owner and ask what the desired behavior should be if the NSC attempts to 'create' when something already exists (and therefore likely was not created by the NSC). 
  - My personal opinion: if you aren't *sure* that it's yours and it's safe to delete/modify, try not to modify anything. 
- Reliance on K8s labels means that anybody who is "applying" a fresh NS or NSC would clear the "last-applied-name" label from a namespace and the "lastResource" field from the NSC spec. This is a difficult consideration to get around, as we must store these 'last' configurations somewhere for reconciling NSC resources. An alternative would be something like a configmap, but that's namespace scoped and all of our resources are cluster scoped. 
  - Another point in favor of an Operator style deployment that would have its own namespace to store things like this configmap data, to separate "last" state from the k8s objects themselves.

## Next work

- More unit tests should be added for the controller. Functional tests are nice for behavior but unit tests are nice for quick repeatability and sanity-checking changes. I generated some basic tests, but more complex test cases that cover the reconcile loops are beyond the scope of the assignment.
- Finalizers. I thought about whether or not they would be appropriate to include here, what actions should be taken if a namespace or namespace class is deleted, if a NSC being deleted should remove all generated resources in all linked namespaces, but decided that was out of scope for the assignment. 
  - The assignment clearly describes what should happen for *Switching Classes* and *Updating Classes* but does not have any requirements for *Deleting Classes*. 
  - Coming from a storage-centric background, one of the most important considerations for me is data loss. I don't like the idea of removing assets that the customer may have been using, and there are no guard rails on the objects created by a NSC-- they could theoretically be *any* Kubernetes object.
- The controller currently uses the dynamic client + unstructured objects for simplicity (no generated typed client). This could be improved by creating a typed client with its own definition spec. It would also remove the need for a function to safely retrieve the parameters from an unstructured object. 
  - This would be an early suggestion for a longer term improvement to expand on NamespaceClasses. 
- NamespaceClass contents only currently support 'type' and 'name', but they could be expanded to actually include some relevant fields (for example, key-value pairs for a configmap). 
  - This is **dangerous**, however. We should not try to stuff too much content into a Kubernetes label, as we can only store so much in it, and etcd shouldn't be abused as a 'database'. 
  - Consider instead an operator that has templates, values, etc, as a much fuller and more comprehensive solution, that doesn't try to pack full objects into labels. 
- I did not bother with creating a Dockerfile, Makefile, pushing an image, etc, as these were not mentioned in the assignment. However, for the sake of robustness, this would be one of my next steps forward in creating this controller. 
  - This would also require minor changes in setting up the access to the API server, as our current approach takes a shortcut to run in the local cluster. 

See `ASSIGNMENT.md` for full requirements.
