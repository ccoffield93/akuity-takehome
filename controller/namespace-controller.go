package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// This controller assumes NamespaceClass is a CRD like:
//
//	apiVersion: apiextensions.k8s.io/v1
//	kind: CustomResourceDefinition
//	metadata:
//	  name: namespaceclasses.example.com

// and that Namespaces opt into a class via a label:
//
//	metadata:
//	  labels:
//	    namespaceclass.akuity.io/name: my-class
//
// Rather than generating a typed clientset for this one CRD, this uses the
// dynamic client + dynamicinformer, which works against unstructured objects
// for any GVR without codegen.
// TODO: Consider using client-gen to generate a typed clientset for NamespaceClass, which would give compile-time type safety and avoid the need to use unstructured.Unstructured. For a small example like this, dynamic client is simpler and avoids codegen boilerplate.
// This would allow compile-time type safety and avoid the need to use unstructured objects.
var namespaceGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "namespaces",
}

var namespaceClassGVR = schema.GroupVersionResource{
	Group:    "akuity.io",
	Version:  "v1",
	Resource: "namespaceclasses",
}

// This is the label that should be checked on namespaces to assign a NSC.
const namespaceClassLabel = "namespaceclass.akuity.io/name"
const lastNamespaceClassLabel = "namespaceclass.akuity.io/last-applied-name"

// Log verbosity used for klog.V(<n>) checks. Increase for more verbose output.
// Common guidance:
//   - 0: Production/info — only important Info/Warning/Error and minimal V(0) messages.
//   - 1: Debug (default) — lightweight debug messages useful during normal troubleshooting.
//   - 2: Verbose/trace — more detailed internal state useful for deeper debugging.
//   - 3+: Very verbose — per-item loops, full object dumps; extremely noisy.
const logVerbosity = 1

// queueKey tags each workqueue entry with which resource it refers to, so a
// single shared queue/worker pool can drive reconciliation for both types.
type queueKey struct {
	kind string // "Namespace" or "NamespaceClass"
	name string // namespace name, or namespaceclass name (both cluster-scoped)
}

type Controller struct {
	dynamicClient   dynamic.Interface
	nsInformer      cache.SharedIndexInformer
	nsClassInformer cache.SharedIndexInformer
	nsClassIndexer  cache.Indexer // indexed so we can find namespaces by class cheaply
	queue           workqueue.RateLimitingInterface
	// per-namespace locks to serialize reconciliation for the same namespace
	locks   map[string]*sync.Mutex
	locksMu sync.Mutex
}

// nsClassNameIndexer indexes Namespace objects by the class they reference,
// so that when a NamespaceClass changes we can look up every Namespace that
// points at it without a full O(n) scan on every reconcile.
// This index is maintained automatically whenever the informer sees a Namespace add/update/delete event.
const byNamespaceClassIndex = "byNamespaceClass"

func main() {
	klog.InitFlags(nil)
	// set klog verbosity from the compile-time `logVerbosity` constant so
	// klog.V(<n>) checks behave according to our chosen default.
	flag.Set("v", fmt.Sprintf("%d", logVerbosity))
	flag.Parse()
	defer klog.Flush()

	// set up access to the cluster via kubeconfig
	// this is only for local use, production would need better config handling
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// set up the dynamic informer factory and controller
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 30*time.Second)
	controller := NewController(dynamicClient, factory)

	// run the controller with 2 workers and a stop channel
	// 2 workers should be enough, namespaces shouldn't be
	// getting created or changing classes often, but we could
	// make this configurable if needed.
	stopCh := make(chan struct{}) // nothing will ever close this, we could wire it to OS signals
	defer close(stopCh)

	if err := controller.Run(2, stopCh); err != nil {
		panic(err.Error())
	}
}

func nsClassNameIndexFunc(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	labels := u.GetLabels()
	class, ok := labels[namespaceClassLabel]
	if !ok || class == "" {
		return nil, nil
	}
	return []string{class}, nil
}

// Build a controller that watches Namespaces and NamespaceClasses, and enqueues them for reconciliation.
// This includes an indexer on Namespaces by the class they reference, so that when a NamespaceClass changes we can look up every Namespace that points at it without a full O(n) scan on every reconcile.
func NewController(dynamicClient dynamic.Interface, factory dynamicinformer.DynamicSharedInformerFactory) *Controller {
	nsInformer := factory.ForResource(namespaceGVR).Informer()
	nsClassInformer := factory.ForResource(namespaceClassGVR).Informer()

	if err := nsInformer.AddIndexers(cache.Indexers{
		byNamespaceClassIndex: nsClassNameIndexFunc,
	}); err != nil {
		panic(err)
	}

	c := &Controller{
		dynamicClient:   dynamicClient,
		nsInformer:      nsInformer,
		nsClassInformer: nsClassInformer,
		nsClassIndexer:  nsInformer.GetIndexer(),
		queue:           workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		locks:           make(map[string]*sync.Mutex),
	}

	nsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueue("Namespace", obj) },
		UpdateFunc: func(old, new interface{}) { c.enqueue("Namespace", new) },
		DeleteFunc: func(obj interface{}) { c.enqueue("Namespace", obj) },
	})

	nsClassInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueue("NamespaceClass", obj) },
		UpdateFunc: func(old, new interface{}) { c.enqueue("NamespaceClass", new) },
		DeleteFunc: func(obj interface{}) { c.enqueue("NamespaceClass", obj) },
	})

	return c
}

func (c *Controller) enqueue(kind string, obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	// Both types are cluster-scoped, so key is just the name (no "ns/name" split needed).
	c.queue.Add(queueKey{kind: kind, name: key})
}

// getNamespaceLock returns a mutex for the given namespace, creating it if
// necessary. The caller should call Lock/Unlock on the returned mutex to
// serialize operations for that namespace.
func (c *Controller) getNamespaceLock(ns string) *sync.Mutex {
	c.locksMu.Lock()
	defer c.locksMu.Unlock()
	m, ok := c.locks[ns]
	if !ok {
		m = &sync.Mutex{}
		c.locks[ns] = m
	}
	return m
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	go c.nsInformer.Run(stopCh)
	go c.nsClassInformer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.nsInformer.HasSynced, c.nsClassInformer.HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.V(1).Info("Controller synced, starting workers...")

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	klog.V(1).Info("Shutting down controller")
	return nil
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func (c *Controller) processNextItem() bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)

	key := item.(queueKey)

	var err error
	switch key.kind {
	case "Namespace":
		err = c.reconcileNamespace(key.name)
	case "NamespaceClass":
		err = c.reconcileNamespaceClass(key.name)
	default:
		err = fmt.Errorf("unknown kind %q in queue", key.kind)
	}

	if err == nil {
		c.queue.Forget(item)
		return true
	}

	if c.queue.NumRequeues(item) < 5 {
		klog.Warningf("Error reconciling %s %q, retrying: %v", key.kind, key.name, err)
		c.queue.AddRateLimited(item)
		return true
	}

	runtime.HandleError(err)
	klog.Errorf("Dropping %s %q out of the queue after too many retries: %v", key.kind, key.name, err)
	c.queue.Forget(item)
	return true
}

// reconcileNamespace handles a single Namespace: prints its labels and, if it
// references a NamespaceClass, looks that class up (from the local informer
// cache, not a live API call) and prints it too.
func (c *Controller) reconcileNamespace(name string) error {
	obj, exists, err := c.nsInformer.GetIndexer().GetByKey(name)
	if err != nil {
		return err
	}
	if !exists {
		klog.V(1).Infof("Namespace %q no longer exists, skipping", name)
		// TODO: Do we need to remove it from indexing?
		return nil
	}
	ns := obj.(*unstructured.Unstructured)

	klog.V(1).Infof("Reconciling Namespace: %s", ns.GetName())
	labels := ns.GetLabels()
	if len(labels) == 0 {
		klog.V(2).Info("  (no labels)")
	}
	for k, v := range labels {
		klog.V(2).Infof("  %s=%s", k, v)
	}

	className, ok := labels[namespaceClassLabel]
	if !ok || className == "" {
		klog.V(2).Info("  no NamespaceClass reference")
		return nil
	}

	classObj, exists, err := c.nsClassInformer.GetIndexer().GetByKey(className)
	if err != nil {
		return err
	}
	if !exists {
		// Referenced class doesn't exist (yet, or was deleted). Depending on
		// your policy this might warrant an event/condition on the namespace;
		// returning an error here would cause a retry with backoff instead.
		// TODO: Shift this into warning, not just printf.
		klog.Warningf("  references NamespaceClass %q, which was not found", className)
		return nil
	}
	class := classObj.(*unstructured.Unstructured)
	klog.V(2).Infof("  belongs to NamespaceClass %q (spec: %v)", class.GetName(), class.Object["spec"])

	// Reconcile logic below.

	// Check label to see if there was a previously applied class
	lastClassName, found := labels[lastNamespaceClassLabel]
	if !found {
		// there was no last class label, so we should apply the current class and set the last-applied label
		klog.V(1).Infof("  no last-applied class label found, applying current class and setting last-applied label")

		// apply current class to namespace (e.g., create/update resources based on class spec)
		err := c.applyDiffToNamespace(name, resourcesFromUnstructured(class), nil)
		if err != nil {
			return fmt.Errorf("error applying current class %q to namespace %q: %v", className, name, err)
		}
		// Update the namespace with the last-applied class label
		labels[lastNamespaceClassLabel] = className
		ns.SetLabels(labels)
		_, err = c.dynamicClient.Resource(namespaceGVR).Update(context.TODO(), ns, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("error updating namespace %q with last-applied class label: %v", name, err)
		}
	} else if lastClassName != className {
		klog.V(1).Infof("  last-applied class label %q differs from current class %q, updating resources and applying new class", lastClassName, className)
		// Try to get the last-applied NamespaceClass from our informer cache first
		// (cheap, avoids a live API call). Fall back to a live GET if not present
		// in the cache. If neither exists, proceed to apply the new class but do
		// not attempt cleanup of old resources.
		cacheObj, exists, err := c.nsClassInformer.GetIndexer().GetByKey(lastClassName)
		if err != nil {
			return fmt.Errorf("error reading previous NamespaceClass %q from cache: %v", lastClassName, err)
		}

		var lastClass *unstructured.Unstructured
		if exists {
			if u, ok := cacheObj.(*unstructured.Unstructured); ok {
				lastClass = u
			} else {
				return fmt.Errorf("unexpected cached type for NamespaceClass %q", lastClassName)
			}
		} else {
			// Not in cache; try a live GET.
			klog.V(2).Infof("previous NamespaceClass %q not found in informer cache, falling back to live GET", lastClassName)
			liveObj, err := c.dynamicClient.Resource(namespaceClassGVR).Get(context.TODO(), lastClassName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.Warningf("  previous NamespaceClass %q not found on server, nothing to clean up", lastClassName)
					lastClass = nil
				} else {
					return fmt.Errorf("error retrieving previous NamespaceClass %q: %v", lastClassName, err)
				}
			} else {
				lastClass = liveObj
			}
		}

		if lastClass != nil {
			klog.V(2).Infof("  previous NamespaceClass %q (spec: %v) retrieved", lastClass.GetName(), lastClass.Object["spec"])

			add, remove := diffNamespaceClassSpecs(resourcesFromUnstructured(lastClass), resourcesFromUnstructured(class))
			klog.V(2).Infof("  resources to add: %v", add)
			klog.V(2).Infof("  resources to remove: %v", remove)
			if err := c.applyDiffToNamespace(name, add, remove); err != nil {
				return fmt.Errorf("error applying diff from previous NamespaceClass %q to namespace %q: %v", lastClassName, name, err)
			}
		}

		// update the namespace with the last-applied class label
		labels[lastNamespaceClassLabel] = className
		ns.SetLabels(labels)
		_, err = c.dynamicClient.Resource(namespaceGVR).Update(context.TODO(), ns, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("error updating namespace %q with last-applied class label: %v", name, err)
		}
	} else {
		klog.V(1).Info("  last-applied class label matches current class, no action needed")
	}
	return nil
}

// reconcileNamespaceClass handles a NamespaceClass change. Since a class
// change can affect every namespace that references it, it looks up all
// such namespaces via the index and re-enqueues them for reconciliation.
func (c *Controller) reconcileNamespaceClass(name string) error {
	obj, exists, err := c.nsClassInformer.GetIndexer().GetByKey(name)
	if err != nil {
		return err
	}
	if !exists {
		klog.V(1).Infof("NamespaceClass %q no longer exists, skipping", name)
		return nil
	}
	class := obj.(*unstructured.Unstructured)
	klog.V(1).Infof("Reconciling NamespaceClass: %s )", class.GetName())
	klog.V(2).Infof("  spec: %v", class.Object["spec"])
	affected, err := c.nsClassIndexer.ByIndex(byNamespaceClassIndex, name)
	if err != nil {
		return err
	}

	klog.V(1).Infof("  %d namespace(s) reference this class", len(affected))

	// We must ONLY delete resources that were created by previous application of the NSC.
	// If the NSC spec changes, we must figure out what changed and only delete resources that were created by the previous spec.
	// We cannot delete resources that were created by other means (e.g. manually, or by another controller).
	// To track this, we will use lastSpec in NSC that contains all objects.
	// Create it as empty when NSC is created. Update it whenever NSC changes. Do a diff between them to find what resources
	// should be created and deleted on all namespaces that reference the NSC.

	lastResources := prevResourcesFromUnstructured(class)
	if lastResources == nil {
		klog.V(1).Infof("  no last-applied resources found in NamespaceClass %q, only need to apply new ones", name)
		newResources := resourcesFromUnstructured(class)
		for _, nsObj := range affected {
			ns := nsObj.(*unstructured.Unstructured)
			err := c.applyDiffToNamespace(ns.GetName(), newResources, nil)
			if err != nil {
				// Don't return, we want to continue processing other namespaces even if one fails.
				klog.Errorf("  error applying new resources to namespace %q: %v", ns.GetName(), err)

			}
		}

		// Update the lastResources in the NSC to the current resources
		spec, ok := class.Object["spec"].(map[string]interface{})
		if !ok || spec == nil {
			spec = make(map[string]interface{})
		}
		spec["lastResources"] = newResources
		class.Object["spec"] = spec
		_, err = c.dynamicClient.Resource(namespaceClassGVR).Update(context.TODO(), class, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("  error updating NamespaceClass %q with last-applied resources: %v", name, err)
		}
	} else {
		// There are some last-applied resources, so we need to diff them with the current resources and apply the changes to all affected namespaces.
		newResources := resourcesFromUnstructured(class)
		add, remove := diffNamespaceClassSpecs(lastResources, newResources)
		klog.V(2).Infof("  resources to add: %v", add)
		klog.V(2).Infof("  resources to remove: %v", remove)

		for _, nsObj := range affected {
			ns := nsObj.(*unstructured.Unstructured)
			err := c.applyDiffToNamespace(ns.GetName(), add, remove)
			if err != nil {
				// Don't return, we want to continue processing other namespaces even if one fails.
				// TODO: Some kind of requeue or retry mechanism might be useful here, but for now just log the error and continue.
				klog.Errorf("  error applying resource changes to namespace %q: %v", ns.GetName(), err)
			}
		}

		// Update the lastResources in the NSC to the current resources
		spec, ok := class.Object["spec"].(map[string]interface{})
		if !ok || spec == nil {
			spec = make(map[string]interface{})
		}
		spec["lastResources"] = newResources
		class.Object["spec"] = spec
		_, err = c.dynamicClient.Resource(namespaceClassGVR).Update(context.TODO(), class, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("  error updating NamespaceClass %q with last-applied resources: %v", name, err)
		}
	}

	return nil
}

// diffNamespaceClassSpecs compares two sets of objects (from the `spec.resources` field of a NamespaceClass) and returns two slices:
// - items present in `class` but not in `lastClass` (to add)
// - items present in `lastClass` but not in `class` (to remove)
// Each item is represented as a map[string]interface{} as it appears in the
// unstructured object's `spec.resources` array.
func diffNamespaceClassSpecs(last, cur []map[string]interface{}) (add []map[string]interface{}, remove []map[string]interface{}) {
	// build index for last and current by a stable key (type:name)
	makeKey := func(m map[string]interface{}) string {
		t := ""
		n := ""
		if v, ok := m["type"]; ok && v != nil {
			t = fmt.Sprintf("%v", v)
		}
		if v, ok := m["name"]; ok && v != nil {
			n = fmt.Sprintf("%v", v)
		}
		return t + ":" + n
	}

	lastIndex := map[string]map[string]interface{}{}
	for _, item := range last {
		lastIndex[makeKey(item)] = item
	}

	curIndex := map[string]map[string]interface{}{}
	for _, item := range cur {
		curIndex[makeKey(item)] = item
	}

	// items in cur but not in last => add
	for k, item := range curIndex {
		if _, ok := lastIndex[k]; !ok {
			add = append(add, item)
		}
	}

	// items in last but not in cur => remove
	for k, item := range lastIndex {
		if _, ok := curIndex[k]; !ok {
			remove = append(remove, item)
		}
	}

	return add, remove
}

// resourcesFromUnstructured safely extracts `spec.resources` from a
// NamespaceClass unstructured object and returns it as a slice of
// map[string]interface{}. It tolerates missing fields and different
// underlying types coming from the API.
// TODO: This would be simpler with a typed clientset for NamespaceClass,
// which would give compile-time type safety and avoid the need to use
// unstructured.Unstructured.
func resourcesFromUnstructured(u *unstructured.Unstructured) []map[string]interface{} {
	if u == nil {
		return nil
	}
	spec, ok := u.Object["spec"].(map[string]interface{})
	if !ok || spec == nil {
		return nil
	}
	raw, ok := spec["resources"].([]interface{})
	if !ok || raw == nil {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

// prevResourcesFromUnstructured extracts `spec.lastResources` from a
// NamespaceClass unstructured object and returns it as a slice of
// map[string]interface{}. Returns nil if the field is missing, empty, or malformed.
func prevResourcesFromUnstructured(u *unstructured.Unstructured) []map[string]interface{} {
	if u == nil {
		return nil
	}
	spec, ok := u.Object["spec"].(map[string]interface{})
	if !ok || spec == nil {
		return nil
	}
	raw, ok := spec["lastResources"].([]interface{})
	if !ok || raw == nil {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// applyDiffToNamespace applies a set of resource additions and removals to a given namespace.
// Currently, it supports ServiceAccount, ConfigMap, and NetworkPolicy.
// It creates or updates resources in the `add` slice and deletes resources in the `remove` slice.
// Returns an error if any operation fails but will proceed with other operations even if one fails, collecting the first error encountered.
func (c *Controller) applyDiffToNamespace(ns string, add []map[string]interface{}, remove []map[string]interface{}) error {
	// Serialize mutations for this namespace so two workers don't race to create
	// the same object concurrently.
	lock := c.getNamespaceLock(ns)
	lock.Lock()
	defer lock.Unlock()
	// Helper to map a simple type string to a GVR, apiVersion and kind
	// TODO: If we decide to support more resource types, support would go here.
	mapType := func(t string) (schema.GroupVersionResource, string, string, bool) {
		switch t {
		case "ServiceAccount":
			return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "serviceaccounts"}, "v1", "ServiceAccount", true
		case "ConfigMap":
			return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, "v1", "ConfigMap", true
		case "NetworkPolicy":
			return schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"}, "networking.k8s.io/v1", "NetworkPolicy", true
		default:
			return schema.GroupVersionResource{}, "", "", false
		}
	}

	// Create or update items in `add`.
	addError := false
	removeError := false
	for _, item := range add {
		traw, okT := item["type"]
		nraw, okN := item["name"]
		if !okT || !okN {
			klog.Warningf("skipping malformed resource entry (missing type or name): %v", item)
			addError = true
			continue
		}
		t := fmt.Sprintf("%v", traw)
		name := fmt.Sprintf("%v", nraw)
		gvr, apiVersion, kind, ok := mapType(t)
		if !ok {
			klog.Warningf("unsupported resource type %q, skipping", t)
			addError = true
			continue
		}

		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ns,
			},
		}}
		// minimal required fields for certain kinds
		if t == "ConfigMap" {
			obj.Object["data"] = map[string]interface{}{}
		} else if t == "NetworkPolicy" {
			obj.Object["spec"] = map[string]interface{}{"podSelector": map[string]interface{}{}}
		}

		resIf := c.dynamicClient.Resource(gvr).Namespace(ns)

		// Use a short timeout per API call to avoid hangs if the API server
		// or a webhook becomes unresponsive.
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		existing, err := resIf.Get(ctx, name, metav1.GetOptions{})
		cancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				klog.Infof("creating %s %s/%s", t, ns, name)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				if _, err := resIf.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
					klog.Errorf("error creating %s %s/%s: %v", t, ns, name, err)
					addError = true
				}
				cancel()
				continue
			}
			klog.Errorf("error fetching existing %s %s/%s: %v", t, ns, name, err)
			addError = true
			continue
		}

		// Merge desired fields into the existing object to avoid accidentally
		// dropping server-populated fields. Then attempt an update and retry
		// once on conflict.
		if t == "ConfigMap" {
			if data, ok := obj.Object["data"]; ok {
				existing.Object["data"] = data
			}
		} else if t == "NetworkPolicy" {
			if spec, ok := obj.Object["spec"]; ok {
				existing.Object["spec"] = spec
			}
		}

		klog.Infof("updating %s %s/%s", t, ns, name)
		// Try an update with retry on conflict.
		// TODO: I'm not sure if we should be doing any updates at all.
		// It may be better to ignore any items that already exist and
		// only create new ones, to avoid overwriting user changes.
		// This is a design decision that isn't covered by the assignment spec.
		// My personal belief is that we should always prioritize user changes
		// over the NSC spec, so we should not update existing resources.
		updateErr := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, err := resIf.Update(ctx, existing, metav1.UpdateOptions{})
			return err
		}()
		if updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				// Refresh and retry once.
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				fresh, err := resIf.Get(ctx, name, metav1.GetOptions{})
				cancel()
				if err != nil {
					klog.Errorf("error re-getting %s %s/%s after conflict: %v", t, ns, name, err)
					addError = true
					continue
				}
				// re-apply desired fields
				if t == "ConfigMap" {
					if data, ok := obj.Object["data"]; ok {
						fresh.Object["data"] = data
					}
				} else if t == "NetworkPolicy" {
					if spec, ok := obj.Object["spec"]; ok {
						fresh.Object["spec"] = spec
					}
				}
				ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
				if _, err := resIf.Update(ctx, fresh, metav1.UpdateOptions{}); err != nil {
					klog.Errorf("error updating %s %s/%s after retry: %v", t, ns, name, err)
					addError = true
				}
				cancel()
			} else {
				klog.Errorf("error updating %s %s/%s: %v", t, ns, name, updateErr)
				addError = true
			}
		}
	}

	// Delete items in `remove`.
	for _, item := range remove {
		traw, okT := item["type"]
		nraw, okN := item["name"]
		if !okT || !okN {
			klog.Warningf("skipping malformed resource entry (missing type or name): %v", item)
			removeError = true
			continue
		}
		t := fmt.Sprintf("%v", traw)
		name := fmt.Sprintf("%v", nraw)
		gvr, _, _, ok := mapType(t)
		if !ok {
			klog.Warningf("unsupported resource type %q for deletion, skipping", t)
			removeError = true
			continue
		}

		klog.Infof("deleting %s %s/%s", t, ns, name)
		if err := c.dynamicClient.Resource(gvr).Namespace(ns).Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				klog.V(2).Infof("resource %s %s/%s already absent", t, ns, name)
				continue
			}
			klog.Errorf("error deleting %s %s/%s: %v", t, ns, name, err)
			removeError = true
		}
	}

	if addError || removeError {
		return fmt.Errorf("one or more errors occurred during applyDiffToNamespace for namespace %q", ns)
	}

	return nil
}
