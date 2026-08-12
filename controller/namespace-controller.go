package main

import (
	"context"
	"fmt"
	"path/filepath"
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
}

// nsClassNameIndexer indexes Namespace objects by the class they reference,
// so that when a NamespaceClass changes we can look up every Namespace that
// points at it without a full O(n) scan on every reconcile.
const byNamespaceClassIndex = "byNamespaceClass"

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

func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	go c.nsInformer.Run(stopCh)
	go c.nsClassInformer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.nsInformer.HasSynced, c.nsClassInformer.HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	fmt.Println("Controller synced, starting workers...")

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	fmt.Println("Shutting down controller")
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
		fmt.Printf("Error reconciling %s %q, retrying: %v\n", key.kind, key.name, err)
		c.queue.AddRateLimited(item)
		return true
	}

	runtime.HandleError(err)
	fmt.Printf("Dropping %s %q out of the queue after too many retries: %v\n", key.kind, key.name, err)
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
		fmt.Printf("Namespace %q no longer exists, skipping\n", name)
		// TODO: Do we need to remove it from indexing?
		return nil
	}
	ns := obj.(*unstructured.Unstructured)

	fmt.Printf("Reconciling Namespace: %s\n", ns.GetName())
	labels := ns.GetLabels()
	if len(labels) == 0 {
		fmt.Println("  (no labels)")
	}
	for k, v := range labels {
		fmt.Printf("  %s=%s\n", k, v)
	}

	className, ok := labels[namespaceClassLabel]
	if !ok || className == "" {
		fmt.Println("  no NamespaceClass reference")
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
		fmt.Printf("  references NamespaceClass %q, which was not found\n", className)
		return nil
	}
	class := classObj.(*unstructured.Unstructured)
	fmt.Printf("  belongs to NamespaceClass %q (spec: %v)\n", class.GetName(), class.Object["spec"])

	// Real reconciling logic would go here, e.g. applying settings from the
	// NamespaceClass spec onto the namespace (labels, annotations, quotas,
	// network policies) via c.dynamicClient.Resource(namespaceGVR).Update(...)
	// or Patch(...).

	// TODO: all reconcile logic. 

	// Check some 'last class' label. 
	// 
	// If it's a different class than the current one, 
	// remove any objects from the old class and apply the new class. 
	// Update the 'last class' label to the current class. 
	// 
	// If it's the same class, do nothing.
	//
	// If it's empty or does not exist, apply the current class and set the 'last class' label to the current class.

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
		fmt.Printf("NamespaceClass %q no longer exists, skipping\n", name)
		return nil
	}
	class := obj.(*unstructured.Unstructured)
	fmt.Printf("Reconciling NamespaceClass: %s (spec: %v)\n", class.GetName(), class.Object["spec"])

	affected, err := c.nsClassIndexer.ByIndex(byNamespaceClassIndex, name)
	if err != nil {
		return err
	}

	// TODO: A "re-enqueue" here isn't going to provide any information on what changed in the NSC.
	// It's good that we're finding out which namespaces need to be updated, but their reconcile needs work.
	fmt.Printf("  %d namespace(s) reference this class, re-enqueuing them\n", len(affected))
	for _, obj := range affected {
		ns := obj.(*unstructured.Unstructured)
		c.queue.Add(queueKey{kind: "Namespace", name: ns.GetName()})
	}

	// TODO: MAJOR problem: 
	// We must ONLY delete resources that were created by previous application of the NSC.
	// If the NSC spec changes, we must figure out what changed and only delete resources that were created by the previous spec.
	// We cannot delete resources that were created by other means (e.g. manually, or by another controller).
	// This is a non-trivial problem and requires careful design. Data loss risk is huge. 
	// Potential solution: lastSpec in NSC that contains all objects. 
	// Create it when NSC is created. Update it whenever NSC changes. Do a diff between them to find what resources 
	// should be created and deleted on all namespaces that reference the NSC.

	return nil
}

func main() {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 30*time.Second)

	controller := NewController(dynamicClient, factory)

	stopCh := make(chan struct{})
	defer close(stopCh)

	if err := controller.Run(2, stopCh); err != nil {
		panic(err.Error())
	}
}

// TODO: Not really sure what these do, revisit. 
var _ = apierrors.IsConflict
var _ = context.Background
var _ = metav1.ListOptions{}