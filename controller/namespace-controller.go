package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/workqueue"
)

// Controller watches Namespaces and reconciles them through a workqueue,
// which is the standard client-go controller pattern: the informer only
// enqueues keys, and a separate worker loop does the actual work with
// retry/backoff on failure.
type Controller struct {
	clientset kubernetes.Interface
	informer  cache.SharedIndexInformer
	lister    cache.GenericLister
	queue     workqueue.RateLimitingInterface
}

func NewController(clientset kubernetes.Interface, informer cache.SharedIndexInformer) *Controller {
	c := &Controller{
		clientset: clientset,
		informer:  informer,
		queue:     workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueue,
		UpdateFunc: func(old, new interface{}) { c.enqueue(new) },
		// no DeleteFunc needed since we only care about create/modify,
		// but you'd add one here (using cache.DeletionHandlingMetaNamespaceKeyFunc)
		// if the reconciler needed to clean anything up.
	})

	return c
}

// enqueue converts the object into a namespace/name key and adds it to the
// queue. Using a string key (rather than the object itself) means that if
// the same namespace changes multiple times before it's processed, it only
// gets reconciled once with the latest state — deduplication is automatic.
func (c *Controller) enqueue(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

// Run starts the informer and the worker pool, and blocks until stopCh is closed.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	go c.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
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

// runWorker repeatedly pulls items off the queue and processes them.
func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

// processNextItem handles a single queue item, including retry logic on error.
func (c *Controller) processNextItem() bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(key.(string))
	if err == nil {
		// success: reset the rate limiter backoff for this key
		c.queue.Forget(key)
		return true
	}

	// failure: requeue with rate-limited backoff, up to a max number of retries
	if c.queue.NumRequeues(key) < 5 {
		fmt.Printf("Error reconciling %q, retrying: %v\n", key, err)
		c.queue.AddRateLimited(key)
		return true
	}

	runtime.HandleError(err)
	fmt.Printf("Dropping %q out of the queue after too many retries: %v\n", key, err)
	c.queue.Forget(key)
	return true
}

// reconcile is where the actual work happens. It's idempotent and driven
// purely by the current state of the object (fetched fresh from the
// informer's cache), not by the specific event that triggered it — that's
// what makes it safe to retry and to coalesce multiple events into one run.
func (c *Controller) reconcile(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	obj, exists, err := c.informer.GetIndexer().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		// Namespace was deleted between enqueue and now; nothing to do.
		fmt.Printf("Namespace %q no longer exists, skipping\n", name)
		return nil
	}

	ns, ok := obj.(*corev1.Namespace)
	if !ok {
		return fmt.Errorf("unexpected object type for key %q", key)
	}

	printLabels(ns)

	// This is where you'd put actual reconciling logic, e.g.:
	//   - ensure a default label/annotation is present
	//   - create a companion resource (ResourceQuota, NetworkPolicy, etc.)
	//   - sync namespace metadata to an external system
	// For a real mutation you'd typically call c.clientset.CoreV1().Namespaces().Update(...)
	// or Patch(...) here, and re-fetch/handle conflicts (apierrors.IsConflict) on retry.

	return nil
}

func printLabels(ns *corev1.Namespace) {
	fmt.Printf("Reconciling namespace: %s\n", ns.Name)
	if len(ns.Labels) == 0 {
		fmt.Println("  (no labels)")
		return
	}
	for k, v := range ns.Labels {
		fmt.Printf("  %s=%s\n", k, v)
	}
}

func main() {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	nsInformer := factory.Core().V1().Namespaces().Informer()

	controller := NewController(clientset, nsInformer)

	stopCh := make(chan struct{})
	defer close(stopCh)

	if err := controller.Run(2, stopCh); err != nil {
		panic(err.Error())
	}
}

// suppress "unused import" if you remove the Update/Patch example above
var _ = apierrors.IsConflict
var _ = context.Background