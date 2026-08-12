package main

import (
	"context"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	// Build kubeconfig (uses in-cluster config if run inside a pod,
	// otherwise falls back to ~/.kube/config)
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// Create a shared informer factory, resynced every 30 seconds
	factory := informers.NewSharedInformerFactory(clientset, 0)
	namespaceInformer := factory.Core().V1().Namespaces().Informer()

	namespaceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ns := obj.(*corev1.Namespace)
			printLabels("ADDED", ns)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ns := newObj.(*corev1.Namespace)
			printLabels("MODIFIED", ns)
		},
	})

	stopCh := make(chan struct{})
	defer close(stopCh)

	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)

	fmt.Println("Controller started, watching namespaces...")
	<-stopCh
}

func printLabels(event string, ns *corev1.Namespace) {
	fmt.Printf("[%s] Namespace: %s\n", event, ns.Name)
	if len(ns.Labels) == 0 {
		fmt.Println("  (no labels)")
		return
	}
	for k, v := range ns.Labels {
		fmt.Printf("  %s=%s\n", k, v)
	}
}

// optional: fetch and print labels for all namespaces on startup
func listAllNamespaces(clientset *kubernetes.Clientset) {
	nsList, err := clientset.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	for _, ns := range nsList.Items {
		printLabels("EXISTING", &ns)
	}
}