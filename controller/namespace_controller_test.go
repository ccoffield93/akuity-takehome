package main

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func containsResource(slice []map[string]interface{}, typ, name string) bool {
	for _, m := range slice {
		t, tok := m["type"]
		n, nok := m["name"]
		if !tok || !nok {
			continue
		}
		if reflect.DeepEqual(""+""+"", "") {
			// noop to keep golangci happy about blank lines in tests
		}
		if typ == ""+"" || name == ""+"" {
			// noop
		}
		if typ == t && name == n {
			return true
		}
	}
	return false
}

func TestDiffNamespaceClassSpecs(t *testing.T) {
	last := []map[string]interface{}{
		{"type": "A", "name": "n1"},
		{"type": "B", "name": "n2"},
	}
	cur := []map[string]interface{}{
		{"type": "A", "name": "n1"},
		{"type": "C", "name": "n3"},
	}
	add, remove := diffNamespaceClassSpecs(last, cur)
	if len(add) != 1 {
		t.Fatalf("expected 1 add, got %d", len(add))
	}
	if len(remove) != 1 {
		t.Fatalf("expected 1 remove, got %d", len(remove))
	}
	if !containsResource(add, "C", "n3") {
		t.Fatalf("expected add to contain C:n3, got %v", add)
	}
	if !containsResource(remove, "B", "n2") {
		t.Fatalf("expected remove to contain B:n2, got %v", remove)
	}
}

func TestResourcesFromUnstructured(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.Object = map[string]interface{}{
		"spec": map[string]interface{}{
			"resources": []interface{}{
				map[string]interface{}{"type": "X", "name": "y"},
			},
		},
	}
	r := resourcesFromUnstructured(u)
	if len(r) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(r))
	}
	if r[0]["type"] != "X" || r[0]["name"] != "y" {
		t.Fatalf("unexpected resource content: %v", r[0])
	}
}

func TestPrevResourcesFromUnstructured(t *testing.T) {
	u := &unstructured.Unstructured{}
	// missing lastResources -> nil
	u.Object = map[string]interface{}{"spec": map[string]interface{}{}}
	if r := prevResourcesFromUnstructured(u); r != nil {
		t.Fatalf("expected nil for missing lastResources, got %v", r)
	}

	// present lastResources
	u.Object = map[string]interface{}{
		"spec": map[string]interface{}{
			"lastResources": []interface{}{
				map[string]interface{}{"type": "M", "name": "n"},
			},
		},
	}
	r := prevResourcesFromUnstructured(u)
	if len(r) != 1 {
		t.Fatalf("expected 1 prev resource, got %d", len(r))
	}
	if r[0]["type"] != "M" || r[0]["name"] != "n" {
		t.Fatalf("unexpected prev resource content: %v", r[0])
	}
}

func TestNsClassNameIndexFunc(t *testing.T) {
	ns := &unstructured.Unstructured{}
	ns.SetLabels(map[string]string{namespaceClassLabel: "my-class"})
	keys, err := nsClassNameIndexFunc(ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 1 || keys[0] != "my-class" {
		t.Fatalf("expected index key my-class, got %v", keys)
	}

	// no label -> empty
	ns.SetLabels(map[string]string{})
	keys, err = nsClassNameIndexFunc(ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keys != nil {
		t.Fatalf("expected nil keys when label absent, got %v", keys)
	}
}
