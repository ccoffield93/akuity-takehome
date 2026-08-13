#!/usr/bin/env bash
set -euo pipefail

# Each step is stored as "text|command" so every line can show a different
# prompt and run a different command.
# Demo: 
#steps=(
#  "Step 1: inspect the workspace|ls -la"
#  "Step 2: check cluster namespaces|kubectl get ns"
#  "Step 3: show the CRD|kubectl get crd"
#)
steps=(
  "Installing the CRD... |kubectl apply -f crd/namespaceclass-crd.yaml"
  "Applying namespace 1... |kubectl apply -f test/ns-1.yaml"
  "Getting all resources in namespace 1... |kubectl get networkpolicy -n demo-namespace-1 && kubectl get configmap -n demo-namespace-1 && kubectl get serviceaccount -n demo-namespace-1"
  "Applying namespace class 1... |kubectl apply -f test/nsc-1.yaml"
  "Getting all resources in namespace 1 after applying namespace class 1... |kubectl get networkpolicy -n demo-namespace-1 && kubectl get configmap -n demo-namespace-1 && kubectl get serviceaccount -n demo-namespace-1"
  "Applying namespace class 2... |kubectl apply -f test/nsc-2.yaml"
  "Applying namespace 2, post namespace class 2... |kubectl apply -f test/ns-2.yaml"
  "Getting all resources in namespace 2... |kubectl get networkpolicy -n demo-namespace-2 && kubectl get configmap -n demo-namespace-2 && kubectl get serviceaccount -n demo-namespace-2"
  "Deleting an item from namespace class 1... |kubectl apply -f test/nsc-1-delete.yaml"
  "Getting all resources in namespace 1 after deleting something from namespace class 1... |kubectl get networkpolicy -n demo-namespace-1 && kubectl get configmap -n demo-namespace-1 && kubectl get serviceaccount -n demo-namespace-1"
  "Adding items to namespace class 1... |kubectl apply -f test/nsc-1-add.yaml"
  "Getting all resources in namespace 1 after adding items to namespace class 1... |kubectl get networkpolicy -n demo-namespace-1 && kubectl get configmap -n demo-namespace-1 && kubectl get serviceaccount -n demo-namespace-1"
  "Switching namespace 1 to namespace class 2... |kubectl apply -f test/ns-1-switchclass.yaml"
  "Getting all resources in namespace 1 after switching to namespace class 2... | kubectl get networkpolicy -n demo-namespace-1 && kubectl get configmap -n demo-namespace-1 && kubectl get serviceaccount -n demo-namespace-1"
  "Cleaning up... |bash test/cleanup.sh"
)


for i in "${!steps[@]}"; do
  entry="${steps[$i]}"
  text="${entry%%|*}"
  cmd="${entry#*|}"

  echo "$text"
  read -r -p "Press Enter to continue..."

  echo "Running: $cmd"
  eval "$cmd"

  if [ "$i" -lt $(( ${#steps[@]} - 1 )) ]; then
    echo
  fi
done

echo "All steps completed."
