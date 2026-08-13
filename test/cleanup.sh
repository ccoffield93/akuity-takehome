#!/bin/bash

kubectl delete ns ns-1
kubectl delete ns ns-2
kubectl delete -f test/nsc-1.yaml
kubectl delete -f test/nsc-2.yaml
kubectl delete crd namespaceclasses.akuity.io
