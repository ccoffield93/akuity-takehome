#!/bin/bash

# Delete all resources created by every .yaml file in this folder
for yaml_file in test/*.yaml; do
    if [ -f "$yaml_file" ]; then
        kubectl delete -f "$yaml_file"
    fi
done

kubectl delete crd namespaceclasses.akuity.io
