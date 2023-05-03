#!/bin/bash

# this waits for a deployment to have all replicas up-to-date and available
function deployment_fully_updated() {
    x_fully_updated "$1" deployment "$2"
}

# this waits for a statefulset to have all replicas up-to-date and available
function statefulset_fully_updated() {
    x_fully_updated "$1" statefulset "$2"
}

# this waits for a resource type (deployment or statefulset) to have all replicas up-to-date and available
function x_fully_updated() {
    local namespace=$1
    local resourcetype=$2
    local name=$3

    local desiredReplicas
    desiredReplicas=$(kubectl get $resourcetype -n "$namespace" "$name" -o jsonpath='{.status.replicas}')

    local availableReplicas
    availableReplicas=$(kubectl get $resourcetype -n "$namespace" "$name" -o jsonpath='{.status.availableReplicas}')

    local readyReplicas
    readyReplicas=$(kubectl get $resourcetype -n "$namespace" "$name" -o jsonpath='{.status.readyReplicas}')

    local updatedReplicas
    updatedReplicas=$(kubectl get $resourcetype -n "$namespace" "$name" -o jsonpath='{.status.updatedReplicas}')

    if [ "$desiredReplicas" != "$availableReplicas" ] ; then
        return 1
    fi

    if [ "$desiredReplicas" != "$readyReplicas" ] ; then
        return 1
    fi

    if [ "$desiredReplicas" != "$updatedReplicas" ] ; then
        return 1
    fi

    return 0
}

# Run a test every second with a spinner until it succeeds
function spinner_until() {
    local timeoutSeconds="$1"
    local cmd="$2"
    local args=${@:3}

    if [ -z "$timeoutSeconds" ]; then
        timeoutSeconds=-1
    fi

    local delay=1
    local elapsed=0
    local spinstr='|/-\'

    while ! $cmd $args; do
        elapsed=$((elapsed + delay))
        if [ "$timeoutSeconds" -ge 0 ] && [ "$elapsed" -gt "$timeoutSeconds" ]; then
            return 1
        fi
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
}

rm ~/.kube/config
mv ~/.kube/config.bak ~/.kube/config # use the root ServiceAccount for validation

kubectl get statefulsets
kubectl get deployments

echo ""
echo "waiting for the 'web' statefulset"
spinner_until 120 statefulset_fully_updated default web
echo ""
echo "'web' statefulset healthy"
echo "waiting for the 'short-pvc-name' deployment"
echo ""
spinner_until 120 deployment_fully_updated default short-pvc-name
echo ""
echo "'short-pvc-name' deployment healthy"

kubectl get statefulsets
kubectl get deployments
kubectl get pvc

if kubectl get pvc | grep -q int-source; then
  echo "found PVCs in the int-source namespace"
  exit 1
fi
