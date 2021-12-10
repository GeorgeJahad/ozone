#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source $SCRIPT_DIR/vars.sh
killK() {
cd $KUBERNETES_DIR; kubectl delete -f .
pkill -9  kubectl
sleep 5

   kubectl delete statefulset --all
   kubectl delete daemonset --all
   kubectl delete deployment --all
   kubectl delete service --all
   kubectl delete configmap --all
   kubectl delete pod --all
   kubectl delete pvc --all
   kubectl delete pv --all
}

killK