#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )"
export OZONE_ROOT=$SCRIPT_DIR/hadoop-ozone/dist/target/ozone-1.3.0-SNAPSHOT
echo OZONE_ROOT is $OZONE_ROOT
DIST_DIR=$OZONE_ROOT/kubernetes/examples/ozone


killK() {
cd $DIST_DIR; kubectl delete -f .
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