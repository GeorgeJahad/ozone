#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source $SCRIPT_DIR/vars.sh
initK() {
cd $KUBERNETES_DIR;$SCRIPT_DIR/flekszible generate -t mount:hostPath="$OZONE_ROOT",path=/opt/hadoop -t image:image=apache/ozone-runner:20200420-1 -t ozone/onenode
cd $KUBERNETES_DIR;kubectl apply -f .;
}

restartPorts() {
pkill -9 kubectl
kubectl port-forward datanode-0 8000:9878 &
kubectl port-forward datanode-1 8001:9878 &
kubectl port-forward datanode-2 8002:9878 &
kubectl port-forward s3g-0 7000:9878 &
kubectl port-forward s3g-1 7001:9878 &
kubectl port-forward s3g-2 7002:9878 &
kubectl port-forward s3g-3 7003:9878 &
kubectl port-forward scm-0 9876:9876 &
kubectl port-forward om-0 5005:5005 &
kubectl port-forward scm-0 6006:6006 &
kubectl port-forward s3g-3 7007:7007 &
}

startK() {
echo
echo WAITING for pods to be ready
echo
while (true); do
  kubectl get pods | grep s3g-3 | grep Running;
  if [[ "$?" == "0" ]] ; then break; fi
  sleep 3
done
cd $KUBERNETES_DIR;kubectl wait pod --for=condition=ready -l 'component in (s3g, datanode, om, scm)'
echo
echo PODS ready
echo
restartPorts
sleep 20
$SCRIPT_DIR/mc alias set s0 http://localhost:7000 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
$SCRIPT_DIR/mc alias set s1 http://localhost:7001 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
$SCRIPT_DIR/mc alias set s2 http://localhost:7002 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
$SCRIPT_DIR/mc alias set s3 http://localhost:7003 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7

$SCRIPT_DIR/mc alias set d0 http://localhost:8000 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
$SCRIPT_DIR/mc alias set d1 http://localhost:8001 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
$SCRIPT_DIR/mc alias set d2 http://localhost:8002 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7

$SCRIPT_DIR/mc mb  d0/$DUMMY_BUCKET
echo s3 poc > /tmp/$DUMMY_FILE
$SCRIPT_DIR/mc cp /tmp/$DUMMY_FILE d0/$DUMMY_BUCKET
$SCRIPT_DIR/mc ls  d0/$DUMMY_BUCKET
}

initK
startK