#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export OZONE_ROOT=$SCRIPT_DIR/../hadoop-ozone/dist/target/ozone-1.3.0-SNAPSHOT
echo OZONE_ROOT is $OZONE_ROOT
DIST_DIR=$OZONE_ROOT/kubernetes/examples/ozone
echo DIST_DIR is $DIST_DIR
DUMMY_BUCKET=dummy-bucket
DUMMY_FILE=dummy-file

initK() {
cd $DIST_DIR;echo flekszible generate -t mount:hostPath="$OZONE_ROOT",path=/opt/hadoop -t image:image=apache/ozone-runner:20200420-1 -t ozone/onenode
cd $DIST_DIR;flekszible generate -t mount:hostPath="$OZONE_ROOT",path=/opt/hadoop -t image:image=apache/ozone-runner:20200420-1 -t ozone/onenode
cd $DIST_DIR;kubectl apply -f .; kubectl scale --replicas=4 statefulset/s3g
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
sleep 35
restartPorts
sleep 5
mc alias set s0 http://localhost:7000 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
mc alias set s1 http://localhost:7001 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
mc alias set s2 http://localhost:7002 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
mc alias set s3 http://localhost:7003 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7

mc alias set d0 http://localhost:8000 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
mc alias set d1 http://localhost:8001 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7
mc alias set d2 http://localhost:8002 dummy duS0g/affs2Bss8A0C7A5UIveM9p4KmBfSfIcQB7

mc mb  d0/$DUMMY_BUCKET
echo s3 poc > /tmp/$DUMMY_FILE
mc cp /tmp/$DUMMY_FILE d0/$DUMMY_BUCKET
mc ls  d0/$DUMMY_BUCKET
}

initK
#startK