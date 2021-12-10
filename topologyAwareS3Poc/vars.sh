#!/bin/bash
export OZONE_ROOT=$SCRIPT_DIR/../hadoop-ozone/dist/target/ozone-1.3.0-SNAPSHOT
export KUBERNETES_DIR=$OZONE_ROOT/kubernetes/examples/ozone
export DUMMY_BUCKET=dummy-bucket
export DUMMY_FILE=dummy-file

declare -A nodeMap
nodeMap[s0]=s3g-0
nodeMap[s1]=s3g-1
nodeMap[s2]=s3g-2
nodeMap[s3]=s3g-3
nodeMap[d0]=datanode-0
nodeMap[d1]=datanode-1
nodeMap[d2]=datanode-2
