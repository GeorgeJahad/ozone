#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )"
source $SCRIPT_DIR/topologyAwareS3Poc/vars.sh
kubectl logs --all-containers --prefix -f  -l 'component in (s3g, datanode)' --max-log-requests 20 | grep S3POC
