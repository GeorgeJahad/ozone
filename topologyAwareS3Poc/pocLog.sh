#!/bin/bash

kubectl logs --all-containers --prefix -f  -l 'component in (s3g, datanode)' --max-log-requests 20 | grep S3POC
