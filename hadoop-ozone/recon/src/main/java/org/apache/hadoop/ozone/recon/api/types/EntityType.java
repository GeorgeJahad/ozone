/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;

import java.util.List;

/**
 * Enum class for namespace type.
 */
public enum EntityType {

  ROOT {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(){
      NamespaceSummaryResponse namespaceSummaryResponse = new NamespaceSummaryResponse(EntityType.ROOT);
      List<OmVolumeArgs> volumes = listVolumes();
      namespaceSummaryResponse.setNumVolume(volumes.size());
      List<OmBucketInfo> allBuckets = listBucketsUnderVolume(null);
      namespaceSummaryResponse.setNumBucket(allBuckets.size());
      int totalNumDir = 0;
      long totalNumKey = 0L;
      for (OmBucketInfo bucket : allBuckets) {
        long bucketObjectId = bucket.getObjectID();
        totalNumDir += getTotalDirCount(bucketObjectId);
        totalNumKey += getTotalKeyCount(bucketObjectId);
      }
      namespaceSummaryResponse.setNumTotalDir(totalNumDir);
      namespaceSummaryResponse.setNumTotalKey(totalNumKey);

      return namespaceSummaryResponse;
    }
  },
  VOLUME {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(){
    }
  },
  BUCKET {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(){
    }
  },
  DIRECTORY {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(){
    }
  },
  KEY {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(){
    }
  },
  UNKNOWN { // if path is invalid
    @Override
    public NamespaceSummaryResponse getSummaryResponse(){
    }
  };
  abstract public NamespaceSummaryResponse getSummaryResponse();
  abstract public DUResponse getDuResponse(String path, String[] names);
  abstract public QuotaUsageResponse getQuotaResponse();
  abstract public FileSizeDistributionResponse getDistResponse();
}
